package sync

import (
	"context"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"

	cid "github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	multihash "github.com/multiformats/go-multihash"
)

type MWMessageCache struct {
	ComputeID     func(msg *pb.Message) string
	topicMsgIDMap map[string]OperationDAG
	IDMsgMap      map[string]*pb.Message
	cidBuilder    cid.Builder
	syncMgr       ManualGraphSynchronizationManager
}

type SynchronizeCompatibleMessageCacher interface {
	pubsub.MessageCacheReader
	Put(msg *pb.Message) map[string]struct{}
}

func NewMWMessageCache(ComputeID func(msg *pb.Message) string) *MWMessageCache {
	return &MWMessageCache{
		topicMsgIDMap: make(map[string]OperationDAG),
		IDMsgMap:      make(map[string]*pb.Message),
		ComputeID:     ComputeID,
		cidBuilder:    cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: -1},
	}
}

func (mc *MWMessageCache) Put(msg *pb.Message) map[string]struct{} {
	mid := mc.ComputeID(msg)
	_, messageSeen := mc.IDMsgMap[mid]
	if !messageSeen {
		mc.IDMsgMap[mid] = msg
	}

	op := &AddNodeOperation{}
	if err := op.Unmarshal(msg.Data); err != nil {
		panic(err)
	}

	topicsToSynchronize := make(map[string]struct{})

	for _, topic := range msg.TopicIDs {
		lastMsgID, ok := mc.topicMsgIDMap[topic]
		graphID, err := cid.Decode(topic)
		if err != nil {
			continue
		}

		if !ok {
			mc.syncMgr.AddGraph(graphID)
			graph := mc.syncMgr.GetGraph(graphID)
			mc.topicMsgIDMap[topic] = graph
			lastMsgID = graph
		}

		// Is the msg the graph root? If so ignore it
		if len(op.Parents) == 0 {
			continue
		}

		// Have we seen this message? If so ignore it
		// Note: it's possible we've only seen this for a particular topic and not all of them
		// That's ok though, since we'll catch the update on the next sync
		if messageSeen {
			continue
		}

		// Does the message build off of existing nodes? If so add it, if not queue up a sync
		syncRequired := false
		for _, p := range op.Parents {
			if _, ok := lastMsgID.TryGetNode(*p); !ok {
				syncRequired = true
			}
		}

		if !syncRequired {
			lastMsgID.AddNewVersion(op.Value, op.Parents...)
		} else {
			topicsToSynchronize[topic] = struct{}{}
		}
	}
	return topicsToSynchronize
}

func (mc *MWMessageCache) Get(mid string) (*pb.Message, bool) {
	m, ok := mc.IDMsgMap[mid]
	return m, ok
}

func (mc *MWMessageCache) GetGossipIDs(topic string) []string {
	mgraph, ok := mc.topicMsgIDMap[topic]
	if ok {
		graphHeads := mgraph.GetLatestVersionHistories()
		gossipMsgs := make([]string, len(graphHeads))
		for i, h := range graphHeads {
			gossipMsgs[i] = h.GetNodeID().String()
		}
		return gossipMsgs
	}
	return []string{}
}

func (mc *MWMessageCache) Shift() {}

type SyncGossipConfiguration struct {
	mcache             SynchronizeCompatibleMessageCacher
	supportedProtocols []protocol.ID
	protocol           protocol.ID
	syncMgr            ManualGraphSynchronizationManager
}

func (gs *SyncGossipConfiguration) GetCacher() pubsub.MessageCacheReader {
	return gs.mcache
}

func (gs *SyncGossipConfiguration) SupportedProtocols() []protocol.ID {
	return gs.supportedProtocols
}

func (gs *SyncGossipConfiguration) Protocol() protocol.ID {
	return gs.protocol
}

func (gs *SyncGossipConfiguration) Publish(rt *pubsub.GossipConfigurableRouter, from peer.ID, msg *pb.Message) {
	topicsToSync := gs.mcache.Put(msg)

	calculatedFrom := peer.ID(msg.GetFrom())

	for _, topic := range msg.GetTopicIDs() {
		if _, ok := topicsToSync[topic]; ok {
			graphID, err := cid.Decode(topic)
			if err != nil {
				continue
			}

			gs.syncMgr.SyncGraph(&graphID, calculatedFrom)
		} else {
			tosend := make(map[peer.ID]struct{})
			rt.AddGossipPeers(tosend, []string{topic}, false)

			_, ok := tosend[from]
			if ok {
				delete(tosend, from)
			}

			_, ok = tosend[calculatedFrom]
			if ok {
				delete(tosend, calculatedFrom)
			}

			rt.PropagateMSG(tosend, msg)
		}
	}
}

// NewGossipBaseSub returns a new PubSub object using GossipSubRouter as the router.
func NewGossipSyncMW(ctx context.Context, h host.Host, mcache *MWMessageCache, protocolID protocol.ID, opts ...pubsub.Option) (*pubsub.PubSub, error) {
	rt := pubsub.NewGossipConfigurableRouter(&SyncGossipConfiguration{
		mcache:             mcache,
		supportedProtocols: []protocol.ID{protocolID},
		protocol:           protocolID,
	})
	return pubsub.NewPubSub(ctx, h, rt, opts...)
}

const MWIPNSKey = protocol.ID("/mwipns/1.0.0")

func NewGossipSyncMWIPNS(ctx context.Context, h host.Host, opts ...pubsub.Option) (*pubsub.PubSub, ManualGraphSynchronizationManager, error) {
	protocolID := MWIPNSKey
	cidbuilder := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: -1}
	graphSynchronizer := NewManualGraphSychronizer(h)
	mcache := &MWMessageCache{
		topicMsgIDMap: make(map[string]OperationDAG),
		IDMsgMap:      make(map[string]*pb.Message),
		ComputeID: func(msg *pb.Message) string {
			cid, err := cidbuilder.Sum(msg.GetData())
			if err != nil {
				return ""
			}
			return cid.String()
		},
		cidBuilder: cidbuilder,
		syncMgr:    graphSynchronizer,
	}
	rt := pubsub.NewGossipConfigurableRouter(&SyncGossipConfiguration{
		mcache:             mcache,
		supportedProtocols: []protocol.ID{protocolID},
		protocol:           protocolID,
		syncMgr:            graphSynchronizer,
	})

	ps, err := pubsub.NewPubSub(ctx, h, rt, opts...)
	return ps, graphSynchronizer, err
}

type PubSubMWIPNS struct {
	pubsub   *pubsub.PubSub
	manualGS ManualGraphSynchronizationManager
	graphID  cid.Cid
}

func NewPubSubMWIPNS(p *pubsub.PubSub, gs ManualGraphSynchronizationManager, IPNSKey cid.Cid) GossipMultiWriterIPNS {
	_, err := p.Subscribe(IPNSKey.String(), pubsub.WithProtocol(MWIPNSKey))
	if err != nil {
		panic(err)
	}

	gs.AddGraph(IPNSKey)

	return &PubSubMWIPNS{
		pubsub:   p,
		manualGS: gs,
		graphID:  IPNSKey,
	}
}

func (ipns *PubSubMWIPNS) GetKey() cid.Cid {
	return ipns.graphID
}

func (gs *PubSubMWIPNS) AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid) {
	op := &AddNodeOperation{Value: newCid, Parents: prevCids}
	data, err := op.Marshal()
	//TODO: what if err?
	if err != nil {
		panic(err)
	}
	if err := gs.pubsub.Publish(gs.graphID.String(), data); err != nil {
		panic(err)
	}
}

func (gs *PubSubMWIPNS) GetLatestVersionHistories() []DagNode {
	return gs.manualGS.GetGraph(gs.graphID).GetLatestVersionHistories()
}

func (gs *PubSubMWIPNS) GetRoot() DagNode {
	return gs.manualGS.GetGraph(gs.graphID).GetRoot()
}

func (gs *PubSubMWIPNS) GetNumberOfOperations() int {
	return len(gs.manualGS.GetGraph(gs.graphID).GetOps())
}
