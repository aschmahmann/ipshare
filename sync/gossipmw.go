package sync

import (
	"context"
	"encoding/base64"
	logging "github.com/ipfs/go-log"
	ropts "github.com/libp2p/go-libp2p-routing/options"
	"github.com/pkg/errors"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"

	cbor "github.com/ipfs/go-ipld-cbor"

	cid "github.com/ipfs/go-cid"
)

var log = logging.Logger("pubsub-mw")

func init() {
	cbor.RegisterCborType(CidList{})
}

// CidList is a list of Cids
type CidList struct {
	Heads []cid.Cid
}

// Marshal returns the byte representation of the object
func (msg CidList) Marshal() ([]byte, error) {
	return cbor.DumpObject(msg)
}

// Unmarshal fills the structure with data from the bytes
func (msg *CidList) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, msg)
}

type MWmgr struct {
	ctx context.Context
	ps  *pubsub.PubSub

	host host.Host

	rebroadcastInitialDelay time.Duration
	rebroadcastInterval     time.Duration

	syncMgr ManualGraphSynchronizationManager

	// Map of keys to subscriptions.
	//
	// If a key is present but the subscription is nil, we've bootstrapped
	// but haven't subscribed.
	mx   sync.Mutex
	subs map[string]*pubsub.Subscription
}

// KeyToTopic converts a binary record key to a pubsub topic key.
func KeyToTopic(key string) string {
	// Record-store keys are arbitrary binary. However, pubsub requires UTF-8 string topic IDs.
	// Encodes to "/record/base64url(key)"
	return "/record/" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

// NewPubsubPublisher constructs a new Publisher that publishes IPNS records through pubsub.
// The constructor interface is complicated by the need to bootstrap the pubsub topic.
// This could be greatly simplified if the pubsub implementation handled bootstrap itself
func NewMultiWriterPubSub(ctx context.Context, host host.Host, ps *pubsub.PubSub, syncMgr ManualGraphSynchronizationManager) *MWmgr {
	return &MWmgr{
		ctx:                     ctx,
		ps:                      ps,
		host:                    host,
		rebroadcastInitialDelay: 100 * time.Millisecond,
		rebroadcastInterval:     time.Second,
		subs:                    make(map[string]*pubsub.Subscription),
		syncMgr:                 syncMgr,
	}
}

func (p *MWmgr) Subscribe(key string) error {
	p.mx.Lock()
	// see if we already have a pubsub subscription; if not, subscribe
	sub := p.subs[key]
	p.mx.Unlock()

	if sub != nil {
		return nil
	}

	topic := KeyToTopic(key)

	// Ignore the error. We have to check again anyways to make sure the
	// record hasn't expired.
	//
	// Also, make sure to do this *before* subscribing.
	myID := p.host.ID()
	_ = p.ps.RegisterTopicValidator(topic, func(ctx context.Context, src peer.ID, msg *pubsub.Message) bool {
		if src == myID {
			return true
		}
		return p.addMessage(key, msg.GetData(), msg.GetFrom())
	})

	p.syncMgr.AddGraph(key)

	sub, err := p.ps.Subscribe(topic)
	if err != nil {
		p.mx.Unlock()
		return err
	}

	p.mx.Lock()
	existingSub, _ := p.subs[key]
	if existingSub != nil {
		p.mx.Unlock()
		sub.Cancel()
		return nil
	}

	p.subs[key] = sub
	go p.handleSubscription(sub, key)
	p.mx.Unlock()

	go p.rebroadcast(key)
	log.Debugf("PubsubResolve: subscribed to %s", key)

	return nil
}

// Publish publishes an IPNS record through pubsub with default TTL
func (p *MWmgr) AddNewVersion(ctx context.Context, key string, newCid *cid.Cid, prevCids []*cid.Cid, opts ...ropts.Option) error {
	// Record-store keys are arbitrary binary. However, pubsub requires UTF-8 string topic IDs.
	// Encode to "/record/base64url(key)"
	topic := KeyToTopic(key)

	if err := p.Subscribe(key); err != nil {
		return err
	}

	log.Debugf("PubsubPublish: publish value for key", key)

	op := &AddNodeOperation{Value: newCid, Parents: prevCids}
	data, err := op.Marshal()
	if err != nil {
		return err
	}

	if err := p.ps.Publish(topic, data); err != nil {
		return err
	}

	return nil
}

func (p *MWmgr) addMessage(key string, val []byte, src peer.ID) bool {
	//if p.Validator.Validate(key, val) != nil {
	//	return false
	//}

	graph, err := p.getLocal(key)
	if err != nil {
		return true
	}

	// Check if message is a list of Cids
	cidLst := &CidList{}
	if err := cidLst.Unmarshal(val); err == nil {
		if len(cidLst.Heads) == 0 {
			return false
		}

		// Do we have these heads already, if not queue up a sync
		// Only propagate an operation if we have the entire graph behind it
		syncRequired := false
		for _, head := range cidLst.Heads {
			if _, ok := graph.TryGetNode(head); !ok {
				syncRequired = true
				break
			}
		}

		if syncRequired {
			p.syncMgr.SyncGraph(key, src)
		}
		return !syncRequired
	} else {
		op := &AddNodeOperation{}
		if err := op.Unmarshal(val); err != nil {
			return false
		}

		// Is the msg the graph root? If so ignore it
		if len(op.Parents) == 0 {
			return false
		}

		// Does the message build off of existing nodes? If so add it, if not queue up a sync
		syncRequired := false
		for _, p := range op.Parents {
			if _, ok := graph.TryGetNode(*p); !ok {
				syncRequired = true
				break
			}
		}

		// If a sync is required do it, otherwise just add the operation
		// Only propagate an operation if we have the entire graph behind it
		if syncRequired {
			p.syncMgr.SyncGraph(key, src)
		} else {
			graph.AddNewVersion(op.Value, op.Parents...)
		}
		return !syncRequired
	}
}

func (p *MWmgr) rebroadcast(key string) {
	topic := KeyToTopic(key)
	time.Sleep(p.rebroadcastInitialDelay)

	ticker := time.NewTicker(p.rebroadcastInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			val, _ := p.getLocal(key)
			if val != nil {
				graphHeads := val.GetLatestVersionHistories()
				gossipMsgs := make([]cid.Cid, len(graphHeads))
				for i, h := range graphHeads {
					gossipMsgs[i] = h.GetNodeID()
				}
				cidLst := &CidList{gossipMsgs}
				cidLstBytes, err := cidLst.Marshal()
				if err != nil {
					log.Debugf("could not marshal list of cids: %v", cidLst)
				} else {
					_ = p.ps.Publish(topic, cidLstBytes)
				}
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *MWmgr) getLocal(key string) (OperationDAG, error) {
	graph := p.syncMgr.GetGraph(key)
	if graph == nil {
		return nil, errors.Errorf("could not find graph %s", key)
	}
	return graph, nil
}

func (p *MWmgr) GetValue(ctx context.Context, key string, opts ...ropts.Option) (GossipMultiWriterIPNS, error) {
	if err := p.Subscribe(key); err != nil {
		return nil, err
	}

	gs, err := p.getLocal(key)

	if err != nil {
		return nil, err
	}

	return &pubSubMWIPNS{
		ctx:     ctx,
		mgr:     p,
		graph:   gs,
		graphID: key,
	}, nil
}

// GetSubscriptions retrieves a list of active topic subscriptions
func (p *MWmgr) GetSubscriptions() []string {
	p.mx.Lock()
	defer p.mx.Unlock()

	var res []string
	for sub := range p.subs {
		res = append(res, sub)
	}

	return res
}

// Cancel cancels a topic subscription; returns true if an active
// subscription was canceled
func (p *MWmgr) Cancel(name string) (bool, error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	sub, ok := p.subs[name]
	if ok {
		sub.Cancel()
		delete(p.subs, name)
	}

	return ok, nil
}

func (p *MWmgr) handleSubscription(sub *pubsub.Subscription, key string) {
	defer sub.Cancel()

	for {
		msg, err := sub.Next(p.ctx)
		if err != nil {
			if err != context.Canceled {
				log.Warningf("PubsubResolve: subscription error in %s: %s", key, err.Error())
			}
			return
		}

		p.addMessage(key, msg.GetData(), msg.GetFrom())
	}
}

type pubSubMWIPNS struct {
	ctx     context.Context
	mgr     *MWmgr
	graph   OperationDAG
	graphID string
}

func (ipns *pubSubMWIPNS) GetKey() string {
	return ipns.graphID
}

func (gs *pubSubMWIPNS) AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid) {
	err := gs.mgr.AddNewVersion(gs.ctx, gs.graphID, newCid, prevCids)
	//TODO: what if err?
	if err != nil {
		panic(err)
	}
}

func (gs *pubSubMWIPNS) GetLatestVersionHistories() []DagNode {
	return gs.graph.GetLatestVersionHistories()
}

func (gs *pubSubMWIPNS) GetRoot() DagNode {
	return gs.graph.GetRoot()
}

func (gs *pubSubMWIPNS) GetNumberOfOperations() int {
	return len(gs.graph.GetOps())
}
