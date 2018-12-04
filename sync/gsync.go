package sync

import (
	"context"
	host "github.com/libp2p/go-libp2p-host"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-protocol"
	"github.com/pkg/errors"
	"log"
	mrand "math/rand"
	"sync"

	lutils "github.com/aschmahmann/ipshare/utils"
	"github.com/ipfs/go-key"
)

type ipnsKey struct {
	key.Key
}

func (key *ipnsKey) Marshal() ([]byte, error) {
	return key.MarshalJSON()
}

func (key *ipnsKey) Unmarshal(mk []byte) error {
	return key.UnmarshalJSON(mk)
}

func (key *ipnsKey) Size() int {
	m, e := key.Marshal()
	if e != nil {
		return -1
	}
	return len(m)
}

type setSyncGraphProvider struct {
	mux     sync.Mutex
	GraphID ipnsKey

	Root    DagNode
	NodeSet map[key.Key]DagNode

	updateStreams map[net.Stream]*lutils.RWMutex
}

type DagNode interface {
	GetValue() key.Key // Content format for the Cid is {ParentCids[], DataCid}
	GetParents() map[key.Key]DagNode
	GetChildren() map[key.Key]DagNode
	AddChildren(...DagNode) int //Returns the number of children added
	GetAsOp() *SingleOp
}

type OpBasedDagNode struct {
	nodeOp   *SingleOp
	Children map[key.Key]DagNode
	Parents  map[key.Key]DagNode
}

func (node *OpBasedDagNode) AddChildren(nodes ...DagNode) int {
	numAdds := 0
	for _, n := range nodes {
		if _, ok := node.Children[n.GetValue()]; !ok {
			node.Children[n.GetValue()] = n
			numAdds++
		}
	}
	return numAdds
}

func (node *OpBasedDagNode) GetChildren() map[key.Key]DagNode {
	return node.Children
}

func (node *OpBasedDagNode) GetParents() map[key.Key]DagNode {
	return node.Parents
}

func (node *OpBasedDagNode) GetValue() key.Key {
	return key.Key(node.nodeOp.Value)
}

func (node *OpBasedDagNode) GetAsOp() *SingleOp {
	return node.nodeOp
}

type GraphSynchronizer interface {
	GetGraphProvider(IPNSKey key.Key) GraphProvider
}

type MultiWriterIPNS interface {
	AddNewVersion(newCid key.Key, prevCids ...key.Key)
	GetLatestVersionHistories() []DagNode // Each DagNode returned represents one possible version of the data and the history leading up to it
}

type GossipMultiWriterIPNS struct {
	IPNSKey    ipnsKey
	Gsync      GraphSynchronizer
	branchEnds []DagNode
}

func (ipns *GossipMultiWriterIPNS) AddNewVersion(newCid key.Key, prevCids ...key.Key) {
	parentsArr := make([]string, len(prevCids))
	for i, p := range prevCids {
		parentsArr[i] = string(p)
	}
	ipns.Gsync.GetGraphProvider(ipns.IPNSKey.Key).Update(&SingleOp{Value: string(newCid), Parents: parentsArr})
}

func (ipns *GossipMultiWriterIPNS) GetLatestVersionHistories() []DagNode {
	gp := ipns.Gsync.GetGraphProvider(ipns.IPNSKey.Key)
	node := gp.GetRoot()
	return getLeafNodes(node)
}

func getLeafNodes(root DagNode) []DagNode {
	children := root.GetChildren()
	if len(children) == 0 {
		return []DagNode{root}
	}

	leaves := make([]DagNode, 0, len(children))
	for _, c := range children {
		childLeaves := getLeafNodes(c)
		leaves = append(leaves, childLeaves...)
	}
	return leaves
}

// GraphProvider Manages a graph of operations, including broadcasting and receiving updates
type GraphProvider interface {
	ReceiveUpdates(...*SingleOp)
	Update(*SingleOp)
	GetOps() []*SingleOp
	GetDagNodes() []DagNode
	GetRoot() DagNode
}

// GetID Gets the ID of the Operation
func (op *SingleOp) GetID() key.Key {
	return key.Key(op.Value)
}

func (gp *setSyncGraphProvider) ReceiveUpdates(ops ...*SingleOp) {
	gp.mux.Lock()
	gp.internalUpdate(ops...)
	gp.mux.Unlock()
}

func (gp *setSyncGraphProvider) internalUpdate(ops ...*SingleOp) {
	for _, op := range ops {
		opID := op.GetID()

		storedOp, ok := gp.NodeSet[opID]
		if !ok {
			node, err := gp.newOpBasedDagNode(op)
			if err != nil {
				log.Fatal(err)
			}
			storedOp = node
			gp.NodeSet[opID] = storedOp
			continue
		}

		for _, p := range op.GetParents() {
			parentID := key.Key(p)
			if storedParentNode, ok := gp.NodeSet[parentID]; ok {
				storedParentNode.AddChildren(storedOp)
			} else {
				log.Fatal(errors.New("Cannot find parent in graph"))
			}
		}
	}

	if gp.Root == nil {
		var rootCandidate DagNode
		for _, n := range gp.NodeSet {
			if len(n.GetParents()) == 0 {
				if rootCandidate != nil {
					log.Fatal(errors.New("Cannot have multiple roots"))
				}
				rootCandidate = n
			}
		}
		if rootCandidate == nil {
			log.Fatal(errors.New("Must have at least one root"))
		}
		gp.Root = rootCandidate
	}
}

func (gp *setSyncGraphProvider) Update(op *SingleOp) {
	gp.mux.Lock()

	gp.internalUpdate(op)

	// Go through all the users we are connected with and send them updates
	for k, v := range gp.updateStreams {
		stream := lutils.ProtectedStream{Stream: k, RWMutex: *v}
		go func() {
			if err := lutils.WriteToProtectedStream(stream, op); err != nil {
				log.Printf("sending update to %v", stream.Conn().RemotePeer())
				log.Print(err)
			}
		}()
	}
	gp.mux.Unlock()
}

func (gp *setSyncGraphProvider) GetOps() []*SingleOp {
	gp.mux.Lock()
	ops := make([]*SingleOp, len(gp.NodeSet))
	i := 0
	for _, v := range gp.NodeSet {
		ops[i] = v.GetAsOp()
		i++
	}
	gp.mux.Unlock()
	return ops
}

func (gp *setSyncGraphProvider) GetDagNodes() []DagNode {
	gp.mux.Lock()
	ops := make([]DagNode, len(gp.NodeSet))
	i := 0
	for _, v := range gp.NodeSet {
		ops[i] = v
		i++
	}
	gp.mux.Unlock()
	return ops
}

func (gp *setSyncGraphProvider) newOpBasedDagNode(nodeOp *SingleOp) (*OpBasedDagNode, error) {
	parents := make(map[key.Key]DagNode)
	children := make(map[key.Key]DagNode)

	node := &OpBasedDagNode{nodeOp: nodeOp, Parents: parents, Children: children}

	for _, p := range nodeOp.Parents {
		if pNode, ok := gp.NodeSet[key.Key(p)]; ok {
			parents[key.Key(p)] = pNode
			pNode.AddChildren(node)
		} else {
			return nil, errors.New("Could not find parent in graph")
		}
	}
	return node, nil
}

func (gp *setSyncGraphProvider) GetRoot() DagNode {
	return gp.Root
}

// GSyncState The state of the Graph Syncronization algorithm
type GSyncState int

// The possible states of the Graph Syncronization algorithm
const (
	UNKNOWN GSyncState = iota
	SEND
	RECEIVE
	DONE
)

// Wire protocol:
// SEND to other (graphID, ops) -> stream return ops
// RECEIVE from other (graphID, ops) -> stream return ops

func gsync(gs *defaultGraphSynchronizer, graphID *ipnsKey, s net.Stream, state GSyncState, done chan bool) error {
	streamMux := &lutils.RWMutex{R: &sync.Mutex{}, W: &sync.Mutex{}}
	ps := lutils.ProtectedStream{Stream: s, RWMutex: *streamMux}

	var err error
	startState := state
	defer func() {
		if err != nil {
			log.Printf("stream handler error: %v | state = %v", s.Conn().LocalPeer(), startState)
		}
	}()

	var gp GraphProvider

	for state != DONE {
		switch state {
		case SEND:
			// If sending data synchronize by writing the graphID to the stream, followed by
			// the local graph operations to the stream.
			// Then read the remote graph operations and merge them into the local graph

			if err = lutils.WriteToProtectedStream(ps, graphID); err != nil {
				return errors.Wrap(err, "Could not write operations to stream")
			}

			gp = gs.graphs[*graphID]
			ops := &GraphOps{Ops: gp.GetOps()}

			if err = lutils.WriteToProtectedStream(ps, ops); err != nil {
				return errors.Wrap(err, "Could not write operations to stream")
			}
			incomingOps := &GraphOps{}
			if err = lutils.ReadFromProtectedStream(ps, incomingOps); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}

			gp.ReceiveUpdates(incomingOps.GetOps()...)
			state = DONE
		case RECEIVE:
			// If receiving data synchronize by reading the remote graphID followed by the
			// remote graph operations from the stream and then merge them into the local graph.
			// Then write the resulting local graph operations to the stream

			graphID := &ipnsKey{}
			if err = lutils.ReadFromProtectedStream(ps, graphID); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}

			incomingOps := &GraphOps{}
			if err = lutils.ReadFromProtectedStream(ps, incomingOps); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}

			gs.mux.Lock()
			graph, ok := gs.graphs[*graphID]
			gp = graph
			if !ok {
				ops := make(map[key.Key]*SingleOp)
				for _, op := range incomingOps.Ops {
					ops[key.Key(op.Value)] = op
				}
				updateMap := make(map[net.Stream]*lutils.RWMutex)
				updateMap[s] = &lutils.RWMutex{R: &sync.Mutex{}, W: &sync.Mutex{}}

				gp = &setSyncGraphProvider{GraphID: *graphID, NodeSet: make(map[key.Key]DagNode),
					updateStreams: updateMap}
				gp.ReceiveUpdates(incomingOps.Ops...)
				gs.graphs[*graphID] = gp
				gs.mux.Unlock()
			} else {
				gs.mux.Unlock()
				gp.ReceiveUpdates(incomingOps.Ops...)
			}

			sendOps := &GraphOps{Ops: gp.GetOps()}
			if err = lutils.WriteToProtectedStream(ps, sendOps); err != nil {
				return errors.Wrap(err, "Could not write operations to stream")
			}
			state = DONE
		}
	}

	// After completing the initial synchronization indicate that it is completed and then start the update read loop

	done <- true

	for {
		incomingOp := &SingleOp{}

		lutils.ReadFromProtectedStream(ps, incomingOp)
		if err == nil {
			gp.ReceiveUpdates(incomingOp)
		} else {
			log.Fatalln(err)
		}
	}
}

type IPNSLocalStorage interface {
	GetPeers(IPNSKey key.Key) []peer.ID
	GetOps(IPNSKey key.Key) []*SingleOp
}

type defaultGraphSynchronizer struct {
	mux    sync.Mutex
	graphs map[ipnsKey]GraphProvider
}

func (gs *defaultGraphSynchronizer) GetGraphProvider(IPNSKey key.Key) GraphProvider {
	return gs.graphs[ipnsKey{IPNSKey}]
}

//NewGraphSychronizer Creates a GraphSynchronizer that manages the updates to a graph
func NewGraphSychronizer(ha host.Host, ipnsKeys []key.Key, storage IPNSLocalStorage, rngSrc mrand.Source) GraphSynchronizer {
	// Create a pseudorandom number generator from the given pseudorandom source
	rng := mrand.New(rngSrc)

	gs := &defaultGraphSynchronizer{graphs: make(map[ipnsKey]GraphProvider)}

	// Setup incoming gsync connections to perform gsync
	ha.SetStreamHandler(protocol.ID("/gsync/1.0.0"), func(s net.Stream) {
		go gsync(gs, nil, s, RECEIVE, make(chan bool, 1))
	})

	for _, k := range ipnsKeys {
		peerIDs := storage.GetPeers(k)
		ipnsK := &ipnsKey{k}

		newGP := &setSyncGraphProvider{GraphID: *ipnsK, NodeSet: make(map[key.Key]DagNode),
			updateStreams: make(map[net.Stream]*lutils.RWMutex)}
		startOps := storage.GetOps(k)
		if len(startOps) > 0 {
			newGP.ReceiveUpdates(startOps...)
		}
		gs.graphs[*ipnsK] = newGP

		// Shuffle the peers
		shuffledPeers := make([]peer.ID, len(peerIDs))
		for i, v := range rng.Perm(len(peerIDs)) {
			shuffledPeers[v] = peerIDs[i]
		}

		syncedStreams := make(chan bool, len(shuffledPeers))

		// Attempt outgoing gsync connection to each of the possible peers
		// TODO: May not need to maintain all peer connections, just a subset
		for _, peer := range shuffledPeers {
			stream, err := ha.NewStream(context.Background(), peer, protocol.ID("/gsync/1.0.0"))
			if err != nil {
				syncedStreams <- false
				log.Print(err)
				continue
			}

			newGP.updateStreams[stream] = &lutils.RWMutex{R: &sync.Mutex{}, W: &sync.Mutex{}}

			go gsync(gs, ipnsK, stream, SEND, syncedStreams)
		}

		for index := 0; index < len(shuffledPeers); index++ {
			<-syncedStreams
		}
	}

	return gs
}
