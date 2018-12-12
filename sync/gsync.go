package sync

import (
	"context"
	"fmt"
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

type ipnsKey key.Key

func (k ipnsKey) Marshal() ([]byte, error) {
	genericKey := key.Key(k)
	return genericKey.MarshalJSON()
}

func (k *ipnsKey) Unmarshal(mk []byte) error {
	genericKey := new(key.Key)
	if err := genericKey.UnmarshalJSON(mk); err != nil {
		return err
	}
	*k = ipnsKey(*genericKey)
	return nil
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
	AddGraph(IPNSKey key.Key)
	RemoveGraph(IPNSKey key.Key)
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
	ipns.Gsync.GetGraphProvider(key.Key(ipns.IPNSKey)).Update(&SingleOp{Value: string(newCid), Parents: parentsArr})
}

func (ipns *GossipMultiWriterIPNS) GetLatestVersionHistories() []DagNode {
	gp := ipns.Gsync.GetGraphProvider(key.Key(ipns.IPNSKey))
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
	numOps := len(gp.NodeSet)
	ops := make([]*SingleOp, numOps)
	i := numOps - 1
	root := gp.GetRoot()

	foundMap := make(map[DagNode]bool)
	err := dfsTopologicalSort(root, ops, &i, foundMap)
	if err != nil {
		log.Fatal(err)
	}

	gp.mux.Unlock()
	return ops
}

func dfsTopologicalSort(n DagNode, ts []*SingleOp, index *int, found map[DagNode]bool) error {
	if permanent, ok := found[n]; ok {
		if !permanent {
			return errors.New("not a DAG, cycle detected")
		}
		return nil
	}
	found[n] = false

	for _, c := range n.GetChildren() {
		dfsTopologicalSort(c, ts, index, found)
	}
	found[n] = true
	ts[*index] = n.GetAsOp()
	*index--
	return nil
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

func gsync(gs *defaultGraphSynchronizer, graphID *ipnsKey, s net.Stream, state GSyncState, done chan net.Stream) error {
	ps := lutils.NewProtectedStream(s)

	isDone := false

	var err error
	startState := state
	defer func() {
		if err != nil {
			log.Printf("stream handler error: %v | state = %v", s.Conn().LocalPeer(), startState)
			log.Print(err)
			closeErr := net.FullClose(s)
			if closeErr != nil {
				log.Print(closeErr)
			}
		}
		if !isDone {
			done <- s
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

			graphID := new(ipnsKey)
			if err = lutils.ReadFromProtectedStream(ps, graphID); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}

			gs.mux.Lock()
			graph, ok := gs.graphs[*graphID]
			gs.mux.Unlock()
			gp = graph

			if !ok {
				err = errors.New(fmt.Sprintf("The Graph:%v could not be found by peer:%v", *graphID, gs.host.ID()))
				return err
			}

			incomingOps := &GraphOps{}
			if err = lutils.ReadFromProtectedStream(ps, incomingOps); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}

			gp.ReceiveUpdates(incomingOps.Ops...)

			sendOps := &GraphOps{Ops: gp.GetOps()}
			if err = lutils.WriteToProtectedStream(ps, sendOps); err != nil {
				return errors.Wrap(err, "Could not write operations to stream")
			}

			if setGp, ok := gp.(*setSyncGraphProvider); ok {
				setGp.updateStreams[ps.Stream] = &ps.RWMutex
			}
			state = DONE
		}
	}

	// After completing the initial synchronization indicate that it is completed and then start the update read loop

	isDone = true
	done <- nil

	for {
		incomingOp := &SingleOp{}

		err = lutils.ReadFromProtectedStream(ps, incomingOp)
		if err == nil {
			gp.ReceiveUpdates(incomingOp)
		} else {
			return errors.Wrap(err, "Failed to read update")
		}
	}
}

type IPNSLocalStorage interface {
	GetIPNSKeys() []key.Key
	GetPeers(IPNSKey key.Key) []peer.ID
	GetOps(IPNSKey key.Key) []*SingleOp
}

type UpdateableIPNSLocalStorage interface {
	IPNSLocalStorage
	AddPeers(IPNSKey key.Key, peers ...peer.ID)
	AddOps(IPNSKey key.Key, ops ...*SingleOp)
}

type localStorageType struct {
	peers map[peer.ID]bool
	ops   []*SingleOp
}

type memoryIPNSLocalStorage struct {
	Storage map[key.Key]*localStorageType
}

func NewMemoryIPNSLocalStorage() UpdateableIPNSLocalStorage {
	return &memoryIPNSLocalStorage{Storage: make(map[key.Key]*localStorageType)}
}

func (ls *memoryIPNSLocalStorage) GetIPNSKeys() []key.Key {
	IDs := make([]key.Key, len(ls.Storage))
	i := 0
	for k := range ls.Storage {
		IDs[i] = k
		i++
	}
	return IDs
}

func (ls *memoryIPNSLocalStorage) GetPeers(IPNSKey key.Key) []peer.ID {
	internalStorage := ls.Storage[IPNSKey]
	peers := make([]peer.ID, len(internalStorage.peers))
	i := 0
	for p := range internalStorage.peers {
		peers[i] = p
		i++
	}
	return peers
}

func (ls *memoryIPNSLocalStorage) GetOps(IPNSKey key.Key) []*SingleOp {
	internalStorage := ls.Storage[IPNSKey]
	return internalStorage.ops
}

func (ls *memoryIPNSLocalStorage) AddPeers(IPNSKey key.Key, peers ...peer.ID) {
	v, ok := ls.Storage[IPNSKey]
	if !ok {
		v = &localStorageType{peers: make(map[peer.ID]bool), ops: make([]*SingleOp, 0)}
		ls.Storage[IPNSKey] = v
	}
	for _, p := range peers {
		v.peers[p] = true
	}
}

func (ls *memoryIPNSLocalStorage) AddOps(IPNSKey key.Key, ops ...*SingleOp) {
	v, ok := ls.Storage[IPNSKey]
	if !ok {
		v = &localStorageType{peers: make(map[peer.ID]bool), ops: make([]*SingleOp, 0)}
		ls.Storage[IPNSKey] = v
	}

	newOps := append(v.ops, ops...)
	v.ops = newOps
}

type defaultGraphSynchronizer struct {
	mux     sync.RWMutex
	host    host.Host
	graphs  map[ipnsKey]GraphProvider
	storage IPNSLocalStorage
	rng     *mrand.Rand
}

func (gs *defaultGraphSynchronizer) GetGraphProvider(IPNSKey key.Key) GraphProvider {
	return gs.graphs[ipnsKey(IPNSKey)]
}

func (gs *defaultGraphSynchronizer) AddGraph(IPNSKey key.Key) {
	gs.mux.Lock()
	defer gs.mux.Unlock()

	if _, ok := gs.graphs[ipnsKey(IPNSKey)]; ok {
		return
	}

	startOps := gs.storage.GetOps(IPNSKey)
	if len(startOps) == 0 {
		log.Fatal(errors.New("Cannot add graph with no starting state"))
	}

	peerIDs := gs.storage.GetPeers(IPNSKey)
	ipnsK := ipnsKey(IPNSKey)

	newGP := &setSyncGraphProvider{GraphID: ipnsK, NodeSet: make(map[key.Key]DagNode),
		updateStreams: make(map[net.Stream]*lutils.RWMutex)}
	if len(startOps) > 0 {
		newGP.ReceiveUpdates(startOps...)
	}
	gs.graphs[ipnsK] = newGP

	// Shuffle the peers
	shuffledPeers := make([]peer.ID, len(peerIDs))
	for i, v := range gs.rng.Perm(len(peerIDs)) {
		shuffledPeers[v] = peerIDs[i]
	}

	syncedStreams := make(chan net.Stream, len(shuffledPeers))

	// Attempt outgoing gsync connection to each of the possible peers
	// TODO: May not need to maintain all peer connections, just a subset
	for _, peer := range shuffledPeers {
		stream, err := gs.host.NewStream(context.Background(), peer, protocol.ID("/gsync/1.0.0"))
		if err != nil {
			syncedStreams <- stream
			log.Print(err)
			continue
		}

		newGP.mux.Lock()
		newGP.updateStreams[stream] = &lutils.RWMutex{R: &sync.Mutex{}, W: &sync.Mutex{}}
		newGP.mux.Unlock()

		go gsync(gs, &ipnsK, stream, SEND, syncedStreams)
	}

	for index := 0; index < len(shuffledPeers); index++ {
		s := <-syncedStreams
		if s != nil {
			newGP.mux.Lock()
			delete(newGP.updateStreams, s)
			newGP.mux.Unlock()
		}
	}
}

//TODO: Cancel gsync if it is running
func (gs *defaultGraphSynchronizer) RemoveGraph(IPNSKey key.Key) {
	gs.mux.Lock()
	delete(gs.graphs, ipnsKey(IPNSKey))
	gs.mux.Unlock()
}

//NewGraphSychronizer Creates a GraphSynchronizer that manages the updates to a graph
func NewGraphSychronizer(ha host.Host, storage IPNSLocalStorage, rngSrc mrand.Source) GraphSynchronizer {
	// Create a pseudorandom number generator from the given pseudorandom source
	rng := mrand.New(rngSrc)

	gs := &defaultGraphSynchronizer{host: ha, graphs: make(map[ipnsKey]GraphProvider), storage: storage, rng: rng}

	for _, k := range storage.GetIPNSKeys() {
		gs.AddGraph(k)
	}

	// Setup incoming gsync connections to perform gsync
	ha.SetStreamHandler(protocol.ID("/gsync/1.0.0"), func(s net.Stream) {
		go gsync(gs, nil, s, RECEIVE, make(chan net.Stream, 1))
	})

	return gs
}
