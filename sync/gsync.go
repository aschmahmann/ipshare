package sync

import (
	"context"
	cid "github.com/ipfs/go-cid"
	host "github.com/libp2p/go-libp2p-host"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	"github.com/pkg/errors"
	"log"
	mrand "math/rand"
	"sync"
	"time"

	multihash "github.com/multiformats/go-multihash"

	lutils "github.com/aschmahmann/ipshare/utils"
)

type setSyncGraphProvider struct {
	mux     sync.Mutex
	GraphID *cid.Cid

	Root    DagNode
	NodeSet map[cid.Cid]DagNode

	updateStreams map[net.Stream]*lutils.RWMutex
	peers         []peer.ID
	host          host.Host
}

// DagNode is a DAG node where both the nodes and their content are described by Cids
type DagNode interface {
	GetNodeID() cid.Cid // Content format for the Cid is {ParentCids[], DataCid}
	GetValue() cid.Cid  // Returns the Cid of the content
	GetParents() map[cid.Cid]DagNode
	GetChildren() map[cid.Cid]DagNode
	AddChildren(...DagNode) int // Returns the number of children added
	GetAsOp() *AddNodeOperation
}

// OpBasedDagNode is a DAG node wherein every node is an operation on the graph's state
type OpBasedDagNode struct {
	nodeOp   *AddNodeOperation
	Children map[cid.Cid]DagNode
	Parents  map[cid.Cid]DagNode
	nodeCID  cid.Cid
}

// AddChildren returns the number of children added
func (node *OpBasedDagNode) AddChildren(nodes ...DagNode) int {
	numAdds := 0
	for _, n := range nodes {
		childCid := n.GetNodeID()
		if _, ok := node.Children[childCid]; !ok {
			node.Children[childCid] = n
			numAdds++
		}
	}
	return numAdds
}

// GetChildren returns a map of Cid -> DagNode. Do not modify
func (node *OpBasedDagNode) GetChildren() map[cid.Cid]DagNode {
	return node.Children
}

// GetParents returns a map of Cid -> DagNode. Do not modify
func (node *OpBasedDagNode) GetParents() map[cid.Cid]DagNode {
	return node.Parents
}

// GetNodeID returns the Cid of the node (as opposed to the node's content)
func (node *OpBasedDagNode) GetNodeID() cid.Cid {
	return node.nodeCID
}

// GetValue returns the Cid of the node's value (as opposed to the node itself)
func (node *OpBasedDagNode) GetValue() cid.Cid {
	return *node.nodeOp.Value
}

// GetAsOp returns the operation that created the node
func (node *OpBasedDagNode) GetAsOp() *AddNodeOperation {
	return node.nodeOp
}

// GraphSynchronizer manages the synchronization of multiple graphs
type GraphSynchronizer interface {
	GetGraphProvider(IPNSKey cid.Cid) GraphProvider
	AddGraph(IPNSKey cid.Cid)
	RemoveGraph(IPNSKey cid.Cid)
}

// MultiWriterIPNS supports multiwriter modification of an object where the modifications are represented by DAG nodes containing Cids of operations on the object
type MultiWriterIPNS interface {
	AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid)
	GetLatestVersionHistories() []DagNode // Each DagNode returned represents one possible version of the data and the history leading up to it
}

// GossipMultiWriterIPNS provides a MultiWriterIPNS layer on top of a GraphSynchronizer
type GossipMultiWriterIPNS struct {
	IPNSKey    cid.Cid
	Gsync      GraphSynchronizer
	branchEnds []DagNode
}

// AddNewVersion modify the object into a new version based on the previous modifications it depends on
func (ipns *GossipMultiWriterIPNS) AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid) {
	ipns.Gsync.GetGraphProvider(ipns.IPNSKey).Update(&AddNodeOperation{Value: newCid, Parents: prevCids})
}

// GetRoot returns the root DagNode
func (ipns *GossipMultiWriterIPNS) GetRoot() DagNode {
	gp := ipns.Gsync.GetGraphProvider(ipns.IPNSKey)
	node := gp.GetRoot()
	return node
}

// GetNumberOfOperations returns the number of Dag nodes/operations processed
func (ipns *GossipMultiWriterIPNS) GetNumberOfOperations() int {
	gp := ipns.Gsync.GetGraphProvider(ipns.IPNSKey)
	nodes := gp.GetDagNodes()
	return len(nodes)
}

// GetLatestVersionHistories Each DagNode returned represents one possible version of the data and the history leading up to it
func (ipns *GossipMultiWriterIPNS) GetLatestVersionHistories() []DagNode {
	gp := ipns.Gsync.GetGraphProvider(ipns.IPNSKey)
	node := gp.GetRoot()
	visted := make(map[DagNode]struct{})
	visted[node] = struct{}{}
	return getLeafNodes(node, visted)
}

func getLeafNodes(root DagNode, visted map[DagNode]struct{}) []DagNode {
	// Use a recursive DFS with a visited set to prevent walking down the same path multiple times
	children := root.GetChildren()
	if len(children) == 0 {
		return []DagNode{root}
	}

	leaves := make([]DagNode, 0, len(children))
	for _, c := range children {
		if _, ok := visted[c]; ok {
			continue
		}
		visted[c] = struct{}{}

		childLeaves := getLeafNodes(c, visted)
		leaves = append(leaves, childLeaves...)
	}
	return leaves
}

// GraphProvider Manages a graph of operations, including broadcasting and receiving updates
type GraphProvider interface {
	ReceiveUpdates(...*AddNodeOperation)
	Update(*AddNodeOperation)
	GetOps() []*AddNodeOperation
	GetDagNodes() []DagNode
	GetRoot() DagNode
}

func (gp *setSyncGraphProvider) ReceiveUpdates(ops ...*AddNodeOperation) {
	gp.mux.Lock()
	gp.internalUpdate(ops...)
	gp.mux.Unlock()
}

func (gp *setSyncGraphProvider) internalUpdate(ops ...*AddNodeOperation) {
	for _, op := range ops {

		builder := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: -1}
		opBytes, err := op.Marshal()
		if err != nil {
			log.Print(err)
			continue
		}
		opID, err := builder.Sum(opBytes)
		if err != nil {
			log.Print(err)
			continue
		}

		storedOp, ok := gp.NodeSet[opID]
		// If the incoming node is new create the new node
		if !ok {
			numParents := len(op.Parents)
			parentNodes := make([]DagNode, numParents)
			for i, j := 0, 0; i < numParents; i, j = i+1, j+1 {
				parentCid := *op.Parents[j]
				p, ok := gp.NodeSet[parentCid]
				if ok {
					parentNodes[i] = p
				} else {
					log.Printf("parentCID: %v", parentCid)
					gp.printNodes()
					numParents--
					i--
				}
			}

			nodeParents := make(map[cid.Cid]DagNode)
			for _, p := range parentNodes[:numParents] {
				nodeParents[p.GetNodeID()] = p
			}

			storedOp = &OpBasedDagNode{
				nodeOp:   op,
				Children: make(map[cid.Cid]DagNode),
				Parents:  nodeParents,
				nodeCID:  opID}

			if err != nil {
				log.Print(err)
				continue
			}
			gp.NodeSet[opID] = storedOp
		}

		// Otherwise, just make sure all the parents have pointers to the new children. TODO: Is this necessary?
		for _, p := range op.Parents {
			parentID := *p
			if storedParentNode, ok := gp.NodeSet[parentID]; ok {
				numAdds := storedParentNode.AddChildren(storedOp)
				_ = numAdds
			} else {
				log.Printf("OpID: %v", opID)
				log.Printf("Parent: %v", parentID)
				gp.printNodes()

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

func (gp *setSyncGraphProvider) printNodes() {
	pp := func(d DagNode) []string {
		par := d.GetParents()
		ret := make([]string, len(par))
		i := 0
		for k := range par {
			ret[i] = k.String()
			i++
		}
		return ret
	}
	for k, v := range gp.NodeSet {
		log.Printf("Node: %v | %v", k, pp(v))
	}
}

func (gp *setSyncGraphProvider) Update(op *AddNodeOperation) {
	gp.mux.Lock()
	gp.internalUpdate(op)

	gp.mux.Unlock()
	gp.sendFullUpdate()
}

func (gp *setSyncGraphProvider) GetOps() []*AddNodeOperation {
	gp.mux.Lock()
	numOps := len(gp.NodeSet)
	ops := make([]*AddNodeOperation, numOps)
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

func dfsTopologicalSort(n DagNode, ts []*AddNodeOperation, index *int, found map[DagNode]bool) error {
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

func (gp *setSyncGraphProvider) GetRoot() DagNode {
	return gp.Root
}

// IPNSLocalStorage is a read interface for data that an MWIPNS node might need
type IPNSLocalStorage interface {
	GetIPNSKeys() []cid.Cid
	GetPeers(IPNSKey cid.Cid) []peer.ID
	GetOps(IPNSKey cid.Cid) []*AddNodeOperation
}

// UpdateableIPNSLocalStorage is a read/write interface for data that an MWIPNS node might need
type UpdateableIPNSLocalStorage interface {
	IPNSLocalStorage
	AddPeers(IPNSKey cid.Cid, peers ...peer.ID)
	AddOps(IPNSKey cid.Cid, ops ...*AddNodeOperation)
}

type localStorageType struct {
	peers map[peer.ID]bool
	ops   []*AddNodeOperation
}

type memoryIPNSLocalStorage struct {
	Storage map[cid.Cid]*localStorageType
}

// NewMemoryIPNSLocalStorage returns a memory backed UpdateableIPNSLocalStorage
func NewMemoryIPNSLocalStorage() UpdateableIPNSLocalStorage {
	return &memoryIPNSLocalStorage{Storage: make(map[cid.Cid]*localStorageType)}
}

func (ls *memoryIPNSLocalStorage) GetIPNSKeys() []cid.Cid {
	IDs := make([]cid.Cid, len(ls.Storage))
	i := 0
	for k := range ls.Storage {
		IDs[i] = k
		i++
	}
	return IDs
}

func (ls *memoryIPNSLocalStorage) GetPeers(IPNSKey cid.Cid) []peer.ID {
	internalStorage := ls.Storage[IPNSKey]
	peers := make([]peer.ID, len(internalStorage.peers))
	i := 0
	for p := range internalStorage.peers {
		peers[i] = p
		i++
	}
	return peers
}

func (ls *memoryIPNSLocalStorage) GetOps(IPNSKey cid.Cid) []*AddNodeOperation {
	internalStorage := ls.Storage[IPNSKey]
	return internalStorage.ops
}

func (ls *memoryIPNSLocalStorage) AddPeers(IPNSKey cid.Cid, peers ...peer.ID) {
	v, ok := ls.Storage[IPNSKey]
	if !ok {
		v = &localStorageType{peers: make(map[peer.ID]bool), ops: make([]*AddNodeOperation, 0)}
		ls.Storage[IPNSKey] = v
	}
	for _, p := range peers {
		v.peers[p] = true
	}
}

func (ls *memoryIPNSLocalStorage) AddOps(IPNSKey cid.Cid, ops ...*AddNodeOperation) {
	v, ok := ls.Storage[IPNSKey]
	if !ok {
		v = &localStorageType{peers: make(map[peer.ID]bool), ops: make([]*AddNodeOperation, 0)}
		ls.Storage[IPNSKey] = v
	}

	newOps := append(v.ops, ops...)
	v.ops = newOps
}

type defaultGraphSynchronizer struct {
	mux     sync.RWMutex
	host    host.Host
	graphs  map[cid.Cid]GraphProvider
	storage IPNSLocalStorage
	rng     *mrand.Rand
}

func (gs *defaultGraphSynchronizer) GetGraphProvider(IPNSKey cid.Cid) GraphProvider {
	return gs.graphs[IPNSKey]
}

const gsyncProtocolID = protocol.ID("/gsync/1.0.0")

func (gs *defaultGraphSynchronizer) AddGraph(IPNSKey cid.Cid) {
	// Setup graph with starting state and peers, then send periodic updates

	gs.mux.Lock()
	defer gs.mux.Unlock()

	if _, ok := gs.graphs[IPNSKey]; ok {
		return
	}

	startOps := gs.storage.GetOps(IPNSKey)
	if len(startOps) == 0 {
		log.Fatal(errors.New("Cannot add graph with no starting state"))
	}

	peerIDs := gs.storage.GetPeers(IPNSKey)

	newGP := &setSyncGraphProvider{GraphID: &IPNSKey, NodeSet: make(map[cid.Cid]DagNode),
		updateStreams: make(map[net.Stream]*lutils.RWMutex), host: gs.host}
	if len(startOps) > 0 {
		newGP.ReceiveUpdates(startOps...)
	}
	gs.graphs[IPNSKey] = newGP
	newGP.peers = peerIDs

	newGP.sendFullUpdate()

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			newGP.sendFullUpdate()
		}
	}()
}

func (gp *setSyncGraphProvider) sendFullUpdate() int {
	// Send full graph to all of our peers so they can incorporate our changes
	// TODO: May not need to create connectections to all peers, just a subset
	waiting := make(chan error, len(gp.peers))

	ops := gp.GetOps()
	for _, p := range gp.peers {
		peer := p
		if peer == gp.host.ID() {
			waiting <- errors.New("Tried to connect to self peer")
			continue
		}

		s, err := gp.host.NewStream(context.Background(), peer, gsyncProtocolID)
		if err != nil {
			waiting <- err
			continue
		}
		ps := lutils.NewProtectedStream(s)
		defer ps.Close()

		msg := &FullSendGSync{GraphID: gp.GraphID, Operations: ops}
		msgBytes, err := msg.Marshal()
		if err != nil {
			waiting <- err
			continue
		}

		gsynMsg := &GSyncMessage{MessageType: FULL_GRAPH, Msg: msgBytes}
		if err = lutils.WriteToProtectedStream(ps, gsynMsg); err != nil {
			waiting <- err
			continue
		}
		waiting <- nil
	}

	peersUpdated := 0
	for range gp.peers {
		err := <-waiting
		if err == nil {
			peersUpdated++
		}
	}
	return peersUpdated
}

//TODO: Cancel gsync if it is running
func (gs *defaultGraphSynchronizer) RemoveGraph(IPNSKey cid.Cid) {
	gs.mux.Lock()
	delete(gs.graphs, IPNSKey)
	gs.mux.Unlock()
}

//NewGraphSychronizer Creates a GraphSynchronizer that manages the updates to a graph
func NewGraphSychronizer(ha host.Host, storage IPNSLocalStorage, rngSrc mrand.Source) GraphSynchronizer {
	// Create a pseudorandom number generator from the given pseudorandom source
	rng := mrand.New(rngSrc)

	gs := &defaultGraphSynchronizer{host: ha, graphs: make(map[cid.Cid]GraphProvider), storage: storage, rng: rng}

	for _, k := range storage.GetIPNSKeys() {
		gs.AddGraph(k)
	}

	// Setup incoming gsync connections to perform gsync
	ha.SetStreamHandler(protocol.ID("/gsync/1.0.0"), func(s net.Stream) {
		go asyncGsyncReceiver(gs, s, make(chan net.Stream, 1))
	})

	return gs
}
