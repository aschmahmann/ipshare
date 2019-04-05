package sync

import (
	"context"
	fmt "fmt"
	"log"
	mrand "math/rand"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	host "github.com/libp2p/go-libp2p-host"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	"github.com/pkg/errors"

	multihash "github.com/multiformats/go-multihash"

	lutils "github.com/aschmahmann/ipshare/utils"
)

type opSetGraph struct {
	mux     sync.Mutex
	GraphID *cid.Cid

	Root    DagNode
	NodeSet map[cid.Cid]DagNode
}

type setSyncGraphProvider struct {
	opSetGraph

	peers []peer.ID
	host  host.Host
}

func (gp *opSetGraph) ReceiveUpdates(ops ...*AddNodeOperation) {
	gp.mux.Lock()
	gp.internalUpdate(ops...)
	gp.mux.Unlock()
}

func (gp *opSetGraph) internalUpdate(ops ...*AddNodeOperation) {
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

func (gp *opSetGraph) printNodes() {
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

func (gp *opSetGraph) AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid) {
	gp.ReceiveUpdates(&AddNodeOperation{Value: newCid, Parents: prevCids})
}

func (gp *opSetGraph) GetLatestVersionHistories() []DagNode {
	node := gp.GetRoot()
	visted := make(map[DagNode]struct{})
	visted[node] = struct{}{}
	return getLeafNodes(node, visted)
}

func (gp *setSyncGraphProvider) Update(op *AddNodeOperation) {
	gp.ReceiveUpdates(op)
	SendFullGraph(gp, gp.GraphID, gp.host, gp.peers)
}

func (gp *setSyncGraphProvider) SyncGraph() {
	SendFullGraph(gp, gp.GraphID, gp.host, gp.peers)
}

func (gp *opSetGraph) GetOps() []*AddNodeOperation {
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

func (gp *opSetGraph) GetDagNodes() map[cid.Cid]DagNode {
	ops := make(map[cid.Cid]DagNode)
	gp.mux.Lock()
	for k, v := range gp.NodeSet {
		ops[k] = v
	}
	gp.mux.Unlock()
	return ops
}

func (gp *opSetGraph) TryGetNode(nodeID cid.Cid) (DagNode, bool) {
	gp.mux.Lock()
	node, found := gp.NodeSet[nodeID]
	gp.mux.Unlock()
	return node, found
}

func (gp *opSetGraph) GetRoot() DagNode {
	return gp.Root
}

func SendFullGraph(gp OperationDAG, graphID *cid.Cid, ha host.Host, peers []peer.ID) int {
	// Send full graph to all of our peers so they can incorporate our changes
	waiting := make(chan error, len(peers))

	ops := gp.GetOps()
	for _, p := range peers {
		peer := p
		if peer == ha.ID() {
			waiting <- errors.New("Tried to connect to self peer")
			continue
		}

		s, err := ha.NewStream(context.Background(), peer, gsyncProtocolID)
		if err != nil {
			waiting <- err
			continue
		}
		ps := lutils.NewProtectedStream(s)
		defer ps.Close()

		msg := &FullSendGSync{GraphID: graphID, Operations: ops}
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
	for range peers {
		err := <-waiting
		if err == nil {
			peersUpdated++
		}
	}
	return peersUpdated
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
	mux           sync.RWMutex
	host          host.Host
	graphs        map[cid.Cid]GraphProvider
	graphUpdaters map[cid.Cid]GraphUpdater
	storage       IPNSLocalStorage
	rng           *mrand.Rand
}

func (gs *defaultGraphSynchronizer) GetGraphProvider(IPNSKey cid.Cid) GraphProvider {
	gs.mux.RLock()
	graph, ok := gs.graphs[IPNSKey]
	gs.mux.RUnlock()
	if !ok {
		return nil
	}
	return graph
}

func (gs *defaultGraphSynchronizer) GetGraph(IPNSKey cid.Cid) OperationDAG {
	return gs.GetGraphProvider(IPNSKey)
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

	newGP := &setSyncGraphProvider{opSetGraph: opSetGraph{GraphID: &IPNSKey, NodeSet: make(map[cid.Cid]DagNode)}, host: gs.host}
	if len(startOps) > 0 {
		newGP.ReceiveUpdates(startOps...)
	}
	gs.graphs[IPNSKey] = newGP
	newGP.peers = peerIDs

	SendFullGraph(newGP, newGP.GraphID, newGP.host, newGP.peers)

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			SendFullGraph(newGP, newGP.GraphID, newGP.host, newGP.peers)
		}
	}()
}

//TODO: Cancel gsync if it is running
func (gs *defaultGraphSynchronizer) RemoveGraph(IPNSKey cid.Cid) {
	gs.mux.Lock()
	delete(gs.graphs, IPNSKey)
	gs.mux.Unlock()
}

func (gs *defaultGraphSynchronizer) SyncGraph(IPNSKey *cid.Cid) {
	gs.GetGraphProvider(*IPNSKey).SyncGraph()
}

//NewGraphSychronizer Creates a GraphSynchronizationManager that manages the updates to the graphs
func NewGraphSychronizer(ha host.Host, storage IPNSLocalStorage, rngSrc mrand.Source) AutomaticGraphSynchronizationManager {
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

func NewManualGraphSychronizer(ha host.Host) ManualGraphSynchronizationManager {
	gs := &manualGraphSynchronizer{
		host:   ha,
		graphs: make(map[cid.Cid]OperationDAG),
	}

	// Setup incoming gsync connections to perform gsync
	ha.SetStreamHandler(protocol.ID("/gsync/1.0.0"), func(s net.Stream) {
		go asyncGsyncReceiver(gs, s, make(chan net.Stream, 1))
	})

	return gs
}

type manualGraphSynchronizer struct {
	mux    sync.RWMutex
	host   host.Host
	graphs map[cid.Cid]OperationDAG
}

func (gs *manualGraphSynchronizer) GetGraph(IPNSKey cid.Cid) OperationDAG {
	gs.mux.RLock()
	graph, ok := gs.graphs[IPNSKey]
	gs.mux.RUnlock()
	if !ok {
		return nil
	}
	return graph
}

func (gs *manualGraphSynchronizer) AddGraph(IPNSKey cid.Cid) {
	gs.mux.Lock()
	defer gs.mux.Unlock()

	if _, ok := gs.graphs[IPNSKey]; ok {
		return
	}

	graph := &opSetGraph{GraphID: &IPNSKey, NodeSet: make(map[cid.Cid]DagNode)}
	graph.ReceiveUpdates(&AddNodeOperation{Parents: []*cid.Cid{}, Value: &IPNSKey})
	gs.graphs[IPNSKey] = graph
}

//TODO: Cancel gsync if it is running
func (gs *manualGraphSynchronizer) RemoveGraph(IPNSKey cid.Cid) {
	gs.mux.Lock()
	delete(gs.graphs, IPNSKey)
	gs.mux.Unlock()
}

func (gs *manualGraphSynchronizer) SyncGraph(IPNSKey *cid.Cid, peerID peer.ID) {
	if gs.host.ID() == peerID {
		return
	}

	s, err := gs.host.NewStream(context.Background(), peerID, gsyncProtocolID)
	if err != nil {
		panic(err)
	}

	ps := lutils.NewProtectedStream(s)
	defer ps.Close()

	msg := &RequestFullSendGSync{GraphID: IPNSKey}
	msgBytes, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	gsyncMsg := &GSyncMessage{MessageType: REQUEST_FULL_GRAPH, Msg: msgBytes}
	if err = lutils.WriteToProtectedStream(ps, gsyncMsg); err != nil {
		panic(err)
	}

	returnMsg := &GSyncMessage{}
	if err := lutils.ReadFromProtectedStream(ps, returnMsg); err != nil {
		panic(err)
	}

	typedMsg := &FullSendGSync{}
	if err := typedMsg.Unmarshal(gsyncMsg.Msg); err != nil {
		panic(errors.Wrap(err, "Could not unmarshal message"))
	}

	graphID := *typedMsg.GraphID

	graph := gs.GetGraph(graphID)

	if graph == nil {
		err = fmt.Errorf("The Graph:%v could not be found", graphID)
		panic(err)
	}

	graph.ReceiveUpdates(typedMsg.Operations...)

	//SendFullGraph(gs.GetGraph(*IPNSKey), IPNSKey, gs.host, []peer.ID{peerID})
}
