package sync

import (
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

type OperationDAG interface {
	MultiWriterIPNS
	ReceiveUpdates(...*AddNodeOperation)
	GetOps() []*AddNodeOperation
	GetDagNodes() map[cid.Cid]DagNode
	TryGetNode(nodeID cid.Cid) (DagNode, bool)
	GetRoot() DagNode
}

type GraphUpdater func(*AddNodeOperation)

// GraphProvider Manages a graph of operations, including broadcasting and receiving updates
type GraphProvider interface {
	OperationDAG
	Update(*AddNodeOperation)
	SyncGraph()
}

// GraphSynchronizationManager manages the synchronization of multiple graphs
type GraphSynchronizationManager interface {
	GetGraph(IPNSKey cid.Cid) OperationDAG
	AddGraph(IPNSKey cid.Cid)
	RemoveGraph(IPNSKey cid.Cid)
}

type AutomaticGraphSynchronizationManager interface {
	GraphSynchronizationManager
	SyncGraph(IPNSKey *cid.Cid)
	GetGraphProvider(IPNSKey cid.Cid) GraphProvider
}

type ManualGraphSynchronizationManager interface {
	GraphSynchronizationManager
	SyncGraph(IPNSKey *cid.Cid, peer peer.ID)
}

// MultiWriterIPNS supports multiwriter modification of an object where the modifications are represented by DAG nodes containing Cids of operations on the object
type MultiWriterIPNS interface {
	AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid)
	GetLatestVersionHistories() []DagNode // Each DagNode returned represents one possible version of the data and the history leading up to it
}
