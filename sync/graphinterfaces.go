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
	GetGraph(graphID string) OperationDAG
	AddGraph(graphID string)
	RemoveGraph(graphID string)
}

type AutomaticGraphSynchronizationManager interface {
	GraphSynchronizationManager
	SyncGraph(graphID string)
	GetGraphProvider(graphID string) GraphProvider
}

type ManualGraphSynchronizationManager interface {
	GraphSynchronizationManager
	SyncGraph(graphID string, peer peer.ID)
}

// MultiWriterIPNS supports multiwriter modification of an object where the modifications are represented by DAG nodes containing Cids of operations on the object
type MultiWriterIPNS interface {
	AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid)
	GetLatestVersionHistories() []DagNode // Each DagNode returned represents one possible version of the data and the history leading up to it
}
