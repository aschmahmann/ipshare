package sync

import cid "github.com/ipfs/go-cid"

// GSMultiWriterIPNS provides a MultiWriterIPNS layer on top of a GraphSynchronizer
type GSMultiWriterIPNS struct {
	BasicMWIPNS
	IPNSKey cid.Cid
	Gsync   GraphSynchronizationManager
}

type GossipMultiWriterIPNS interface {
	MultiWriterIPNS
	GetRoot() DagNode
	GetNumberOfOperations() int
	GetKey() cid.Cid
}

func NewGossipMultiWriterIPNS(IPNSKey cid.Cid, Gsync GraphSynchronizationManager) GossipMultiWriterIPNS {
	return &GSMultiWriterIPNS{
		BasicMWIPNS: BasicMWIPNS{
			graph: Gsync.GetGraph(IPNSKey),
		},
		IPNSKey: IPNSKey,
		Gsync:   Gsync,
	}
}

func (ipns *GSMultiWriterIPNS) GetKey() cid.Cid {
	return ipns.IPNSKey
}

type BasicMWIPNS struct {
	graph      OperationDAG
	branchEnds []DagNode
}

// AddNewVersion modify the object into a new version based on the previous modifications it depends on
func (ipns *BasicMWIPNS) AddNewVersion(newCid *cid.Cid, prevCids ...*cid.Cid) {
	ipns.graph.AddNewVersion(newCid, prevCids...)
}

// GetRoot returns the root DagNode
func (ipns *BasicMWIPNS) GetRoot() DagNode {
	gp := ipns.graph
	node := gp.GetRoot()
	return node
}

// GetNumberOfOperations returns the number of Dag nodes/operations processed
func (ipns *BasicMWIPNS) GetNumberOfOperations() int {
	gp := ipns.graph
	nodes := gp.GetDagNodes()
	return len(nodes)
}

// GetLatestVersionHistories Each DagNode returned represents one possible version of the data and the history leading up to it
func (ipns *BasicMWIPNS) GetLatestVersionHistories() []DagNode {
	gp := ipns.graph
	node := gp.GetRoot()
	return getLeafNodes(node, nil)
}

func getLeafNodes(root DagNode, visted map[DagNode]struct{}) []DagNode {
	if visted == nil {
		visted = make(map[DagNode]struct{})
		visted[root] = struct{}{}
	}

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
