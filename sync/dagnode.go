package sync

import cid "github.com/ipfs/go-cid"

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
