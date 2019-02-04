package sync

import (
	cid "github.com/ipfs/go-cid"
	multihash "github.com/multiformats/go-multihash"
	testing "testing"
)

func addNodeOperationsEqual(a, b []*AddNodeOperation) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if *(v.Value) != *(b[i].Value) || !cidsEqual(v.Parents, b[i].Parents) {
			return false
		}
	}
	return true
}

func cidsEqual(a, b []*cid.Cid) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if *v != *b[i] {
			return false
		}
	}
	return true
}

func TestAlgoMarshalRoundtrip(t *testing.T) {
	cidBuilder := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: -1}

	graphCid, err := cidBuilder.Sum([]byte("Graph ID"))
	if err != nil {
		t.Fatal(err)
	}
	op1Cid, err := cidBuilder.Sum([]byte("Op1"))
	if err != nil {
		t.Fatal(err)
	}
	op2Cid, err := cidBuilder.Sum([]byte("Op2"))
	if err != nil {
		t.Fatal(err)
	}

	rpc := &FullSendGSync{GraphID: &graphCid, Operations: []*AddNodeOperation{&AddNodeOperation{Value: &op1Cid, Parents: []*cid.Cid{}}, &AddNodeOperation{Value: &op2Cid, Parents: []*cid.Cid{&op1Cid}}}}
	rpcBytes, err := rpc.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	wrappedRPC := &GSyncMessage{MessageType: FULL_GRAPH, Msg: rpcBytes}

	bytes, err := rpc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	copyRPC := &FullSendGSync{}
	if err = copyRPC.Unmarshal(bytes); err != nil {
		t.Fatal(err)
	}

	if !addNodeOperationsEqual(rpc.Operations, copyRPC.Operations) {
		t.Fatalf("Operations %v copied into %v", rpc.Operations, copyRPC.Operations)
	}

	bytes, err = wrappedRPC.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	copyWrappedRPC := &GSyncMessage{}
	if err = copyWrappedRPC.Unmarshal(bytes); err != nil {
		t.Fatal(err)
	}

	if wrappedRPC.MessageType != copyWrappedRPC.MessageType {
		t.Fatalf("Message type %v copied into %v", wrappedRPC.MessageType, copyWrappedRPC.MessageType)
	}

	typedMsgCopy := &FullSendGSync{}
	if err = typedMsgCopy.Unmarshal(copyWrappedRPC.Msg); err != nil {
		t.Fatalf("Message type not preserved in the copy: %v", err)
	}
	if !addNodeOperationsEqual(rpc.Operations, typedMsgCopy.Operations) {
		t.Fatalf("Operations %v copied into %v", rpc.Operations, typedMsgCopy.Operations)
	}
}
