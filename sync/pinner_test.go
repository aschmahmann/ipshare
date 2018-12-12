package sync

import (
	"github.com/libp2p/go-libp2p-peer"
	mrand "math/rand"
	testing "testing"

	key "github.com/ipfs/go-key"
)

func testPeerIDArrEqual(a, b []peer.ID) bool {
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRegisterGraphRPCMarshal(t *testing.T) {
	rpc := &RegisterGraph{GraphID: "Graph", Peers: []peer.ID{"User 1"}, RootCID: "StartOpCID"}
	bytes, err := rpc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	copyRPC := &RegisterGraph{}
	if err = copyRPC.Unmarshal(bytes); err != nil {
		t.Fatal(err)
	}

	if rpc.GraphID != copyRPC.GraphID {
		t.Fatalf("GraphID %v copied into %v", string(rpc.GraphID), string(copyRPC.GraphID))
	}
	if rpc.RootCID != copyRPC.RootCID {
		t.Fatalf("RootCID %v copied into %v", string(rpc.RootCID), string(copyRPC.RootCID))
	}
	if !testPeerIDArrEqual(rpc.Peers, copyRPC.Peers) {
		t.Fatalf("PeerIDs %v copied into %v", rpc.Peers, copyRPC.Peers)
	}
}

func TestPinner(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := createHostAndPeers(reader, 10001, 2, true)
	if err != nil {
		t.Fatal(err)
	}

	user1sPinner := NewPinner(hosts[1])

	graphKey := key.Key("TestGraph")

	root := &SingleOp{Value: "100", Parents: []string{}}
	child := &SingleOp{Value: "101", Parents: []string{"100"}}

	u1 := NewMemoryIPNSLocalStorage()
	u1.AddPeers(graphKey, peers[1])
	u1.AddOps(graphKey, root, child)
	gs1 := NewGraphSychronizer(hosts[0], u1, mrand.NewSource(1))

	gp1 := gs1.GetGraphProvider(graphKey)

	user1sPinnerManager := &RemotePinner{ID: peers[1], caller: hosts[0]}
	err = user1sPinnerManager.RegisterGraph(graphKey, []peer.ID{peers[0]}, gp1.GetRoot().GetValue())
	if err != nil {
		t.Fatal(err)
	}

	gp1.Update(&SingleOp{Value: "102", Parents: []string{"101"}})

	pinnerGraph := user1sPinner.Synchronizer.GetGraphProvider(graphKey)
	waitForGraphSize(pinnerGraph, 3)
}
