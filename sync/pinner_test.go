package sync

import (
	mrand "math/rand"
	testing "testing"

	peer "github.com/libp2p/go-libp2p-peer"

	"github.com/aschmahmann/ipshare/testutils"
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
	rpc := &RegisterGraph{GraphID: "Graph", Peers: []peer.ID{"User 1"}, RootCID: testutils.CreateCid("StartOpCid")}
	bytes, err := rpc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	copyRPC := &RegisterGraph{}
	if err = copyRPC.Unmarshal(bytes); err != nil {
		t.Fatal(err)
	}

	if rpc.GraphID != copyRPC.GraphID {
		t.Fatalf("GraphID %v copied into %v", rpc.GraphID, copyRPC.GraphID)
	}
	if rpc.RootCID != copyRPC.RootCID {
		t.Fatalf("RootCID %v copied into %v", rpc.RootCID, copyRPC.RootCID)
	}
	if !testPeerIDArrEqual(rpc.Peers, copyRPC.Peers) {
		t.Fatalf("PeerIDs %v copied into %v", rpc.Peers, copyRPC.Peers)
	}
}

func TestPinner(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := testutils.CreateHostAndPeers(reader, 10001, 2, true)
	if err != nil {
		t.Fatal(err)
	}

	user1sPinner := NewPinner(hosts[1])

	graphID := "TestGraph"
	graphRoot := createAddNodeOp(graphID)
	graphRootID := *graphRoot.Value
	root := createAddNodeOp("100", graphRoot)
	child := createAddNodeOp("101", root)

	u1 := NewMemoryIPNSLocalStorage()
	u1.AddPeers(graphID, peers[1])
	u1.AddOps(graphID, graphRoot, root, child)
	gs1 := NewGraphSychronizer(hosts[0], u1, mrand.NewSource(1))

	gp1 := gs1.GetGraphProvider(graphID)

	user1sPinnerManager := &RemotePinner{ID: peers[1], caller: hosts[0]}
	err = user1sPinnerManager.RegisterGraph(graphID, graphRootID, []peer.ID{peers[0]})
	if err != nil {
		t.Fatal(err)
	}

	grandChild := createAddNodeOp("102", child)
	gp1.Update(grandChild)

	pinnerGraph := user1sPinner.Synchronizer.GetGraph(graphID)
	waitForGraphSize(pinnerGraph, 3)
}
