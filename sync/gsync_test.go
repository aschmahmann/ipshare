package sync

import (
	"log"
	mrand "math/rand"
	"strconv"
	testing "testing"
	"time"

	crypto "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

func createHost(rnd *mrand.Rand, portNum int) (host.Host, error) {
	priv, _, err := crypto.GenerateEd25519Key(rnd)
	if err != nil {
		return nil, err
	}

	const localDaemon = true
	var peers []pstore.PeerInfo
	if localDaemon {
		peers = getLocalPeerInfo()
	} else {
		peers = IPFS_PEERS
	}

	ha, err := makeRoutedHost(portNum, priv, peers)
	if err != nil {
		return nil, err
	}

	return ha, nil
}

func waitForGraphSize(gp GraphProvider, graphSize int) {
	c := make(chan bool)

	go func() {
		for {
			if len(gp.GetOps()) >= graphSize {
				c <- true
			}
			time.Sleep(time.Millisecond * 1000)
		}
	}()

	<-c

	for k, v := range gp.GetOps() {
		log.Printf("ID: %v | Children %v | Parents %v", strconv.Itoa(k), v.Children, v.Parents)
	}
}

//TODO: Add unit tests of gsync in addition to the integration tests

func TestOneChangeGraphSync(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))

	ha1, err := createHost(reader, 10001)
	if err != nil {
		t.Fatal(err)
	}

	ha2, err := createHost(reader, 10002)
	if err != nil {
		t.Fatal(err)
	}

	peer1 := ha1.ID()
	peer2 := ha2.ID()

	log.Printf("peer1: %v", peer1)
	log.Printf("peer2: %v", peer2)

	g1Root := &SingleOp{Value: "100", Parents: []string{}, Children: []string{}}
	g2Root := &SingleOp{Value: "100", Parents: []string{}, Children: []string{"101"}}
	g2Child := &SingleOp{Value: "101", Parents: []string{"100"}, Children: []string{}}

	gp1 := NewGraphProvider([]*SingleOp{g1Root}, ha1, []peer.ID{peer2}, mrand.NewSource(1))
	_ = NewGraphProvider([]*SingleOp{g2Root, g2Child}, ha2, []peer.ID{peer1}, mrand.NewSource(2))

	waitForGraphSize(gp1, 2)

	for _, op := range gp1.GetOps() {
		switch op.Value {
		case "100":
			if !(len(op.Children) == 1 && op.Children[0] == "101" && len(op.Parents) == 0) {
				t.Fatalf("Unexpected operation: instead of Value:100, Parents:[], Children: [101] found Value:%v, Parents:%v, Children: %v", op.Value, op.Parents, op.Children)
			}
		case "101":
			if !(len(op.Parents) == 1 && op.Parents[0] == "100" && len(op.Children) == 0) {
				t.Fatalf("Unexpected operation: instead of Value:101, Parents:[100], Children: [] found Value:%v, Parents:%v, Children: %v", op.Value, op.Parents, op.Children)
			}
		default:
			t.Fatalf("Unexpected Operation: Operation value is %v", op.Value)
		}
	}
}

func TestOneSyncAndOneUpdate(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))

	ha1, err := createHost(reader, 10001)
	if err != nil {
		t.Fatal(err)
	}

	ha2, err := createHost(reader, 10002)
	if err != nil {
		t.Fatal(err)
	}

	peer1 := ha1.ID()
	peer2 := ha2.ID()

	log.Printf("peer1: %v", peer1)
	log.Printf("peer2: %v", peer2)

	g1Root := &SingleOp{Value: "100", Parents: []string{}, Children: []string{}}
	g2Root := &SingleOp{Value: "100", Parents: []string{}, Children: []string{"101"}}
	g2Child := &SingleOp{Value: "101", Parents: []string{"100"}, Children: []string{}}

	gp1 := NewGraphProvider([]*SingleOp{g1Root}, ha1, []peer.ID{peer2}, mrand.NewSource(1))
	gp2 := NewGraphProvider([]*SingleOp{g2Root, g2Child}, ha2, []peer.ID{peer1}, mrand.NewSource(2))

	gp2.Update(&SingleOp{Value: "102", Parents: []string{"101"}, Children: []string{}})

	waitForGraphSize(gp1, 3)
}
