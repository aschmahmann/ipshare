package sync

import (
	"bytes"
	"context"
	"time"

	ipns "github.com/ipfs/go-ipns"
	ipnspb "github.com/ipfs/go-ipns/pb"
	host "github.com/libp2p/go-libp2p-host"

	"math/rand"
	mrand "math/rand"

	"github.com/aschmahmann/ipshare/testutils"

	proto "github.com/gogo/protobuf/proto"

	testing "testing"

	crypto "github.com/libp2p/go-libp2p-crypto"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"log"
)

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func sparseConnect(t *testing.T, hosts []host.Host) {
	connectSome(t, hosts, 3)
}

func denseConnect(t *testing.T, hosts []host.Host) {
	connectSome(t, hosts, 10)
}

func connectSome(t *testing.T, hosts []host.Host, d int) {
	for i, a := range hosts {
		for j := 0; j < d; j++ {
			n := rand.Intn(len(hosts))
			if n == i {
				j--
				continue
			}

			b := hosts[n]

			connect(t, a, b)
		}
	}
}

func connectAll(t *testing.T, hosts []host.Host) {
	for i, a := range hosts {
		for j, b := range hosts {
			if i == j {
				continue
			}

			connect(t, a, b)
		}
	}
}

func NewIPNSGossipSub(ctx context.Context, h host.Host, opts ...pubsub.Option) (*pubsub.PubSub, error) {
	createIpnsCache := func() *pubsub.LWWMessageCache {
		return pubsub.NewLWWMessageCache(
			func(msg1, msg2 *pb.Message) bool {
				v := ipns.Validator{}
				msgBytes := [][]byte{msg1.Data, msg2.Data}
				winningIndex, err := v.Select("", msgBytes)
				if err != nil {
					panic(err)
				}
				return winningIndex == 0
			},
			func(msg *pb.Message) string {
				// Parse the value into an IpnsEntry
				entry := new(ipnspb.IpnsEntry)
				err := proto.Unmarshal(msg.Data, entry)
				if err != nil {
					panic(ipns.ErrBadRecord)
				}
				return string(entry.Value)
			},
		)
	}

	return pubsub.NewGossipSyncLWW(context.Background(), h, createIpnsCache(), "ipnsps/0.0.1")
}

func waitForGossipSize(g GossipMultiWriterIPNS, graphSize int) {
	c := make(chan bool)

	go func() {
		for {
			if g.GetNumberOfOperations() >= graphSize {
				c <- true
			}
			time.Sleep(time.Millisecond * 1000)
		}
	}()

	<-c
}

func TestTwoGraphsMWPSIPNS(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, _, err := testutils.CreateHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		t.Fatal(err)
	}

	graph1 := testutils.CreateCid("G1")
	graph2 := testutils.CreateCid("G2")

	root1 := createAddNodeOp("G1")
	child1 := createAddNodeOp("101", root1)

	root2 := createAddNodeOp("G2")
	child2 := createAddNodeOp("101", root2)

	ps1, gs1, err := NewGossipSyncMWIPNS(context.Background(), hosts[0])
	u1G1 := NewPubSubMWIPNS(ps1, gs1, graph1)
	if err != nil {
		t.Fatal(err)
	}

	ps2, gs2, err := NewGossipSyncMWIPNS(context.Background(), hosts[1])
	u2G1 := NewPubSubMWIPNS(ps2, gs2, graph1)
	u2G2 := NewPubSubMWIPNS(ps2, gs2, graph2)
	if err != nil {
		t.Fatal(err)
	}

	ps3, gs3, err := NewGossipSyncMWIPNS(context.Background(), hosts[2])
	u3G2 := NewPubSubMWIPNS(ps3, gs3, graph2)
	if err != nil {
		t.Fatal(err)
	}

	u1G1.AddNewVersion(root1.Value, root1.Parents...)
	u1G1.AddNewVersion(child1.Value, child1.Parents...)
	u2G2.AddNewVersion(root2.Value, root2.Parents...)
	u2G2.AddNewVersion(child2.Value, child2.Parents...)

	connectAll(t, hosts)

	time.Sleep(time.Millisecond * 2000)

	newOp1 := createAddNodeOp("103", child1)
	newOp2 := createAddNodeOp("103", child2)

	u2G1.AddNewVersion(newOp1.Value, newOp1.Parents...)
	u3G2.AddNewVersion(newOp2.Value, newOp2.Parents...)

	waitForGossipSize(u1G1, 3)
	waitForGossipSize(u2G2, 3)

	log.Printf("User 2 - Graph 1")
	u2g1versions := u2G1.GetLatestVersionHistories()
	for _, n := range u2g1versions {
		printDag(t, n, 0)
	}

	log.Printf("User 3 - Graph 2")
	u3g2versions := u3G2.GetLatestVersionHistories()
	for _, n := range u3g2versions {
		printDag(t, n, 0)
	}
}

func TestIPNSPS(t *testing.T) {
	hosts, _, err := testutils.CreateHostAndPeers(mrand.New(mrand.NewSource(0)), 0, 20, true)
	if err != nil {
		t.Fatal(err)
	}

	gossips := make([]*pubsub.PubSub, len(hosts))
	subs := make([]*pubsub.Subscription, len(hosts))
	topic := "InsertIPNSKeyHere"

	for i := 0; i < len(hosts); i++ {
		gossips[i], err = NewIPNSGossipSub(context.Background(), hosts[i])
		if err != nil {
			t.Fatal(err)
		}

		subs[i], err = gossips[i].Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
	}

	sparseConnect(t, hosts[1:])

	time.Sleep(time.Second * 2)

	ts := time.Now().Add(time.Minute * 5)
	priv, _, err := crypto.GenerateEd25519Key(mrand.New(mrand.NewSource(mrand.Int63())))
	if err != nil {
		t.Fatal(err)
	}

	ipnsEntry, err := ipns.Create(priv, []byte("/ipfs/ABCDEFGHIJKLMNOP"), 1, ts)
	if err != nil {
		t.Fatal(err)
	}

	msgBytes, err := proto.Marshal(ipnsEntry)
	if err != nil {
		t.Fatal(err)
	}

	if err = gossips[1].Publish(topic, msgBytes); err != nil {
		t.Fatal(err)
	}

	time.Sleep(6 * time.Second)

	for i := 1; i < len(hosts); i++ {
		verifyEntry(t, subs[i], ipnsEntry)
	}

	connect(t, hosts[0], hosts[1])

	verifyEntry(t, subs[0], ipnsEntry)
}

func verifyEntry(t *testing.T, sub *pubsub.Subscription, ipnsEntry *ipnspb.IpnsEntry) {
	receivedMsg, err := sub.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receivedEntry := &ipnspb.IpnsEntry{}
	if err = proto.Unmarshal(receivedMsg.Data, receivedEntry); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(receivedEntry.Value, ipnsEntry.Value) {
		t.Fatalf("Received message: %v not equal to sent message %v", string(receivedEntry.Value), string(ipnsEntry.Value))
	}
}
