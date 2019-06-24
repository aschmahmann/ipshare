package sync

import (
	"context"
	"os"
	"time"

	"runtime"
	"runtime/pprof"

	ipns "github.com/ipfs/go-ipns"
	host "github.com/libp2p/go-libp2p-host"

	"math/rand"
	mrand "math/rand"

	"github.com/aschmahmann/ipshare/testutils"

	proto "github.com/gogo/protobuf/proto"

	testing "testing"

	crypto "github.com/libp2p/go-libp2p-crypto"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"fmt"
	ds "github.com/ipfs/go-datastore"
	core "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/namesys"
	"github.com/ipfs/go-path"
	peer "github.com/libp2p/go-libp2p-peer"
	pstoremem "github.com/libp2p/go-libp2p-peerstore/pstoremem"
	psrouter "github.com/libp2p/go-libp2p-pubsub-router"
	record "github.com/libp2p/go-libp2p-record"
)

func connect(t testing.TB, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

func sparseConnect(t testing.TB, hosts []host.Host) {
	connectSome(t, hosts, 3)
}

func denseConnect(t testing.TB, hosts []host.Host) {
	connectSome(t, hosts, 10)
}

func connectSome(t testing.TB, hosts []host.Host, d int) {
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

func connectAll(t testing.TB, hosts []host.Host) {
	for i, a := range hosts {
		for j, b := range hosts {
			if i == j {
				continue
			}

			connect(t, a, b)
		}
	}
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

func NewGossipSyncMWIPNS(ctx context.Context, host host.Host) (*MWmgr, error) {
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		return nil, err
	}

	syncMgr := NewManualGraphSychronizer(host)

	return NewMultiWriterPubSub(ctx, host, ps, syncMgr), nil
}

func TestTwoGraphsMWPSIPNS(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, _, err := testutils.CreateHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		t.Fatal(err)
	}

	graph1 := "G1"
	graph2 := "G2"

	root1 := createAddNodeOp("G1")
	child1 := createAddNodeOp("101", root1)

	root2 := createAddNodeOp("G2")
	child2 := createAddNodeOp("101", root2)

	ctx := context.Background()

	gs1, err := NewGossipSyncMWIPNS(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}
	u1G1, err := gs1.GetValue(ctx, graph1)
	if err != nil {
		t.Fatal(err)
	}

	gs2, err := NewGossipSyncMWIPNS(context.Background(), hosts[1])
	if err != nil {
		t.Fatal(err)
	}

	u2G1, err := gs2.GetValue(ctx, graph1)
	if err != nil {
		t.Fatal(err)
	}

	u2G2, err := gs2.GetValue(ctx, graph2)
	if err != nil {
		t.Fatal(err)
	}

	gs3, err := NewGossipSyncMWIPNS(context.Background(), hosts[2])
	if err != nil {
		t.Fatal(err)
	}

	u3G2, err := gs3.GetValue(ctx, graph2)
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

	log.Infof("User 2 - Graph 1")
	u2g1versions := u2G1.GetLatestVersionHistories()
	for _, n := range u2g1versions {
		printDag(t, n, 0)
	}

	log.Infof("User 3 - Graph 2")
	u3g2versions := u3G2.GetLatestVersionHistories()
	for _, n := range u3g2versions {
		printDag(t, n, 0)
	}
}

func Benchmark(b *testing.B) {
	runtime.GC()
	f, err := os.Create("t.out")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	for i := 0; i < b.N; i++ {
		hosts, _, err := testutils.CreateHostAndPeers(mrand.New(mrand.NewSource(0)), 0, 20, false)
		//hosts, _, err :=testutils.CreateHostAndPeersTest(nil,mrand.New(mrand.NewSource(0)), 20,false)
		if err != nil {
			b.Fatal(err)
		}
		RunIPNSPS(b, hosts)
	}
}

func TestIPNSPS(t *testing.T) {
	hosts, _, err := testutils.CreateHostAndPeersTest(t, mrand.New(mrand.NewSource(0)), 2, false)
	//hosts, _, err := testutils.CreateHostAndPeers(mrand.New(mrand.NewSource(0)), 0, 20, false)
	if err != nil {
		t.Fatal(err)
	}

	RunIPNSPS(t, hosts)
}

func RunIPNSPS(t testing.TB, hosts []host.Host) {
	routers := make([]*psrouter.PubsubValueStore, len(hosts))
	for i, h := range hosts {
		routers[i] = getPSRouter(h)
	}

	ts := time.Now().Add(time.Minute * 5)
	priv, pub, err := crypto.GenerateEd25519Key(mrand.New(mrand.NewSource(mrand.Int63())))
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

	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}

	if err = routers[0].Subscribe(ipns.RecordKey(id)); err != nil {
		t.Fatal(err)
	}

	if err = routers[0].PutValue(context.Background(), ipns.RecordKey(id), msgBytes); err != nil {
		t.Fatal(err)
	}

	//time.Sleep(time.Millisecond * 6000)

	for i := 1; i < len(hosts); i++ {
		if err = routers[i].Subscribe(ipns.RecordKey(id)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < len(hosts); i++ {
		connect(t, hosts[0], hosts[i])
	}

	time.Sleep(time.Millisecond * 100) //was 200

	successes := 0
	for i := 1; i < len(hosts); i++ {
		_, err := routers[i].GetValue(context.Background(), ipns.RecordKey(id))
		if err == nil {
			successes++
		}
	}
	if successes == 0 {
		t.Fatal("no hosts received update")
	}
}

func TestNamesysPS(t *testing.T) {
	hosts, _, err := testutils.CreateHostAndPeersTest(t, mrand.New(mrand.NewSource(0)), 2, false)
	if err != nil {
		t.Fatal(err)
	}

	routers := make([]*psrouter.PubsubValueStore, len(hosts))
	for i, h := range hosts {
		routers[i] = getPSRouter(h)
	}

	names := make([]namesys.NameSystem, len(hosts))
	for i := 0; i < len(hosts); i++ {
		names[i] = namesys.NewNameSystem(routers[i], ds.NewMapDatastore(), 100)
	}

	priv, _, err := crypto.GenerateEd25519Key(mrand.New(mrand.NewSource(mrand.Int63())))
	if err != nil {
		t.Fatal(err)
	}

	origPath := path.FromString("/ipfs/QmY4mFqm81hECYm6Z2NHzHtJwGaQB53sAQfhRmRPuN6keV")
	err = names[0].Publish(context.Background(), priv, origPath)
	if err != nil {
		t.Fatal(err)
	}

	ipnsID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}

	connect(t, hosts[0], hosts[1])

	path, err := names[1].Resolve(context.Background(), fmt.Sprintf("/ipns/%v", ipnsID.Pretty()))
	if err != nil {
		t.Fatal(err)
	}
	if path != origPath {
		t.Fatalf("orig:%v | now:%v", origPath, path)
	}
}

func getPSRouter(host host.Host) *psrouter.PubsubValueStore {
	ctx := context.Background()
	v := record.NamespacedValidator{
		"pk":   record.PublicKeyValidator{},
		"ipns": ipns.Validator{KeyBook: pstoremem.NewPeerstore()},
	}

	router, err := core.DHTOption(ctx, host, ds.NewMapDatastore(), v)
	if err != nil {
		panic(err)
	}
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		panic(err)
	}

	PSRouter := psrouter.NewPubsubValueStore(
		ctx,
		host,
		router,
		ps,
		v,
	)
	return PSRouter
}
