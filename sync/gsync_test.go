package sync

import (
	crypto "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	"log"
	mrand "math/rand"
	"strings"
	testing "testing"
	"time"

	lutils "github.com/aschmahmann/ipshare/utils"
	"github.com/ipfs/go-key"
)

func createHost(rnd *mrand.Rand, portNum int) (host.Host, error) {
	priv, _, err := crypto.GenerateEd25519Key(rnd)
	if err != nil {
		return nil, err
	}

	const localDaemon = true

	ha, err := lutils.MakeDefaultRoutedHost(portNum, priv, localDaemon)
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

	mapToValueList := func(m map[key.Key]DagNode) []string {
		lst := make([]string, len(m))
		i := 0
		for _, v := range m {
			lst[i] = v.GetAsOp().Value
			i++
		}
		return lst
	}

	for _, v := range gp.GetDagNodes() {
		op := v.GetAsOp()
		children := mapToValueList(v.GetChildren())
		log.Printf("ID: %v | Children %v | Parents %v", op.Value, children, op.Parents)
	}
}

func printDag(t *testing.T, n DagNode, level int) {
	spaces := strings.Repeat(" ", level*2)
	if level != 0 {
		spaces = spaces + "->"
	}
	log.Printf("%v%v", spaces, n.GetAsOp().GetValue())
	for _, parent := range n.GetParents() {
		printDag(t, parent, level+1)
	}
}

//TODO: Add unit tests of gsync in addition to the integration tests

func createHostAndPeers(rnd *mrand.Rand, startPort, numHosts int, printPeers bool) ([]host.Host, []peer.ID, error) {
	var hosts []host.Host
	var peers []peer.ID

	for i := 0; i < numHosts; i++ {
		h, err := createHost(rnd, startPort+i)
		if err != nil {
			return nil, nil, err
		}
		hosts = append(hosts, h)
		peers = append(peers, h.ID())
	}

	if printPeers {
		for i, p := range peers {
			log.Printf("peer%v: %v", i, p)
		}
	}

	return hosts, peers, nil
}

func baseTestOneChangeGraphSync() (graph1, graph2 GraphProvider, err error) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := createHostAndPeers(reader, 10001, 2, true)

	if err != nil {
		return
	}

	graphKey := key.Key("TestGraph")

	root := &SingleOp{Value: "100", Parents: []string{}}
	child := &SingleOp{Value: "101", Parents: []string{"100"}}

	u1 := NewMemoryIPNSLocalStorage()
	u1.AddPeers(graphKey, peers[1])
	u1.AddOps(graphKey, root)
	gs1 := NewGraphSychronizer(hosts[0], u1, mrand.NewSource(1))

	u2 := NewMemoryIPNSLocalStorage()
	u2.AddPeers(graphKey, peers[0])
	u2.AddOps(graphKey, root, child)
	gs2 := NewGraphSychronizer(hosts[1], u2, mrand.NewSource(2))

	graph1 = gs1.GetGraphProvider(graphKey)
	graph2 = gs2.GetGraphProvider(graphKey)
	return
}

func TestOneChangeGraphSync(t *testing.T) {
	gp1, _, err := baseTestOneChangeGraphSync()
	if err != nil {
		t.Fatal(err)
	}

	waitForGraphSize(gp1, 2)

	for _, op := range gp1.GetOps() {
		switch op.Value {
		case "100":
			if !(len(op.Parents) == 0) {
				t.Fatalf("Unexpected operation: instead of Value:100, Parents:[] found Value:%v, Parents:%v", op.Value, op.Parents)
			}
		case "101":
			if !(len(op.Parents) == 1 && op.Parents[0] == "100") {
				t.Fatalf("Unexpected operation: instead of Value:101, Parents:[100] found Value:%v, Parents:%v", op.Value, op.Parents)
			}
		default:
			t.Fatalf("Unexpected Operation: Operation value is %v", op.Value)
		}
	}
}

func TestOneSyncAndOneUpdate(t *testing.T) {
	gp1, gp2, err := baseTestOneChangeGraphSync()
	if err != nil {
		t.Fatal(err)
	}

	gp2.Update(&SingleOp{Value: "102", Parents: []string{"101"}})

	waitForGraphSize(gp1, 3)
}

func twoGraphTestBase() (gs1, gs2, gs3 GraphSynchronizer, err error) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := createHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		return
	}

	root := &SingleOp{Value: "100", Parents: []string{}}
	child := &SingleOp{Value: "101", Parents: []string{"100"}}

	u1 := NewMemoryIPNSLocalStorage()
	u1.AddPeers("G1", peers[1], peers[2])
	u1.AddOps("G1", root)
	gs1 = NewGraphSychronizer(hosts[0], u1, mrand.NewSource(1))

	u2 := NewMemoryIPNSLocalStorage()
	u2.AddPeers("G1", peers[0], peers[2])
	u2.AddOps("G1", root, child)
	u2.AddPeers("G2", peers[0], peers[2])
	u2.AddOps("G2", root)
	gs2 = NewGraphSychronizer(hosts[1], u2, mrand.NewSource(2))

	u3 := NewMemoryIPNSLocalStorage()
	u3.AddPeers("G2", peers[0], peers[1])
	u3.AddOps("G2", root, child)
	gs3 = NewGraphSychronizer(hosts[2], u3, mrand.NewSource(3))

	return
}

func TestTwoGraphs(t *testing.T) {
	gs1, gs2, gs3, err := twoGraphTestBase()
	if err != nil {
		t.Fatal(err)
	}

	u1G1 := gs1.GetGraphProvider("G1")
	u2G1 := gs2.GetGraphProvider("G1")
	u2G2 := gs2.GetGraphProvider("G2")
	u3G2 := gs3.GetGraphProvider("G2")

	gNewOp := &SingleOp{Value: "102", Parents: []string{"101"}}
	u2G1.Update(gNewOp)
	u3G2.Update(gNewOp)

	waitForGraphSize(u1G1, 3)
	log.Println("------------")
	waitForGraphSize(u2G2, 3)
}
func TestTwoGraphsMWIPNS(t *testing.T) {
	gs1, gs2, gs3, err := twoGraphTestBase()
	if err != nil {
		t.Fatal(err)
	}

	u1G1 := gs1.GetGraphProvider("G1")
	u2G2 := gs2.GetGraphProvider("G2")

	ipnsU2G1 := &GossipMultiWriterIPNS{Gsync: gs2, IPNSKey: ipnsKey(key.Key("G1"))}
	ipnsU2G1.AddNewVersion(key.Key("102"), key.Key("101"))

	ipnsU3G2 := &GossipMultiWriterIPNS{Gsync: gs3, IPNSKey: ipnsKey(key.Key("G2"))}
	ipnsU3G2.AddNewVersion(key.Key("102"), key.Key("101"))

	waitForGraphSize(u1G1, 3)
	waitForGraphSize(u2G2, 3)

	log.Printf("User 2 - Graph 1")
	for _, n := range ipnsU2G1.GetLatestVersionHistories() {
		printDag(t, n, 0)
	}

	log.Printf("User 3 - Graph 2")
	for _, n := range ipnsU3G2.GetLatestVersionHistories() {
		printDag(t, n, 0)
	}
}
