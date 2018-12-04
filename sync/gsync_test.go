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

	mapToValueList := func(m map[key.Key]DagNode) []DagNode {
		lst := make([]DagNode, len(m))
		for _, v := range m {
			lst = append(lst, v)
		}
		return lst
	}

	for _, v := range gp.GetDagNodes() {
		op := v.GetAsOp()
		log.Printf("ID: %v | Children %v | Parents %v", op.Value, mapToValueList(v.GetChildren()), op.Parents)
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

type localStorageType struct {
	peers []peer.ID
	ops   []*SingleOp
}

type MockIPNSLocalStorage struct {
	Storage map[key.Key]localStorageType
}

func (ls *MockIPNSLocalStorage) GetPeers(IPNSKey key.Key) []peer.ID {
	return ls.Storage[IPNSKey].peers
}

func (ls *MockIPNSLocalStorage) GetOps(IPNSKey key.Key) []*SingleOp {
	return ls.Storage[IPNSKey].ops
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

func TestOneChangeGraphSync(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := createHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		t.Fatal(err)
	}

	g1Root := &SingleOp{Value: "100", Parents: []string{}}
	g2Root := &SingleOp{Value: "100", Parents: []string{}}
	g2Child := &SingleOp{Value: "101", Parents: []string{"100"}}

	gs1 := NewGraphSychronizer(hosts[0], []key.Key{"TestGraph"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("TestGraph"): localStorageType{peers: []peer.ID{peers[1]}, ops: []*SingleOp{g1Root}},
		}},
		mrand.NewSource(1))

	_ = NewGraphSychronizer(hosts[1], []key.Key{"TestGraph"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("TestGraph"): localStorageType{peers: []peer.ID{peers[2]}, ops: []*SingleOp{g2Root, g2Child}},
		}},
		mrand.NewSource(2))

	gp1 := gs1.GetGraphProvider("TestGraph")
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
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := createHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		t.Fatal(err)
	}

	g1Root := &SingleOp{Value: "100", Parents: []string{}}
	g2Root := &SingleOp{Value: "100", Parents: []string{}}
	g2Child := &SingleOp{Value: "101", Parents: []string{"100"}}

	gs1 := NewGraphSychronizer(hosts[0], []key.Key{"TestGraph"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("TestGraph"): localStorageType{peers: []peer.ID{peers[1]}, ops: []*SingleOp{g1Root}},
		}},
		mrand.NewSource(1))

	gs2 := NewGraphSychronizer(hosts[1], []key.Key{"TestGraph"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("TestGraph"): localStorageType{peers: []peer.ID{peers[0]}, ops: []*SingleOp{g2Root, g2Child}},
		}},
		mrand.NewSource(2))

	gp1 := gs1.GetGraphProvider("TestGraph")
	gp2 := gs2.GetGraphProvider("TestGraph")

	gp2.Update(&SingleOp{Value: "102", Parents: []string{"101"}})

	waitForGraphSize(gp1, 3)
}

func twoGraphTestBase(t *testing.T) (gs1, gs2, gs3 GraphSynchronizer) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := createHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		t.Fatal(err)
	}

	gRoot := &SingleOp{Value: "100", Parents: []string{}}
	gChild := &SingleOp{Value: "101", Parents: []string{"100"}}

	gs1 = NewGraphSychronizer(hosts[0], []key.Key{"G1"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("G1"): localStorageType{peers: []peer.ID{peers[1], peers[2]}, ops: []*SingleOp{gRoot}},
		}},
		mrand.NewSource(1))

	gs2 = NewGraphSychronizer(hosts[1], []key.Key{"G1", "G2"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("G1"): localStorageType{peers: []peer.ID{peers[0], peers[2]}, ops: []*SingleOp{gRoot, gChild}},
			key.Key("G2"): localStorageType{peers: []peer.ID{peers[0], peers[2]}, ops: []*SingleOp{gRoot}},
		}},
		mrand.NewSource(2))

	gs3 = NewGraphSychronizer(hosts[2], []key.Key{"G2"},
		&MockIPNSLocalStorage{Storage: map[key.Key]localStorageType{
			key.Key("G2"): localStorageType{peers: []peer.ID{peers[0], peers[1]}, ops: []*SingleOp{gRoot, gChild}},
		}},
		mrand.NewSource(3))
	return
}
func TestTwoGraphs(t *testing.T) {
	gs1, gs2, gs3 := twoGraphTestBase(t)

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
	gs1, gs2, gs3 := twoGraphTestBase(t)

	u1G1 := gs1.GetGraphProvider("G1")
	u2G2 := gs2.GetGraphProvider("G2")

	ipnsU2G1 := &GossipMultiWriterIPNS{Gsync: gs2, IPNSKey: ipnsKey{key.Key("G1")}}
	ipnsU2G1.AddNewVersion(key.Key("102"), key.Key("101"))

	ipnsU3G2 := &GossipMultiWriterIPNS{Gsync: gs3, IPNSKey: ipnsKey{key.Key("G2")}}
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
