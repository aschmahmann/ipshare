package sync

import (
	cid "github.com/ipfs/go-cid"
	multihash "github.com/multiformats/go-multihash"
	"log"
	mrand "math/rand"
	"strings"
	testing "testing"
	"time"

	"github.com/aschmahmann/ipshare/testutils"
)

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

	mapToValueList := func(m map[cid.Cid]DagNode) []string {
		lst := make([]string, len(m))
		i := 0
		for _, v := range m {
			lst[i] = v.GetAsOp().Value.String()
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
	log.Printf("%v%v", spaces, n.GetAsOp().Value)
	for _, parent := range n.GetParents() {
		printDag(t, parent, level+1)
	}
}

//TODO: Add unit tests of gsync in addition to the integration tests

var cidBuilder = cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: -1}

func createAddNodeOp(data string, parents ...*AddNodeOperation) *AddNodeOperation {
	dataCid := testutils.CreateCid(data)
	parentCids := make([]*cid.Cid, len(parents))
	for i, p := range parents {
		pBytes, err := p.Marshal()
		if err != nil {
			panic(err)
		}
		pCid, err := cidBuilder.Sum(pBytes)
		if err != nil {
			panic(err)
		}
		parentCids[i] = &pCid
	}
	return &AddNodeOperation{Value: &dataCid, Parents: parentCids}
}

func baseTestOneChangeGraphSync() (graph1, graph2 GraphProvider, root, child *AddNodeOperation, err error) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := testutils.CreateHostAndPeers(reader, 10001, 2, true)

	if err != nil {
		return
	}

	graphKey := testutils.CreateCid("TestGraph")

	root = createAddNodeOp("100")
	child = createAddNodeOp("101", root)

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
	gp1, _, root, child, err := baseTestOneChangeGraphSync()
	if err != nil {
		t.Fatal(err)
	}

	waitForGraphSize(gp1, 2)

	for _, op := range gp1.GetOps() {
		switch *op.Value {
		case *root.Value:
			if !(len(op.Parents) == 0) {
				t.Fatalf("Unexpected operation: instead of Value:100, Parents:[] found Value:%v, Parents:%v", op.Value, op.Parents)
			}
		case *child.Value:
			if !(len(op.Parents) == 1 && *op.Parents[0] == *child.Parents[0]) {
				t.Fatalf("Unexpected operation: instead of Value:101, Parents:[100] found Value:%v, Parents:%v", op.Value, op.Parents)
			}
		default:
			t.Fatalf("Unexpected Operation: Operation value is %v", op.Value)
		}
	}
}

func TestOneSyncAndOneUpdate(t *testing.T) {
	gp1, gp2, _, child, err := baseTestOneChangeGraphSync()
	if err != nil {
		t.Fatal(err)
	}

	updateOp := createAddNodeOp("103", child)
	gp2.Update(updateOp)

	waitForGraphSize(gp1, 3)
}

func twoGraphTestBase() (gs1, gs2, gs3 GraphSynchronizer, graph1, graph2 cid.Cid, root, child *AddNodeOperation, err error) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	hosts, peers, err := testutils.CreateHostAndPeers(reader, 10001, 3, true)

	if err != nil {
		return
	}

	graph1 = testutils.CreateCid("G1")
	graph2 = testutils.CreateCid("G2")

	root = createAddNodeOp("100")
	child = createAddNodeOp("101", root)

	u1 := NewMemoryIPNSLocalStorage()
	u1.AddPeers(graph1, peers[1], peers[2])
	u1.AddOps(graph1, root)
	gs1 = NewGraphSychronizer(hosts[0], u1, mrand.NewSource(1))

	u2 := NewMemoryIPNSLocalStorage()
	u2.AddPeers(graph1, peers[0], peers[2])
	u2.AddOps(graph1, root, child)
	u2.AddPeers(graph2, peers[0], peers[2])
	u2.AddOps(graph2, root)
	gs2 = NewGraphSychronizer(hosts[1], u2, mrand.NewSource(2))

	u3 := NewMemoryIPNSLocalStorage()
	u3.AddPeers(graph2, peers[0], peers[1])
	u3.AddOps(graph2, root, child)
	gs3 = NewGraphSychronizer(hosts[2], u3, mrand.NewSource(3))

	return
}

func TestTwoGraphs(t *testing.T) {
	gs1, gs2, gs3, graph1, graph2, _, child, err := twoGraphTestBase()
	if err != nil {
		t.Fatal(err)
	}

	u1G1 := gs1.GetGraphProvider(graph1)
	u2G1 := gs2.GetGraphProvider(graph1)
	u2G2 := gs2.GetGraphProvider(graph2)
	u3G2 := gs3.GetGraphProvider(graph2)

	newOp := createAddNodeOp("103", child)

	u2G1.Update(newOp)
	u3G2.Update(newOp)

	waitForGraphSize(u1G1, 3)
	log.Println("------------")
	waitForGraphSize(u2G2, 3)
}

func TestTwoGraphsMWIPNS(t *testing.T) {
	gs1, gs2, gs3, graph1, graph2, _, child, err := twoGraphTestBase()
	if err != nil {
		t.Fatal(err)
	}

	u1G1 := gs1.GetGraphProvider(graph1)
	u2G2 := gs2.GetGraphProvider(graph2)

	newOp := createAddNodeOp("103", child)

	ipnsU2G1 := &GossipMultiWriterIPNS{Gsync: gs2, IPNSKey: graph1}
	ipnsU2G1.AddNewVersion(newOp.Value, newOp.Parents...)

	ipnsU3G2 := &GossipMultiWriterIPNS{Gsync: gs3, IPNSKey: graph2}
	ipnsU3G2.AddNewVersion(newOp.Value, newOp.Parents...)

	waitForGraphSize(u1G1, 3)
	waitForGraphSize(u2G2, 3)

	log.Printf("User 2 - Graph 1")
	u2g1versions := ipnsU2G1.GetLatestVersionHistories()
	for _, n := range u2g1versions {
		printDag(t, n, 0)
	}

	log.Printf("User 3 - Graph 2")
	u3g2versions := ipnsU3G2.GetLatestVersionHistories()
	for _, n := range u3g2versions {
		printDag(t, n, 0)
	}
}
