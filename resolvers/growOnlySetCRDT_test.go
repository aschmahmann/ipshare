package resolvers

import (
	mrand "math/rand"
	"testing"
	"time"

	ipfsApi "github.com/ipfs/go-ipfs-api"

	testutils "github.com/aschmahmann/ipshare/testutils"
)

func TestGWSetCRDT(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	graphKey := testutils.CreateCid("GWSetCRDT")

	gsArr, err := createFullyConnectedGsync(reader, 2, graphKey)
	if err != nil {
		t.Fatal(err)
	}

	ipfsShell := &LockedIPFS{Ipfs: ipfsApi.NewLocalShell()}

	set1 := &NamedMultiWriterStringSetCRDT{GraphManager: gsArr[0], Ipfs: ipfsShell, Set: make(map[string]struct{})}
	set2 := &NamedMultiWriterStringSetCRDT{GraphManager: gsArr[1], Ipfs: ipfsShell, Set: make(map[string]struct{})}

	set1.AddElem("String1")
	set1.AddElem("String2")
	set1.AddElem("String1")
	set2.AddElem("String1")

	waitForGraphSize := func(syncedSet *NamedMultiWriterStringSetCRDT, graphSize int) {
		c := make(chan bool)

		go func() {
			for {
				setCRDT := syncedSet.GetSet()
				if len(setCRDT) >= graphSize {
					c <- true
					return
				}
				time.Sleep(time.Millisecond * 1000)
			}
		}()

		<-c
	}

	waitForGraphSize(set2, 2)
	if len(set1.GetSet()) != 2 || len(set2.GetSet()) != 2 {
		t.Fatal("Not two operations")
	}

	set2.AddElem("String2")
	set2.AddElem("String3")
	waitForGraphSize(set1, 3)
	if len(set1.GetSet()) != 3 || len(set2.GetSet()) != 3 {
		t.Fatal("Not three operations")
	}
}
