package resolvers

import (
	mrand "math/rand"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"

	testutils "github.com/aschmahmann/ipshare/testutils"
)

func TestMWOverwrite(t *testing.T) {
	reader := mrand.New(mrand.NewSource(mrand.Int63()))
	graphKey := testutils.CreateCid("MWOverwrite")

	gsArr, err := createFullyConnectedGsync(reader, 2, graphKey)
	if err != nil {
		t.Fatal(err)
	}

	val1 := &OverwriteWithConflicts{GraphManager: gsArr[0]}
	val2 := &OverwriteWithConflicts{GraphManager: gsArr[1]}

	val1.UpdateVersion(testutils.CreateCid("String1-1"))
	expectedOption1 := testutils.CreateCid("String1-2")
	val1.UpdateVersion(expectedOption1)

	expectedOption2 := testutils.CreateCid("String2-1")
	val2.UpdateVersion(expectedOption2)

	waitForGraphSize := func(syncedValue *OverwriteWithConflicts, graphSize int) {
		c := make(chan bool)

		go func() {
			for {
				if syncedValue.GraphManager.GetNumberOfOperations() >= graphSize {
					c <- true
					return
				}
				time.Sleep(time.Millisecond * 1000)
			}
		}()

		<-c
	}

	waitForGraphSize(val2, 4)
	waitForGraphSize(val1, 4)

	valVersions := [][]cid.Cid{val1.GetLatestVersions(), val2.GetLatestVersions()}
	for i := 0; i < 2; i++ {
		if len(valVersions[i]) != 2 {
			t.Fatalf("Value # %v does not have two value options", i)
		}
		if !((valVersions[i][0] == expectedOption1 && valVersions[i][1] == expectedOption2) || (valVersions[i][0] == expectedOption2 && valVersions[i][1] == expectedOption1)) {
			t.Fatalf("Value # %v has the wrong two value options", i)
		}
	}

	expectedEndResult := testutils.CreateCid("String12")
	val2.UpdateVersion(expectedEndResult)

	waitForGraphSize(val1, 5)

	if val2.GraphManager.GetNumberOfOperations() != 5 {
		t.Fatal("Nodes not synchronized")
	}
	valVersions = [][]cid.Cid{val1.GetLatestVersions(), val2.GetLatestVersions()}
	for i := 0; i < 2; i++ {
		if len(valVersions[i]) != 1 {
			t.Fatalf("Value # %v does not have one value option", i)
		}
		if !(valVersions[i][0] == expectedEndResult) {
			t.Fatalf("Value # %v has the wrong value", i)
		}
	}
}
