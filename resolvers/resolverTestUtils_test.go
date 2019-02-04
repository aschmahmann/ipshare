package resolvers

import (
	cid "github.com/ipfs/go-cid"
	mrand "math/rand"

	gsync "github.com/aschmahmann/ipshare/sync"
	testutils "github.com/aschmahmann/ipshare/testutils"
)

func createFullyConnectedGsync(rnd *mrand.Rand, numHosts int, graphKey cid.Cid) ([]gsync.GossipMultiWriterIPNS, error) {
	hosts, peers, err := testutils.CreateHostAndPeers(rnd, 10001, numHosts, false)
	if err != nil {
		return nil, err
	}

	gs := make([]gsync.GossipMultiWriterIPNS, numHosts)
	for i, h := range hosts {
		s := gsync.NewMemoryIPNSLocalStorage()
		s.AddPeers(graphKey, peers...)
		s.AddOps(graphKey, &gsync.AddNodeOperation{Parents: []*cid.Cid{}, Value: &graphKey})

		gs[i] = gsync.GossipMultiWriterIPNS{
			Gsync:   gsync.NewGraphSychronizer(h, s, mrand.NewSource(rnd.Int63())),
			IPNSKey: graphKey}
	}

	return gs, nil
}
