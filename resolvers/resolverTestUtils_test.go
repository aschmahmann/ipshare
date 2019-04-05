package resolvers

import (
	"context"
	mrand "math/rand"

	cid "github.com/ipfs/go-cid"

	gsync "github.com/aschmahmann/ipshare/sync"
	testutils "github.com/aschmahmann/ipshare/testutils"

	host "github.com/libp2p/go-libp2p-host"
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

		gs[i] = gsync.NewGossipMultiWriterIPNS(graphKey, gsync.NewGraphSychronizer(h, s, mrand.NewSource(rnd.Int63())))
	}

	return gs, nil
}

func createFullyConnectedPubSubMWIPNS(rnd *mrand.Rand, numHosts int, graphKey cid.Cid) ([]gsync.GossipMultiWriterIPNS, error) {
	hosts, _, err := testutils.CreateHostAndPeers(rnd, 10001, numHosts, false)
	if err != nil {
		return nil, err
	}

	gs := make([]gsync.GossipMultiWriterIPNS, numHosts)
	for i, h := range hosts {
		p, gsm, err := gsync.NewGossipSyncMWIPNS(context.Background(), h)
		if err != nil {
			return nil, err
		}
		g := gsync.NewPubSubMWIPNS(p, gsm, graphKey)
		//s.AddOps(graphKey, &gsync.AddNodeOperation{Parents: []*cid.Cid{}, Value: &graphKey})

		gs[i] = g
	}

	connectAll(hosts)

	return gs, nil
}

func connect(a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		panic(err)
	}
}

func connectAll(hosts []host.Host) {
	for i, a := range hosts {
		for j, b := range hosts {
			if i == j {
				continue
			}

			connect(a, b)
		}
	}
}
