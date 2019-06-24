package resolvers

import (
	"context"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	mrand "math/rand"

	gsync "github.com/aschmahmann/ipshare/sync"
	"github.com/aschmahmann/ipshare/testutils"

	host "github.com/libp2p/go-libp2p-host"
)

func createFullyConnectedGsync(rnd *mrand.Rand, numHosts int, graphID string) ([]gsync.GossipMultiWriterIPNS, error) {
	hosts, peers, err := testutils.CreateHostAndPeers(rnd, 10001, numHosts, false)
	if err != nil {
		return nil, err
	}

	gs := make([]gsync.GossipMultiWriterIPNS, numHosts)
	for i, h := range hosts {
		s := gsync.NewMemoryIPNSLocalStorage()
		s.AddPeers(graphID, peers...)
		op, err := gsync.CreateRootNode(graphID)
		if err != nil {
			return nil, err
		}
		s.AddOps(graphID, op)

		gs[i] = gsync.NewGossipMultiWriterIPNS(graphID, gsync.NewGraphSychronizer(h, s, mrand.NewSource(rnd.Int63())))
	}

	return gs, nil
}

func createFullyConnectedPubSubMWIPNS(rnd *mrand.Rand, numHosts int, graphID string) ([]gsync.GossipMultiWriterIPNS, error) {
	hosts, _, err := testutils.CreateHostAndPeers(rnd, 10001, numHosts, false)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	gs := make([]gsync.GossipMultiWriterIPNS, numHosts)
	for i, h := range hosts {
		ps, err := pubsub.NewGossipSub(ctx, h)
		if err != nil {
			return nil, err
		}
		mgr := gsync.NewMultiWriterPubSub(ctx, h, ps, gsync.NewManualGraphSychronizer(h))
		g, err := mgr.GetValue(ctx, graphID)
		if err != nil {
			return nil, err
		}
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
