package sync

import (
	"context"
	cbor "github.com/ipfs/go-ipld-cbor"
	host "github.com/libp2p/go-libp2p-host"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-protocol"
	"log"
	"math/rand"
	"sync"

	lutils "github.com/aschmahmann/ipshare/utils"
	"github.com/ipfs/go-key"
)

type MWIPNSPinner struct {
	mux          sync.Mutex
	Synchronizer GraphSynchronizer
	Storage      UpdateableIPNSLocalStorage
}

type RegisterGraph struct {
	GraphID key.Key
	RootCID key.Key
	Peers   []peer.ID
}

func (reg RegisterGraph) Marshal() ([]byte, error) {
	return cbor.DumpObject(reg)
}

func (reg *RegisterGraph) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, reg)
}

func init() {
	//cbor.RegisterCborType(key.Key(""))
	//cbor.RegisterCborType(peer.ID(""))
	cbor.RegisterCborType(RegisterGraph{})
}

const pinnerProtocolID = protocol.ID("/gsync-pinner/0.0.1")

func NewPinner(h host.Host) *MWIPNSPinner {
	updateableStorage := NewMemoryIPNSLocalStorage()
	gs := NewGraphSychronizer(h, updateableStorage, rand.NewSource(rand.Int63()))
	pinner := &MWIPNSPinner{Synchronizer: gs, Storage: updateableStorage}

	h.SetStreamHandler(pinnerProtocolID, func(s net.Stream) {
		defer s.Close()
		ps := lutils.NewProtectedStream(s)
		rpc := &RegisterGraph{}
		err := lutils.ReadFromProtectedStream(ps, rpc)

		if err != nil {
			log.Print(err)
			return
		}

		pinner.mux.Lock()
		pinner.Storage.AddPeers(rpc.GraphID, rpc.Peers...)
		pinner.Storage.AddOps(rpc.GraphID, &SingleOp{Value: string(rpc.RootCID), Parents: make([]string, 0)})
		pinner.Synchronizer.AddGraph(rpc.GraphID)
		pinner.mux.Unlock()
	})

	return pinner
}

type RemotePinner struct {
	ID     peer.ID
	caller host.Host
}

func (pinner *RemotePinner) RegisterGraph(graphID key.Key, peers []peer.ID, rootCID key.Key) error {
	s, err := pinner.caller.NewStream(context.Background(), pinner.ID, pinnerProtocolID)
	if err != nil {
		return err
	}

	ps := lutils.NewProtectedStream(s)
	if err = lutils.WriteToProtectedStream(ps, &RegisterGraph{GraphID: graphID, Peers: peers, RootCID: rootCID}); err != nil {
		return err
	}

	err = net.AwaitEOF(s)
	return err
}
