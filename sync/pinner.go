package sync

import (
	"context"
	"log"
	"math/rand"
	"sync"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	host "github.com/libp2p/go-libp2p-host"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"

	lutils "github.com/aschmahmann/ipshare/utils"
)

// MWIPNSPinner handles node that can be registered with to persist MWIPNS structures
type MWIPNSPinner struct {
	mux          sync.Mutex
	Synchronizer GraphSynchronizationManager
	Storage      UpdateableIPNSLocalStorage
}

// RegisterGraph is the RPC structure for registering an append-only DAG with an MWIPNSPinner
type RegisterGraph struct {
	GraphID cid.Cid
	RootCID cid.Cid
	Peers   []peer.ID
}

// Marshal returns the byte representation of the object
func (reg RegisterGraph) Marshal() ([]byte, error) {
	return cbor.DumpObject(reg)
}

// Unmarshal fills the structure with data from the bytes
func (reg *RegisterGraph) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, reg)
}

func init() {
	cbor.RegisterCborType(RegisterGraph{})
}

const pinnerProtocolID = protocol.ID("/gsync-pinner/0.0.1")

// NewPinner instantiates an MWIPNSPinner
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
		pinner.Storage.AddOps(rpc.GraphID, &AddNodeOperation{Value: &rpc.RootCID, Parents: make([]*cid.Cid, 0)})
		pinner.Synchronizer.AddGraph(rpc.GraphID)
		pinner.mux.Unlock()
	})

	return pinner
}

// RemotePinner handles remote calls to a MWIPNSPinner node
type RemotePinner struct {
	ID     peer.ID
	caller host.Host
}

// RegisterGraph registers a graph with the MWIPNSPinner
func (pinner *RemotePinner) RegisterGraph(graphID cid.Cid, peers []peer.ID) error {
	s, err := pinner.caller.NewStream(context.Background(), pinner.ID, pinnerProtocolID)
	if err != nil {
		return err
	}

	ps := lutils.NewProtectedStream(s)
	if err = lutils.WriteToProtectedStream(ps, &RegisterGraph{GraphID: graphID, Peers: peers, RootCID: graphID}); err != nil {
		return err
	}

	err = net.AwaitEOF(s)
	return err
}
