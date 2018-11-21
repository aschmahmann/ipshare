package sync

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"sync"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-key"
	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	rhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

// A structure containing separate read and write mutexes
type rwMutex struct {
	r, w *sync.Mutex
}

// A stream protected by a rwMutex
type protectedStream struct {
	net.Stream
	rwMutex
}

type setSyncGraphProvider struct {
	mux   sync.Mutex
	OpSet map[key.Key]*SingleOp

	updateStreams map[net.Stream]*rwMutex
}

// GraphProvider Manages a graph of operations, including broadcasting and receiving updates
type GraphProvider interface {
	ReceiveUpdates(...*SingleOp)
	Update(*SingleOp)
	GetOps() []*SingleOp
}

type message interface {
	proto.Marshaler
	proto.Unmarshaler
	proto.Sizer
}

// GetID Gets the ID of the Operation
func (op *SingleOp) GetID() key.Key {
	return key.Key(op.Value)
}

func (gp *setSyncGraphProvider) ReceiveUpdates(ops ...*SingleOp) {
	gp.mux.Lock()
	for _, op := range ops {
		opID := op.GetID()

		// If the received operation is already in the graph merge the edges that go into and out of the node
		// Otherwise, just insert the operation
		if storedOp, ok := gp.OpSet[opID]; ok {
			children := make(map[key.Key]bool)
			parents := make(map[key.Key]bool)
			for _, c := range storedOp.Children {
				children[key.Key(c)] = true
			}
			for _, c := range op.Children {
				children[key.Key(c)] = true
			}
			for _, p := range storedOp.Parents {
				parents[key.Key(p)] = true
			}
			for _, p := range storedOp.Parents {
				parents[key.Key(p)] = true
			}

			childrenArr := make([]string, len(children))
			i := 0
			for c := range children {
				childrenArr[i] = string(c)
				i++
			}

			parentArr := make([]string, len(parents))
			i = 0
			for p := range parents {
				parentArr[i] = string(p)
				i++
			}

			gp.OpSet[opID] = &SingleOp{Value: string(opID), Children: childrenArr, Parents: parentArr}
		} else {
			gp.OpSet[opID] = op
		}
	}
	gp.mux.Unlock()
}

func (gp *setSyncGraphProvider) Merge(ops *GraphOps) {
	gp.ReceiveUpdates(ops.Ops...)
}

func (gp *setSyncGraphProvider) Update(op *SingleOp) {
	gp.mux.Lock()

	// Go through all the users we are connected with and send them updates
	for k, v := range gp.updateStreams {
		stream := protectedStream{k, *v}
		go func() {
			if err := writeToProtectedStream(stream, op); err != nil {
				log.Printf("sending update to %v", stream.Conn().RemotePeer())
				log.Print(err)
			}
		}()
	}
	gp.mux.Unlock()
}

func (gp *setSyncGraphProvider) GetOps() []*SingleOp {
	gp.mux.Lock()
	ops := make([]*SingleOp, len(gp.OpSet))
	i := 0
	for _, v := range gp.OpSet {
		ops[i] = v
		i++
	}
	gp.mux.Unlock()
	return ops
}

const sizeLengthBytes = 8

// readNumBytesFromReader reads a specific number of bytes from a Reader, or returns an error
func readNumBytesFromReader(r io.Reader, numBytes uint64) ([]byte, error) {
	data := make([]byte, numBytes)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return data, errors.Wrap(err, "Error reading from stream")
	} else if uint64(n) != numBytes {
		return data, errors.New("Could not read full length from stream")
	}
	return data, nil
}

func ReadFromStream(s net.Stream, m message) error {
	buf := bufio.NewReader(s)
	return ReadFromReader(buf, m, s.Conn().LocalPeer().String())
}

func ReadFromReader(r io.Reader, m message, dbg string) error {
	// Protocol: uint64 MessageLength followed by byte[] MarshalledMessage

	sizeData, err := readNumBytesFromReader(r, sizeLengthBytes)
	log.Printf("peer: %v | read | %v", dbg, sizeData)

	size := binary.LittleEndian.Uint64(sizeData)
	data, err := readNumBytesFromReader(r, size)
	log.Printf("peer: %v | read | %v", dbg, data)

	err = m.Unmarshal(data)
	if err != nil {
		return err
	}
	return nil
}

func readFromProtectedStream(ps protectedStream, m message) error {
	ps.r.Lock()
	err := ReadFromStream(ps, m)
	ps.r.Unlock()
	return err
}

func writeToProtectedStream(ps protectedStream, m message) error {
	ps.w.Lock()
	err := WriteToStream(ps, m)
	ps.w.Unlock()
	return err
}

func WriteToStream(s net.Stream, m message) error {
	return WriteToWriter(s, m, s.Conn().LocalPeer().String())
}

func WriteToWriter(w io.Writer, m message, dbg string) error {
	size := m.Size()
	data, err := m.Marshal()

	if err != nil {
		return errors.Wrap(err, "Could not Marshal data")
	}

	// Protocol: uint64 MessageLength followed by byte[] MarshalledMessage
	sizeData := make([]byte, sizeLengthBytes)
	binary.LittleEndian.PutUint64(sizeData, uint64(size))
	log.Printf("peer: %v | write | %v", dbg, sizeData)

	_, err = w.Write(sizeData)
	if err != nil {
		return errors.Wrap(err, "Error writing size of data to stream")
	}

	log.Printf("peer: %v | write | %v", dbg, data)
	_, err = w.Write(data)

	return errors.Wrap(err, "Error writing data to stream")
}

// GSyncState The state of the Graph Syncronization algorithm
type GSyncState int

// The possible states of the Graph Syncronization algorithm
const (
	UNKNOWN GSyncState = iota
	SEND
	RECEIVE
	DONE
)

func gsync(gp *setSyncGraphProvider, s net.Stream, state GSyncState, done chan bool) error {
	// Track stream if it is not already tracked
	gp.mux.Lock()
	streamMux, ok := gp.updateStreams[s]
	if ok != true {
		streamMux = &rwMutex{&sync.Mutex{}, &sync.Mutex{}}
		gp.updateStreams[s] = streamMux
	}
	gp.mux.Unlock()

	ps := protectedStream{s, *streamMux}

	var err error
	startState := state
	defer func() {
		if err != nil {
			log.Printf("stream handler error: %v | state = %v", s.Conn().LocalPeer(), startState)
		}
	}()

	for state != DONE {
		switch state {
		case SEND:
			// If sending data synchronize by writing the local graph operations to the stream,
			// then read the remote graph operations and merge them into the local graph
			ops := &GraphOps{Ops: gp.GetOps()}

			if err = writeToProtectedStream(ps, ops); err != nil {
				return errors.Wrap(err, "Could not write operations to stream")
			}
			incomingOps := &GraphOps{}
			if err = readFromProtectedStream(ps, incomingOps); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}

			gp.Merge(incomingOps)
			state = DONE
		case RECEIVE:
			// If receiving data synchronize by reading the remote graph operations from the stream,
			// then merge them into the local graph and write the resulting local graph operations to the stream

			incomingOps := &GraphOps{}
			if err = readFromProtectedStream(ps, incomingOps); err != nil {
				return errors.Wrap(err, "Could not read operations from stream")
			}
			gp.Merge(incomingOps)
			sendOps := &GraphOps{Ops: gp.GetOps()}
			if err = writeToProtectedStream(ps, sendOps); err != nil {
				return errors.Wrap(err, "Could not write operations to stream")
			}
			state = DONE
		}
	}

	// After completing the initial synchronization indicate that it is completed and then start the update read loop

	done <- true

	for {
		incomingOp := &SingleOp{}

		readFromProtectedStream(ps, incomingOp)
		if err == nil {
			gp.ReceiveUpdates(incomingOp)
		} else {
			log.Fatalln(err)
		}
	}
}

//NewGraphProvider Creates a GraphProvider that manages the updates to a graph
func NewGraphProvider(ops []*SingleOp,
	ha host.Host, peerIDs []peer.ID, rngSrc mrand.Source) GraphProvider {

	// Create a pseudorandom number generator from the given pseudorandom source
	rng := mrand.New(rngSrc)

	// Create the struct containing the GraphProvider data
	opSet := make(map[key.Key]*SingleOp)
	for _, elem := range ops {
		opSet[elem.GetID()] = elem
	}

	gp := &setSyncGraphProvider{OpSet: opSet, updateStreams: make(map[net.Stream]*rwMutex)}

	// Shuffle the peers
	shuffledPeers := make([]peer.ID, len(peerIDs))
	for i, v := range rng.Perm(len(peerIDs)) {
		shuffledPeers[v] = peerIDs[i]
	}

	// Setup incoming gsync connections to perform gsync
	ha.SetStreamHandler("/gsync/1.0.0", func(s net.Stream) {
		go gsync(gp, s, RECEIVE, make(chan bool, 1))
	})

	syncedStreams := make(chan bool, len(shuffledPeers))

	// Attempt outgoing gsync connection to each of the possible peers
	// TODO: May not need to maintain all peer connections, just a subset
	for _, peer := range shuffledPeers {
		stream, err := ha.NewStream(context.Background(), peer, "/gsync/1.0.0")
		if err != nil {
			syncedStreams <- false
			log.Print(err)
			continue
		}

		go gsync(gp, stream, SEND, syncedStreams)
	}

	for index := 0; index < len(shuffledPeers); index++ {
		<-syncedStreams
	}

	return gp
}

// makeRoutedHost creates a LibP2P host with a random peer ID listening on the
// given multiaddress. It will use secio if secio is true. It will bootstrap using the
// provided PeerInfo
func makeRoutedHost(listenPort int, priv crypto.PrivKey, bootstrapPeers []pstore.PeerInfo) (host.Host, error) {

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)),
		libp2p.Identity(priv),
		libp2p.DefaultTransports,
		libp2p.DefaultMuxers,
		libp2p.DefaultSecurity,
		libp2p.NATPortMap(),
	}

	ctx := context.Background()

	basicHost, err := libp2p.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Construct a datastore (needed by the DHT). This is just a simple, in-memory thread-safe datastore.
	dstore := dsync.MutexWrap(ds.NewMapDatastore())

	// Make the DHT
	dht := dht.NewDHT(ctx, basicHost, dstore)

	// Make the routed host
	routedHost := rhost.Wrap(basicHost, dht)

	// connect to the chosen ipfs nodes
	err = bootstrapConnect(ctx, routedHost, bootstrapPeers)
	if err != nil {
		return nil, err
	}

	// Bootstrap the host
	err = dht.Bootstrap(ctx)
	if err != nil {
		return nil, err
	}

	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", routedHost.ID().Pretty()))

	// Now we can build a full multiaddress to reach this host
	// by encapsulating both addresses:
	// addr := routedHost.Addrs()[0]
	addrs := routedHost.Addrs()
	log.Println("I can be reached at:")
	for _, addr := range addrs {
		log.Println(addr.Encapsulate(hostAddr))
	}

	log.Printf("Now run \"./routed-echo -l %d -d %s\" on a different terminal\n", listenPort+1, routedHost.ID().Pretty())

	return routedHost, nil
}
