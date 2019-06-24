package sync

import (
	fmt "fmt"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	net "github.com/libp2p/go-libp2p-net"
	"github.com/pkg/errors"

	lutils "github.com/aschmahmann/ipshare/utils"
)

func init() {
	cbor.RegisterCborType(GSyncMessage{})
	cbor.RegisterCborType(FullSendGSync{})
	cbor.RegisterCborType(AddNodeOperation{})
	cbor.RegisterCborType(RequestFullSendGSync{})
}

// GSyncMessage is the container for all messages in the gsync protocol
type GSyncMessage struct {
	MessageType GSyncState
	Msg         []byte
}

// Marshal returns the byte representation of the object
func (msg GSyncMessage) Marshal() ([]byte, error) {
	return cbor.DumpObject(msg)
}

// Unmarshal fills the structure with data from the bytes
func (msg *GSyncMessage) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, msg)
}

// GSyncState The state of the Graph Syncronization algorithm
type GSyncState int

// The possible states of the Graph Syncronization algorithm
const (
	UNKNOWN GSyncState = iota
	FULL_GRAPH
	UPDATE
	REQUEST_FULL_GRAPH
)

// AddNodeOperation is the basic operation for adding a node to a DAG
type AddNodeOperation struct {
	Value   *cid.Cid
	Parents []*cid.Cid
}

// Marshal returns the byte representation of the object
func (msg AddNodeOperation) Marshal() ([]byte, error) {
	return cbor.DumpObject(msg)
}

// Unmarshal fills the structure with data from the bytes
func (msg *AddNodeOperation) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, msg)
}

// FullSendGSync is a (large) message that sends a peer's full state about the graph being synchronized
type FullSendGSync struct {
	GraphID    string
	Operations []*AddNodeOperation
}

// Marshal returns the byte representation of the object
func (msg FullSendGSync) Marshal() ([]byte, error) {
	return cbor.DumpObject(msg)
}

// Unmarshal fills the structure with data from the bytes
func (msg *FullSendGSync) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, msg)
}

// FullSendGSync is a (large) message that sends a peer's full state about the graph being synchronized
type RequestFullSendGSync struct {
	GraphID string
}

// Marshal returns the byte representation of the object
func (msg RequestFullSendGSync) Marshal() ([]byte, error) {
	return cbor.DumpObject(msg)
}

// Unmarshal fills the structure with data from the bytes
func (msg *RequestFullSendGSync) Unmarshal(mk []byte) error {
	return cbor.DecodeInto(mk, msg)
}

func asyncGsyncReceiver(gs GraphSynchronizationManager, s net.Stream, done chan net.Stream) error {
	ps := lutils.NewProtectedStream(s)

	isDone := false

	var err error
	defer func() {
		if err != nil {
			closeErr := net.FullClose(s)
			if closeErr != nil {
				log.Info(closeErr)
			}
		} else {
			if err = s.Close(); err != nil {
				log.Info(err)
			}
		}
		if !isDone {
			done <- s
		}
	}()

	gsyncMsg := &GSyncMessage{}
	if err = lutils.ReadFromProtectedStream(ps, gsyncMsg); err != nil {
		return errors.Wrap(err, "Could not read message from stream")
	}

	switch gsyncMsg.MessageType {
	case FULL_GRAPH:
		fallthrough
	case UPDATE:
		typedMsg := &FullSendGSync{}
		if err = typedMsg.Unmarshal(gsyncMsg.Msg); err != nil {
			return errors.Wrap(err, "Could not unmarshal message")
		}

		graphID := typedMsg.GraphID

		graph := gs.GetGraph(graphID)

		if graph == nil {
			err = fmt.Errorf("The Graph:%v could not be found", graphID)
			return err
		}

		graph.ReceiveUpdates(typedMsg.Operations...)
	case REQUEST_FULL_GRAPH:
		typedMsg := &RequestFullSendGSync{}
		if err = typedMsg.Unmarshal(gsyncMsg.Msg); err != nil {
			return errors.Wrap(err, "Could not unmarshal message")
		}

		graphID := typedMsg.GraphID
		graph := gs.GetGraph(graphID)

		msg := &FullSendGSync{GraphID: graphID, Operations: graph.GetOps()}
		msgBytes, err := msg.Marshal()
		if err != nil {
			return err
		}

		gsynMsg := &GSyncMessage{MessageType: FULL_GRAPH, Msg: msgBytes}
		if err = lutils.WriteToProtectedStream(ps, gsynMsg); err != nil {
			return err
		}
	}

	// After completing the initial synchronization indicate that it is completed
	isDone = true
	done <- nil
	return nil
}
