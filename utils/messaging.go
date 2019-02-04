package utils

import (
	"bufio"
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	net "github.com/libp2p/go-libp2p-net"
	"github.com/pkg/errors"
	"io"
	"sync"
)

// Message A protobuf message
type Message interface {
	proto.Marshaler
	proto.Unmarshaler
}

// RWMutex A structure containing separate read and write mutexes
type RWMutex struct {
	R, W *sync.Mutex
}

// ProtectedStream A stream protected by a RWMutex
type ProtectedStream struct {
	net.Stream
	RWMutex
}

const sizeLengthBytes = 8

func NewProtectedStream(s net.Stream) ProtectedStream {
	streamMux := &RWMutex{R: &sync.Mutex{}, W: &sync.Mutex{}}
	ps := ProtectedStream{Stream: s, RWMutex: *streamMux}
	return ps
}

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

func ReadFromStream(s net.Stream, m Message) error {
	buf := bufio.NewReader(s)
	return ReadFromReader(buf, m, s.Conn().LocalPeer().String())
}

func ReadFromReader(r io.Reader, m Message, dbg string) error {
	// Protocol: uint64 MessageLength followed by byte[] MarshalledMessage

	sizeData, err := readNumBytesFromReader(r, sizeLengthBytes)
	if err != nil {
		return err
	}

	size := binary.LittleEndian.Uint64(sizeData)
	data, err := readNumBytesFromReader(r, size)
	if err != nil {
		return err
	}

	err = m.Unmarshal(data)
	return err
}

func ReadFromProtectedStream(ps ProtectedStream, m Message) error {
	ps.R.Lock()
	err := ReadFromStream(ps, m)
	ps.R.Unlock()
	return err
}

func WriteToProtectedStream(ps ProtectedStream, m Message) error {
	ps.W.Lock()
	err := WriteToStream(ps, m)
	ps.W.Unlock()
	return err
}

func WriteToStream(s net.Stream, m Message) error {
	return WriteToWriter(s, m, s.Conn().LocalPeer().String())
}

func WriteToWriter(w io.Writer, m Message, dbg string) error {
	data, err := m.Marshal()
	if err != nil {
		return errors.Wrap(err, "Could not Marshal data")
	}

	size := len(data)

	// Protocol: uint64 MessageLength followed by byte[] MarshalledMessage
	sizeData := make([]byte, sizeLengthBytes)
	binary.LittleEndian.PutUint64(sizeData, uint64(size))

	_, err = w.Write(sizeData)
	if err != nil {
		return errors.Wrap(err, "Error writing size of data to stream")
	}

	_, err = w.Write(data)

	return errors.Wrap(err, "Error writing data to stream")
}
