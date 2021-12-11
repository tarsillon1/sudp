package sudp

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	pingTag    = 0
	pongTag    = 1
	ackTag     = 2
	messageTag = 3
)

var (
	messageHeader = []byte{messageTag}
	ackHeader     = []byte{ackTag}
	pingHeader    = []byte{pingTag}
	pongHeader    = []byte{pongTag}
)

const (
	defaultMaxPacketSize    = 8192
	defaultMaxBufferSize    = 1024
	defaultPongTimeout      = time.Second * 2
	defaultAckRetryInterval = time.Millisecond * 500
	defaultAckTimeout       = time.Second * 5
)

var (
	ErrConn        = errors.New("failed to connect to udp address")
	ErrWrite       = errors.New("failed to write to udp connection")
	ErrRead        = errors.New("failed to read from udp connection")
	ErrMarshal     = errors.New("failed to marshal")
	ErrUnmarshal   = errors.New("failed to unmarshal packet bytes")
	ErrUnknownType = errors.New("received unknown packet type")
	ErrPongTimeout = errors.New("timed out while waiting for pong")
	ErrAckTimeout  = errors.New("message was not acknowledged by receiver within the specified timeout")
)

func Bool(b bool) *bool {
	return &b
}

func encodeMessagePacket(msg *Message) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("%w message: %s", ErrMarshal, err)
	}
	return append(messageHeader, b...), nil
}

func encodeAckPacket(ack *Ack) ([]byte, error) {
	b, err := proto.Marshal(ack)
	if err != nil {
		return nil, fmt.Errorf("%w ack: %s", ErrMarshal, err)
	}
	return append(ackHeader, b...), nil
}
