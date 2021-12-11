package sudp

import (
	"errors"
	"time"
)

const (
	pingTag           = 0
	pongTag           = 1
	ackTag            = 2
	messageTag        = 3
	messageWithAckTag = 4 // Save using an extra byte in message header by making message with ack its own tag.
)

var (
	pingPacket = []byte{pingTag}
	pongPacket = []byte{pongTag}
)

const (
	maxSeq                  = uint32(4294967295)
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

// marshalMessagePacket marshals a message into an array of bytes.
// Each message packet starts with a header, followed by the message data.
// The following describes the header format:
//    - The header is 5 bytes long.
//    - The first byte represents the packet type.
//    - Save using an extra byte in message header by making message with ack its own tag.
//    - The next four bytes is the message sequence.
func marshalMessagePacket(msg *Message) []byte {
	dataLen := len(msg.Data)
	b := make([]byte, 5+dataLen)
	if msg.Ack {
		b[0] = messageWithAckTag
	} else {
		b[0] = messageTag
	}
	for i := uint32(0); i < 4; i++ {
		b[i+1] = byte((msg.Seq >> (8 * i)) & 0xff)
	}
	for i := 0; i < dataLen; i++ {
		b[i+5] = msg.Data[i]
	}
	return b
}

func unmarshalMessagePacket(b []byte, n int, msg *Message) {
	for i := uint32(0); i < 4; i++ {
		msg.Seq |= uint32(b[i+1]) << (8 * i)
	}
	if b[0] == messageWithAckTag {
		msg.Ack = true
	}
	if len(b) > 5 {
		msg.Data = b[5:n]
	}
}

func marshalAckPacket(seq uint32) []byte {
	b := make([]byte, 5)
	b[0] = ackTag
	for i := uint32(0); i < 4; i++ {
		b[i+1] = byte((seq >> (8 * i)) & 0xff)
	}
	return b
}

func unmarshalAckPacket(b []byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(b[i+1]) << (8 * i)
	}
	return r
}
