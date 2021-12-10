package sudp

import (
	"fmt"

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

func encodeMessagePacket(msg *Message) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %s", err)
	}
	return append(messageHeader, b...), nil
}

func encodeAckPacket(ack *Ack) ([]byte, error) {
	b, err := proto.Marshal(ack)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ack: %s", err)
	}
	return append(ackHeader, b...), nil
}

func encodePingPacket(ping *Ping) ([]byte, error) {
	b, err := proto.Marshal(ping)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ping: %s", err)
	}
	return append(pingHeader, b...), nil
}

func encodePongPacket(pong *Pong) ([]byte, error) {
	b, err := proto.Marshal(pong)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pong: %s", err)
	}
	return append(pongHeader, b...), nil
}
