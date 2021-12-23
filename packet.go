package sudp

import "net"

/* Packet Tags */

const (
	tagPing           = 0 // A ping sent to a specified UDP address.
	tagPong           = 1 // A pong sent from the receiver of a ping back to its sender.
	tagAck            = 2 // An acknowledgement from message receiver to original sender.
	tagMessage        = 3 // A message that does not require acknowledgment by receiver.
	tagMessageWithAck = 4 // A message that does requires acknowledgment by receiver.
)

var (
	packetPing = []byte{tagPing}
	packetPong = []byte{tagPong}
)

// Message respresents a message.
type Message struct {
	To   *net.UDPAddr // Address of the recipient.
	Seq  uint32       // Unique message sequence number.
	Data []byte       // The bytes to send to the recipient.
	Ack  bool         // True if this message should be acknowledged by recipient.
}

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
		b[0] = tagMessageWithAck
	} else {
		b[0] = tagMessage
	}
	for i := uint32(0); i < 4; i++ {
		b[i+1] = byte((msg.Seq >> (8 * i)) & 0xff)
	}
	for i := 0; i < dataLen; i++ {
		b[i+5] = msg.Data[i]
	}
	return b
}

// unmarshalMessagePacket converts an array of bytes into a message.
//
// In order to unmarshal the message packet, we follow these steps:
//    1. Check the first byte to determine tag type. If tag is with ack, set Message.Ack to true.
//    2. Convert the next four byte to a uint32. Set Message.Seq to derived uint32.
//    3. Set Message.Data to the remaining byte until specified index n.
func unmarshalMessagePacket(b []byte, n int) *Message {
	msg := &Message{}
	if b[0] == tagMessageWithAck {
		msg.Ack = true
	}
	for i := uint32(0); i < 4; i++ {
		msg.Seq |= uint32(b[i+1]) << (8 * i)
	}
	if len(b) > 5 {
		msg.Data = b[5:n]
	}
	return msg
}

type ackWithAddress struct {
	addr *net.UDPAddr
	seq  uint32
}

// marshalAckPacket converts a uint32 representing a message sequence number to bytes.
func marshalAckPacket(seq uint32) []byte {
	b := make([]byte, 5)
	b[0] = tagAck
	for i := uint32(0); i < 4; i++ {
		b[i+1] = byte((seq >> (8 * i)) & 0xff)
	}
	return b
}

// unmarshalAckPacket converts an array of bytes representing a message sequence number back into a uint32.
func unmarshalAckPacket(b []byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(b[i+1]) << (8 * i)
	}
	return r
}
