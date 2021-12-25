package sudp

import (
	"encoding/binary"
	"net"
)

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
	From *net.UDPAddr // Address of the sender.
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
	binary.LittleEndian.PutUint32(b[1:5], msg.Seq)
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
	msg.Seq = binary.LittleEndian.Uint32(b[1:5])
	if len(b) > 5 {
		msg.Data = b[5:n]
	}
	return msg
}

// Ack is the acknowledgement of a message packet.
type Ack struct {
	To   *net.UDPAddr
	From *net.UDPAddr
	Seq  uint32
	Data []byte
}

// marshalAckPacket marshals a ack into an array of bytes.
func marshalAckPacket(ack *Ack) []byte {
	dataLen := len(ack.Data)
	b := make([]byte, 5+dataLen)
	b[0] = tagAck
	binary.LittleEndian.PutUint32(b[1:5], ack.Seq)

	slice := b[5:]
	for i := 0; i < dataLen; i++ {
		slice[i] = ack.Data[i]
	}
	return b
}

// unmarshalAckPacket converts an array of bytes into an ack.
func unmarshalAckPacket(b []byte, n int) *Ack {
	ack := &Ack{}
	ack.Seq = binary.LittleEndian.Uint32(b[1:5])
	if len(b) > 5 {
		ack.Data = b[5:n]
	}
	return ack
}
