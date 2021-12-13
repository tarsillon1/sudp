package sudp

import "net"

// Message respresents a message.
type Message struct {
	To   *net.UDPAddr // Address of the recipient.
	Seq  uint32       // Unique message sequence number.
	Data []byte       // The bytes to send to the recipient.
	Ack  bool         // True if this message should be acknowledged by recipient.
}

type ackWithAddress struct {
	addr *net.UDPAddr
	seq  uint32
}
