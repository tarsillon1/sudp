package sudp

import "net"

// Message respresents a message.
type Message struct {
	Seq  uint32
	Data []byte
	Ack  bool
}

// MessageWithAddr is a message with the address of the sender.
type MessageWithAddr struct {
	Message
	Addr *net.UDPAddr
}

type ackWithAddress struct {
	addr *net.UDPAddr
	seq  uint32
}
