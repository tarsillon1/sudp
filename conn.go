package sudp

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
)

// ConnConfig configures the UDP connection.
type ConnConfig struct {
	Addr             *net.UDPAddr
	PongTimeout      time.Duration
	AckRetryInterval time.Duration
	AckTimeout       time.Duration
	MaxPacketSize    int32
}

// Conn a simple UDP connection.
type Conn struct {
	UDP             *net.UDPConn
	config          *ConnConfig
	readBuffer      []byte
	readMut         *sync.Mutex
	messageChansMut *sync.Mutex
	ackChansMut     *sync.Mutex
	pongChansMut    *sync.Mutex
	errChansMut     *sync.Mutex
	seqMut          *sync.Mutex
	messageChans    []chan MessageWithAddr
	ackChans        []chan ackWithAddress
	pongChans       []chan *net.UDPAddr
	errChans        []chan error
	cache           *cache
	seq             uint32
}

// NewConn creates a new UDP connection.
func NewConn(conf *ConnConfig) (*Conn, error) {
	if conf == nil {
		conf = &ConnConfig{}
	}
	if conf.PongTimeout == 0 {
		conf.PongTimeout = defaultPongTimeout
	}
	if conf.MaxPacketSize == 0 {
		conf.MaxPacketSize = defaultMaxPacketSize
	}
	if conf.AckRetryInterval == 0 {
		conf.AckRetryInterval = defaultAckRetryInterval
	}
	if conf.AckTimeout == 0 {
		conf.AckTimeout = defaultAckTimeout
	}

	serv, err := net.ListenUDP("udp", conf.Addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrConn, err)
	}
	conn := &Conn{
		UDP:             serv,
		config:          conf,
		readBuffer:      make([]byte, conf.MaxPacketSize),
		readMut:         &sync.Mutex{},
		messageChansMut: &sync.Mutex{},
		ackChansMut:     &sync.Mutex{},
		pongChansMut:    &sync.Mutex{},
		errChansMut:     &sync.Mutex{},
		seqMut:          &sync.Mutex{},
		messageChans:    make([]chan MessageWithAddr, 0),
		ackChans:        make([]chan ackWithAddress, 0),
		pongChans:       make([]chan *net.UDPAddr, 0),
		errChans:        make([]chan error, 0),
		cache:           newCache(),
		seq:             0,
	}
	return conn, err
}

// Ping A UDP address.
func (c *Conn) Ping(addr *net.UDPAddr) (time.Duration, error) {
	start := time.Now()

	_, err := c.UDP.WriteToUDP(pingPacket, addr)
	if err != nil {
		return time.Duration(0), fmt.Errorf("%w for ping: %s", ErrWrite, err)
	}

	pingChan := make(chan *net.UDPAddr)
	c.subPong(pingChan)
	defer c.unsubPong(pingChan)

	timeoutTimer := time.NewTimer(c.config.PongTimeout)
	for {
		select {
		case <-timeoutTimer.C:
			return time.Duration(0), ErrPongTimeout
		case pingAddr := <-pingChan:
			if addr.String() == pingAddr.String() {
				return time.Since(start), nil
			}
		}
	}
}

// Pub publishes a message to a client.
func (c *Conn) Pub(addr *net.UDPAddr, message *Message) error {
	if message.Seq == 0 {
		message.Seq = c.nextSeq()
	}

	b := marshalMessagePacket(message)
	_, err := c.UDP.WriteToUDP(b, addr)
	if err != nil {
		return fmt.Errorf("%w for pub: %s", ErrWrite, err)
	}
	if message.Ack {
		ackChan := make(chan ackWithAddress)
		c.subAck(ackChan)
		defer c.unsubAck(ackChan)

		timeoutTimer := time.NewTimer(c.config.AckTimeout)
		for {
			retryTimer := time.NewTimer(c.config.AckRetryInterval)
			select {
			case ack := <-ackChan:
				if ack.seq == message.Seq && addr.String() == ack.addr.String() {
					return nil
				}
			case <-retryTimer.C:
				_, err = c.UDP.WriteToUDP(b, addr)
				if err != nil {
					return fmt.Errorf("%w for pub retry: %s", ErrWrite, err)
				}
			case <-timeoutTimer.C:
				return ErrAckTimeout
			}
		}
	}
	return nil
}

// Sub will publish messages into the provided channel
// when they are received by the UDP connection from clients.
func (c *Conn) Sub(messageChan chan MessageWithAddr) {
	c.messageChansMut.Lock()
	defer c.messageChansMut.Unlock()
	for _, ch := range c.messageChans {
		if ch == messageChan {
			return
		}
	}
	c.messageChans = append(c.messageChans, messageChan)
}

// Unsub stops the UDP connection from publishing messages into the provided channel.
func (c *Conn) Unsub(messageChan chan MessageWithAddr) {
	c.messageChansMut.Lock()
	defer c.messageChansMut.Unlock()
	newMessageChans := make([]chan MessageWithAddr, 0)
	for _, ch := range c.messageChans {
		if ch != messageChan {
			newMessageChans = append(newMessageChans, ch)
		}
	}
	c.messageChans = newMessageChans
}

func (c *Conn) SubErr(errChan chan error) {
	c.errChansMut.Lock()
	defer c.errChansMut.Unlock()
	for _, ch := range c.errChans {
		if ch == errChan {
			return
		}
	}
	c.errChans = append(c.errChans, errChan)
}

func (c *Conn) UnsubErr(errChan chan error) {
	c.errChansMut.Lock()
	defer c.errChansMut.Unlock()
	newErrChans := make([]chan error, 0)
	for _, ch := range c.errChans {
		if ch != errChan {
			newErrChans = append(newErrChans, ch)
		}
	}
	c.errChans = newErrChans
}

// Poll starts reading UDP packets on a loop.
// This loop is terminated when program is interrupted.
func (c *Conn) Poll() {
	termCh := make(chan os.Signal, 5)
	signal.Notify(termCh, os.Interrupt)
	for {
		select {
		case <-termCh:
			for _, ch := range c.messageChans {
				close(ch)
			}
			return
		default:
		}
		err := c.readPacket()
		if err != nil {
			for _, ch := range c.errChans {
				ch <- err
			}
		}
	}
}

func (c *Conn) nextSeq() uint32 {
	c.seqMut.Lock()
	defer c.seqMut.Unlock()

	c.seq++
	seq := c.seq
	if c.seq == maxSeq {
		c.seq = 0
	}
	return seq
}

func (c *Conn) handlePongPacket(addr *net.UDPAddr) error {
	for _, ch := range c.pongChans {
		ch <- addr
	}
	return nil
}

func (c *Conn) pubPong(addr *net.UDPAddr) error {
	_, err := c.UDP.WriteToUDP(pongPacket, addr)
	if err != nil {
		return fmt.Errorf("%w for pong: %s", ErrWrite, err)
	}
	return nil
}

func (c *Conn) subPong(pongChan chan *net.UDPAddr) {
	c.pongChansMut.Lock()
	defer c.pongChansMut.Unlock()
	for _, ch := range c.pongChans {
		if ch == pongChan {
			return
		}
	}
	c.pongChans = append(c.pongChans, pongChan)
}

func (c *Conn) unsubPong(pongChan chan *net.UDPAddr) {
	c.pongChansMut.Lock()
	defer c.pongChansMut.Unlock()
	newPongChans := make([]chan *net.UDPAddr, 0)
	for _, ch := range c.pongChans {
		if ch != pongChan {
			newPongChans = append(newPongChans, ch)
		}
	}
	c.pongChans = newPongChans
}

// pubAck publishes a ack to a client.
func (c *Conn) pubAck(addr *net.UDPAddr, seq uint32) error {
	b := marshalAckPacket(seq)
	_, err := c.UDP.WriteToUDP(b, addr)
	if err != nil {
		return fmt.Errorf("%w for ack: %s", ErrWrite, err)
	}
	return nil
}

func (c *Conn) subAck(ackChan chan ackWithAddress) {
	c.ackChansMut.Lock()
	defer c.ackChansMut.Unlock()
	for _, ch := range c.ackChans {
		if ch == ackChan {
			return
		}
	}
	c.ackChans = append(c.ackChans, ackChan)
}

func (c *Conn) unsubAck(ackChan chan ackWithAddress) {
	c.ackChansMut.Lock()
	defer c.ackChansMut.Unlock()
	newAckChans := make([]chan ackWithAddress, 0)
	for _, ch := range c.ackChans {
		if ch != ackChan {
			newAckChans = append(newAckChans, ch)
		}
	}
	c.ackChans = newAckChans
}

func (c *Conn) handleMessagePacket(addr *net.UDPAddr, n int) error {
	msg := Message{}
	unmarshalMessagePacket(c.readBuffer, n, &msg)

	if msg.Ack {
		err := c.pubAck(addr, msg.Seq)
		if err != nil {
			return err
		}
	}

	strAddr := addr.String()
	if c.cache.has(strAddr, msg.Seq) {
		return nil // Message with this addr + seq has already been processed.
	}
	c.cache.set(strAddr, msg.Seq, c.config.AckTimeout)

	messageWithAddr := MessageWithAddr{
		Message: msg,
		Addr:    addr,
	}
	for _, ch := range c.messageChans {
		ch <- messageWithAddr
	}
	return nil
}

func (c *Conn) handleAckPacket(addr *net.UDPAddr) error {
	seq := unmarshalAckPacket(c.readBuffer)
	for _, ch := range c.ackChans {
		ch <- ackWithAddress{
			seq:  seq,
			addr: addr,
		}
	}
	return nil
}

// readPacket reads the next packet from a UDP connection.
//
// Each packet starts with a header describing the type of the packet.
// The first byte represents the type of packet (limit of 256 different types of packets).
func (c *Conn) readPacket() error {
	c.readMut.Lock()
	defer c.readMut.Unlock()

	n, addr, err := c.UDP.ReadFromUDP(c.readBuffer)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrRead, err)
	}
	packetType := c.readBuffer[0]
	switch packetType {
	case pingTag:
		return c.pubPong(addr)
	case pongTag:
		return c.handlePongPacket(addr)
	case ackTag:
		return c.handleAckPacket(addr)
	case messageWithAckTag, messageTag:
		return c.handleMessagePacket(addr, n)
	}
	return ErrUnknownType
}
