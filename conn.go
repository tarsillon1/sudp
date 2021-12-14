package sudp

import (
	"fmt"
	"net"
	"os"
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
	UDP          *net.UDPConn
	config       *ConnConfig
	closeMut     *sync.Mutex
	ackChansMut  *sync.Mutex
	pongChansMut *sync.Mutex
	seqMut       *sync.Mutex
	ackChans     []chan ackWithAddress
	pongChans    []chan *net.UDPAddr
	cache        *cache
	seq          uint32
	isClosed     bool
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
		UDP:          serv,
		config:       conf,
		closeMut:     &sync.Mutex{},
		ackChansMut:  &sync.Mutex{},
		pongChansMut: &sync.Mutex{},
		seqMut:       &sync.Mutex{},
		ackChans:     make([]chan ackWithAddress, 0),
		pongChans:    make([]chan *net.UDPAddr, 0),
		cache:        newCache(),
		seq:          0,
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
func (c *Conn) Pub(msg *Message) error {
	if msg.Seq == 0 {
		msg.Seq = c.nextSeq()
	}

	b := marshalMessagePacket(msg)
	_, err := c.UDP.WriteToUDP(b, msg.To)
	if err != nil {
		return fmt.Errorf("%w for pub: %s", ErrWrite, err)
	}
	if !msg.Ack {
		return nil
	}

	ackChan := make(chan ackWithAddress)
	c.subAck(ackChan)
	defer c.unsubAck(ackChan)

	timeoutTimer := time.NewTimer(c.config.AckTimeout)
	for {
		retryTimer := time.NewTimer(c.config.AckRetryInterval)
		select {
		case ack := <-ackChan:
			if ack.seq == msg.Seq && msg.To.String() == ack.addr.String() {
				return nil
			}
		case <-retryTimer.C:
			_, err = c.UDP.WriteToUDP(b, msg.To)
			if err != nil {
				return fmt.Errorf("%w for pub retry: %s", ErrWrite, err)
			}
		case <-timeoutTimer.C:
			return ErrAckTimeout
		}
	}
}

// Poll starts a go routine that reads and handles UDP packets on a loop.
// Multiple goroutines may invoke Poll simultaneously.
//
// This function returns two channels of which need to be read from in order to avoid deadlock:
//    chan *Message - used to handle messages received while polling.
//    chan error    - used to handle errors encountered while polling.
//
// These channels will automatically be closed when the polling loop is terminated.
// The polling loop will be termianted when Conn.Close() is invoked.
func (c *Conn) Poll() (chan *Message, chan error, func()) {
	doneChan := make(chan bool)
	errChan := make(chan error, errChanSize)
	msgChan := make(chan *Message, msgChanSize)

	isClosed := false
	closeLoop := func() {
		c.closeMut.Lock()
		defer c.closeMut.Unlock()

		isClosed = true
		c.UDP.SetReadDeadline(time.Now()) // Set read deadline to escape blocking ReadFromUDP invocation.
		<-doneChan
		c.UDP.SetReadDeadline(timeZeroVal) // Reset read deadline.
	}

	go func() {
		defer close(doneChan)
		defer close(errChan)
		defer close(msgChan)

		buff := make([]byte, c.config.MaxPacketSize)
		for !c.isClosed && !isClosed {
			msg, err := c.handlePacket(buff)

			if err != nil && !os.IsTimeout(err) && !c.isClosed && !isClosed {
				errChan <- err
			}
			if msg != nil {
				msgChan <- msg
			}
		}
	}()
	return msgChan, errChan, closeLoop
}

// Close closes the UDP connection.
func (c *Conn) Close() error {
	c.isClosed = true
	return c.UDP.Close()
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

func (c *Conn) handleMessagePacket(buff []byte, n int, addr *net.UDPAddr) (*Message, error) {
	msg := unmarshalMessagePacket(buff, n)

	if msg.Ack {
		err := c.pubAck(addr, msg.Seq)
		if err != nil {
			return nil, err
		}
	}

	if !c.cache.set(addr.String(), msg.Seq, c.config.AckTimeout) {
		return nil, nil // Message with this addr + seq has already been processed.
	}

	msg.To = addr
	return msg, nil
}

func (c *Conn) handleAckPacket(buff []byte, addr *net.UDPAddr) error {
	seq := unmarshalAckPacket(buff)
	for _, ch := range c.ackChans {
		ch <- ackWithAddress{
			seq:  seq,
			addr: addr,
		}
	}
	return nil
}

func (c *Conn) handlePongPacket(addr *net.UDPAddr) error {
	for _, ch := range c.pongChans {
		ch <- addr
	}
	return nil
}

func (c *Conn) handlePingPacket(addr *net.UDPAddr) error {
	_, err := c.UDP.WriteToUDP(pongPacket, addr)
	if err != nil {
		return fmt.Errorf("%w for pong: %s", ErrWrite, err)
	}
	return nil
}

// handlePacket reads and handles the next packet from a UDP connection.
//
// Each packet starts with a header describing the type of the packet.
// The first byte represents the type of packet (limit of 256 different types of packets).
func (c *Conn) handlePacket(buff []byte) (*Message, error) {
	n, addr, err := c.UDP.ReadFromUDP(buff)
	if err != nil {
		if os.IsTimeout(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", ErrRead, err)
	}
	switch buff[0] {
	case pingTag:
		return nil, c.handlePingPacket(addr)
	case pongTag:
		return nil, c.handlePongPacket(addr)
	case ackTag:
		return nil, c.handleAckPacket(buff, addr)
	case messageWithAckTag, messageTag:
		return c.handleMessagePacket(buff, n, addr)
	}
	return nil, ErrUnknownType
}
