package sudp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxPacketSize    = 8192
	defaultMaxBufferSize    = 1024
	defaultPongTimeout      = time.Second * 2
	defaultAckRetryInterval = time.Millisecond * 500
	defaultAckTimeout       = time.Second * 5
)

var (
	ErrPongTimeout = errors.New("timed out while waiting for pong")
	ErrAckTimeout  = errors.New("message was not acknowledged by receiver within the specified timeout")
)

// MessageWithAddr is a message with the address of the sender.
type MessageWithAddr struct {
	*Message
	Addr *net.UDPAddr
}

// ackWithAddress is a ack with the address of the sender.
type ackWithAddress struct {
	*Ack
	addr *net.UDPAddr
}

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
	messageChans    []chan *MessageWithAddr
	ackChans        []chan *ackWithAddress
	pongChans       []chan *net.UDPAddr
	errChans        []chan error
	messageCache    *messageCache
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
		return nil, fmt.Errorf("failed to connect to udp addr: %s", err)
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
		messageChans:    make([]chan *MessageWithAddr, 0),
		ackChans:        make([]chan *ackWithAddress, 0),
		pongChans:       make([]chan *net.UDPAddr, 0),
		errChans:        make([]chan error, 0),
		messageCache:    newMessageCache(),
	}
	go conn.poll()
	return conn, err
}

// Ping A UDP address.
func (c *Conn) Ping(addr *net.UDPAddr) (time.Duration, error) {
	start := time.Now()

	_, err := c.UDP.WriteToUDP(pingHeader, addr)
	if err != nil {
		return time.Duration(0), fmt.Errorf("failed to write udp message from server: %s", err)
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
				return time.Now().Sub(start), nil
			}
		}
	}
}

// Pub publishes a message to a client.
func (c *Conn) Pub(addr *net.UDPAddr, message *Message) error {
	b, err := encodeMessagePacket(message)
	if err != nil {
		return fmt.Errorf("failed to encode message packet: %s", err)
	}
	_, err = c.UDP.WriteToUDP(b, addr)
	if err != nil {
		return fmt.Errorf("failed to write udp message from server: %s", err)
	}
	if message.Ack != nil {
		ackChan := make(chan *ackWithAddress)
		c.subAck(ackChan)
		defer c.unsubAck(ackChan)

		timeoutTimer := time.NewTimer(c.config.AckTimeout)
		for {
			retryTimer := time.NewTimer(c.config.AckRetryInterval)
			select {
			case ack := <-ackChan:
				if ack.Id == message.Seq && addr.String() == ack.addr.String() {
					return nil
				}
			case <-retryTimer.C:
				_, err = c.UDP.WriteToUDP(b, addr)
				if err != nil {
					return fmt.Errorf("failed to write udp message from server: %s", err)
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
func (c *Conn) Sub(messageChan chan *MessageWithAddr) {
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
func (c *Conn) Unsub(messageChan chan *MessageWithAddr) {
	c.messageChansMut.Lock()
	defer c.messageChansMut.Unlock()
	newMessageChans := make([]chan *MessageWithAddr, 0)
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

func (c *Conn) poll() {
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt)
	for {
		select {
		case <-ch:
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

func (c *Conn) pubPong(addr *net.UDPAddr) error {
	_, err := c.UDP.WriteToUDP(pongHeader, addr)
	if err != nil {
		return fmt.Errorf("failed to write udp message from server: %s", err)
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
func (c *Conn) pubAck(addr *net.UDPAddr, ack *Ack) error {
	b, err := encodeAckPacket(ack)
	if err != nil {
		return fmt.Errorf("failed to encode ack packet: %s", err)
	}
	_, err = c.UDP.WriteToUDP(b, addr)
	if err != nil {
		return fmt.Errorf("failed to write udp ack from server: %s", err)
	}
	return nil
}

func (c *Conn) subAck(ackChan chan *ackWithAddress) {
	c.ackChansMut.Lock()
	defer c.ackChansMut.Unlock()
	for _, ch := range c.ackChans {
		if ch == ackChan {
			return
		}
	}
	c.ackChans = append(c.ackChans, ackChan)
}

func (c *Conn) unsubAck(ackChan chan *ackWithAddress) {
	c.ackChansMut.Lock()
	defer c.ackChansMut.Unlock()
	newAckChans := make([]chan *ackWithAddress, 0)
	for _, ch := range c.ackChans {
		if ch != ackChan {
			newAckChans = append(newAckChans, ch)
		}
	}
	c.ackChans = newAckChans
}

func (c *Conn) handleMessagePacket(addr *net.UDPAddr, b []byte) error {
	message := Message{}
	err := proto.Unmarshal(b, &message)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message bytes: %s", err)
	}

	if message.Ack != nil && *message.Ack {
		err = c.pubAck(addr, &Ack{Id: message.Seq})
		if err != nil {
			return fmt.Errorf("failed to ack received message: %s", err)
		}
	}

	if c.messageCache.has(addr, message.Seq) {
		return nil // Message with this addr + seq has already been processed.
	}

	c.messageCache.set(addr, message.Seq, c.config.AckTimeout)

	messageWithAddr := &MessageWithAddr{
		Message: &message,
		Addr:    addr,
	}
	for _, ch := range c.messageChans {
		ch <- messageWithAddr
	}
	return nil
}

func (c *Conn) handleAckPacket(addr *net.UDPAddr, b []byte) error {
	ack := Ack{}
	err := proto.Unmarshal(b, &ack)
	if err != nil {
		return fmt.Errorf("failed to unmarshal ack bytes: %s", err)
	}
	for _, ch := range c.ackChans {
		ch <- &ackWithAddress{
			Ack:  &ack,
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
		return fmt.Errorf("failed to read from UDP connection: %s", err)
	}
	packetType := c.readBuffer[0]
	packetBytes := c.readBuffer[1:n]
	switch packetType {
	case pingTag:
		return c.pubPong(addr)
	case pongTag:
		for _, ch := range c.pongChans {
			ch <- addr
		}
	case ackTag:
		return c.handleAckPacket(addr, packetBytes)
	case messageTag:
		return c.handleMessagePacket(addr, packetBytes)
	}
	return nil
}
