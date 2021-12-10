package sudp

import (
	"fmt"
	"net"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestNewConn(t *testing.T) {
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}

	messageChan := make(chan *MessageWithAddr, 1)
	conn1.Sub(messageChan)

	const expectedMsg = "hello world"

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	err = conn2.Pub(conn1Addr, &Message{
		Data: []byte(expectedMsg),
	})
	if err != nil {
		t.Fatalf("failed to send message from client: %s", err)
	}

	msg := <-messageChan
	if string(msg.Data) != expectedMsg {
		t.Fatalf("got unexpected message: %s", string(msg.Data))
	}
}

func TestNewConnWithAck(t *testing.T) {
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}

	messageChan := make(chan *MessageWithAddr, 1)
	conn1.Sub(messageChan)

	const expectedMsg = "hello world"

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}

	errChan := make(chan error)
	go func() {
		err = conn2.Pub(conn1Addr, &Message{
			Data: []byte(expectedMsg),
			Ack:  proto.Bool(true),
		})
		if err != nil {
			errChan <- fmt.Errorf("failed to send message from client: %s", err)
		}
		close(errChan)
	}()

	msg := <-messageChan
	if string(msg.Data) != expectedMsg {
		t.Fatalf("got unexpected message: %s", string(msg.Data))
	}

	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnPing(t *testing.T) {
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}

	messageChan := make(chan *MessageWithAddr, 1)
	conn1.Sub(messageChan)

	const expectedMsg = "hello world"

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}

	d, err := conn2.Ping(conn1Addr)
	if err != nil {
		t.Fatalf("failed to ping udp server: %s", err)
	}

	milli := d.Milliseconds()
	if milli > 10 {
		t.Fatalf("unexpectedly long ping pong latency")
	}
}
