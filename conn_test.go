package sudp

import (
	"fmt"
	"net"
	"testing"
	"time"
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

	messageChan := make(chan MessageWithAddr, 1)
	conn1.Sub(messageChan)
	go conn1.Poll()

	const expectedMsg = "hello world"

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	go conn2.Poll()

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
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8081")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}

	messageChan := make(chan MessageWithAddr, 1)
	conn1.Sub(messageChan)
	go conn1.Poll()

	const expectedMsg = "hello world"

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	go conn2.Poll()

	errChan := make(chan error)
	go func() {
		err = conn2.Pub(conn1Addr, &Message{
			Data: []byte(expectedMsg),
			Ack:  true,
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
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8082")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}
	go conn1.Poll()

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	go conn2.Poll()

	d, err := conn2.Ping(conn1Addr)
	if err != nil {
		t.Fatalf("failed to ping udp server: %s", err)
	}

	milli := d.Milliseconds()
	if milli > 2 {
		t.Fatalf("unexpectedly long ping pong latency")
	}
	fmt.Println(d.Microseconds())
}

func TestExactlyOnceProcessing(t *testing.T) {
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8083")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}

	messageChan := make(chan MessageWithAddr, 2)
	conn1.Sub(messageChan)
	go conn1.Poll()

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	go conn2.Poll()

	msg := &Message{Data: []byte("hello world")}
	err = conn2.Pub(conn1Addr, msg)
	if err != nil {
		t.Fatalf("failed to send message: %s", err)
	}
	err = conn2.Pub(conn1Addr, msg)
	if err != nil {
		t.Fatalf("failed to send message: %s", err)
	}

	<-messageChan
	select {
	case <-messageChan:
		t.Fatal("received unexpected message")
	case <-time.NewTimer(time.Second).C:
		break
	}
}
