package sudp

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestNewConn(t *testing.T) {
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8079")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}
	defer conn1.Close()

	msgChan, errChan, _ := conn1.Poll()

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

	_, err = conn2.Pub(&Message{
		To:   conn1Addr,
		Data: []byte(expectedMsg),
	})
	if err != nil {
		t.Fatalf("failed to send message from client: %s", err)
	}
	defer conn2.Close()

	msg := <-msgChan
	if string(msg.Data) != expectedMsg {
		t.Fatalf("got unexpected message: %s", string(msg.Data))
	}

	select {
	case err := <-errChan:
		t.Fatalf("got error %s", err)
	default:
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
	defer conn1.Close()

	msgChan, errChan, _ := conn1.Poll()

	const expectedMsg = "hello world"
	const expectedAck = "foo bar"

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	defer conn2.Close()
	go conn2.Poll()

	pubMsg := &Message{
		To:   conn1Addr,
		Data: []byte(expectedMsg),
		Ack:  true,
	}
	go func() {
		ack, err := conn2.Pub(pubMsg)
		if err != nil {
			errChan <- fmt.Errorf("failed to send message from client: %s", err)
			return
		}
		strAck := string(ack.Data)
		strExpectedAck := string(expectedAck)
		if strAck != strExpectedAck {
			errChan <- fmt.Errorf("received unexpected ack data: got [%s], expected [%s]", strAck, strExpectedAck)
			return
		}
		errChan <- nil
	}()

	resMsg := <-msgChan
	if string(resMsg.Data) != expectedMsg {
		t.Fatalf("got unexpected message: %s", string(resMsg.Data))
	}
	if !resMsg.Ack {
		t.Fatalf("unexpected message ack value: %v", resMsg.Ack)
	}
	conn1.Ack(&Ack{
		To:   resMsg.From,
		Seq:  resMsg.Seq,
		Data: []byte(expectedAck),
	})

	err = <-errChan
	if err != nil {
		t.Fatalf("got error %s", err)
	}

	ack, err := conn2.Pub(pubMsg)
	if err != nil {
		t.Fatalf("got error %s", err)
	}
	strAck := string(ack.Data)
	strExpectedAck := string(expectedAck)
	if strAck != strExpectedAck {
		t.Fatalf("received unexpected ack data: got [%s], expected [%s]", strAck, strExpectedAck)
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
	defer conn1.Close()
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
	defer conn2.Close()

	milli := d.Milliseconds()
	if milli > 2 {
		t.Fatalf("unexpectedly long ping pong latency")
	}
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
	defer conn1.Close()

	msgChan, errChan, _ := conn1.Poll()

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	defer conn2.Close()
	go conn2.Poll()

	msg := &Message{To: conn1Addr, Data: []byte("hello world")}
	_, err = conn2.Pub(msg)
	if err != nil {
		t.Fatalf("failed to send message: %s", err)
	}
	_, err = conn2.Pub(msg)
	if err != nil {
		t.Fatalf("failed to send message: %s", err)
	}

	<-msgChan
	select {
	case err := <-errChan:
		t.Fatalf("got error %s", err)
	case <-msgChan:
		t.Fatal("received unexpected message")
	case <-time.NewTimer(time.Second).C:
		break
	}
}

func TestMultiplePoll(t *testing.T) {
	conn1Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8083")
	if err != nil {
		t.Fatalf("failed to resolve udp addr: %s", err)
	}

	conn1, err := NewConn(&ConnConfig{Addr: conn1Addr})
	if err != nil {
		t.Fatalf("failed to init udp server: %s", err)
	}

	msgChan1, errChan1, close1 := conn1.Poll()

	msgChan2, errChan2, _ := conn1.Poll()

	conn2Addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve local udp address: %s", err)
	}

	conn2, err := NewConn(&ConnConfig{Addr: conn2Addr})
	if err != nil {
		t.Fatalf("failed to init udp client: %s", err)
	}
	defer conn2.Close()

	msg := &Message{To: conn1Addr, Data: []byte("hello world")}
	_, err = conn2.Pub(msg)
	if err != nil {
		t.Fatalf("failed to send message: %s", err)
	}

	select {
	case <-msgChan1:
	case <-msgChan2:
	}

	close1()

	msg = &Message{To: conn1Addr, Data: []byte("hello again")}
	_, err = conn2.Pub(msg)
	if err != nil {
		t.Fatalf("failed to send message: %s", err)
	}

	<-msgChan2

	conn1.Close()

	err = <-errChan1
	if err != nil {
		t.Fatalf("got unexpected err: %s", err)
	}
	err = <-errChan2
	if err != nil {
		t.Fatalf("got unexpected err: %s", err)
	}
}
