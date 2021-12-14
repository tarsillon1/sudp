package sudp

import "errors"

var (
	ErrConn        = errors.New("failed to connect to udp address")
	ErrWrite       = errors.New("failed to write to udp connection")
	ErrRead        = errors.New("failed to read from udp connection")
	ErrMarshal     = errors.New("failed to marshal")
	ErrUnmarshal   = errors.New("failed to unmarshal packet bytes")
	ErrUnknownType = errors.New("received unknown packet type")
	ErrPongTimeout = errors.New("timed out while waiting for pong")
	ErrAckTimeout  = errors.New("message was not acknowledged by receiver within the specified timeout")
)
