package sudp

import (
	"time"
)

/* Default Values */

const (
	defaultMaxPacketSize    = 8192
	defaultMaxBufferSize    = 1024
	defaultPongTimeout      = time.Second * 2
	defaultAckRetryInterval = time.Millisecond * 500
	defaultAckTimeout       = time.Second * 5
)

/* Pre-Computed Values */

var (
	timeZeroVal = time.Time{}
	maxSeq      = uint32(4294967295)
	errChanSize = 10
	msgChanSize = 10
)
