package sudp

import (
	"net"
	"time"
)

// messageCache can be used to keep track of messages that have already been
// received and processed by the udp connection.
type messageCache struct {
	values map[string]map[uint32]bool
}

func newMessageCache() *messageCache {
	return &messageCache{make(map[string]map[uint32]bool)}
}

// set adds a udp address + message seq to the cache as a composite key.
//
// After a certain duration, it is safe to assume that the sender of the message
// no longer is keeping track of the message and duplicates should no longer be received.
// Therefore, we expire the composite key in the cache to save memory.
func (c *messageCache) set(addr *net.UDPAddr, seq uint32, dur time.Duration) {
	strAddr := addr.String()
	subMap := c.values[strAddr]
	if subMap == nil {
		subMap = make(map[uint32]bool)
		c.values[strAddr] = subMap
	}
	subMap[seq] = true
	go func() {
		timer := time.NewTimer(dur)
		select {
		case <-timer.C:
			delete(subMap, seq)
		}
	}()
}

// has checks if a udp address + message seq combonation is in the message cache.
func (c *messageCache) has(addr *net.UDPAddr, seq uint32) bool {
	subMap := c.values[addr.String()]
	if subMap == nil {
		return false
	}
	return subMap[seq]
}
