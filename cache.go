package sudp

import (
	"time"
)

// cache can be used to keep track of messages that have already been
// received and processed by the udp connection.
type cache struct {
	values map[string]map[uint32]bool
}

func newCache() *cache {
	return &cache{make(map[string]map[uint32]bool)}
}

// set adds a udp address + message seq to the cache as a composite key.
//
// After a certain duration, it is safe to assume that the sender of the message
// no longer is keeping track of the message and duplicates should no longer be received.
// Therefore, we expire the composite key in the cache to save memory.
func (c *cache) set(addr string, seq uint32, ttl time.Duration) {
	subMap := c.values[addr]
	if subMap == nil {
		subMap = make(map[uint32]bool)
		c.values[addr] = subMap
	}
	subMap[seq] = true
	go func() {
		timer := time.NewTimer(ttl)
		<-timer.C
		delete(subMap, seq)
	}()
}

// has checks if a udp address + message seq combonation is in the message cache.
func (c *cache) has(addr string, seq uint32) bool {
	subMap := c.values[addr]
	if subMap == nil {
		return false
	}
	return subMap[seq]
}
