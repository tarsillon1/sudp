package sudp

import (
	"sync"
	"time"
)

// cache can be used to keep track of messages that have already been
// received and processed by the udp connection.
type cache struct {
	mut    *sync.Mutex
	values map[string]map[uint32]bool
}

func newCache() *cache {
	return &cache{values: make(map[string]map[uint32]bool), mut: &sync.Mutex{}}
}

// set adds a udp address + message seq to the cache as a composite key.
//
// After a certain duration, it is safe to assume that the sender of the message
// no longer is keeping track of the message and duplicates should no longer be received.
// Therefore, we expire the composite key in the cache to save memory.
//
// This function returns true if the composite key is successfully added to the cache.
// It will return false if the composite key is already in the cache.
func (c *cache) set(addr string, seq uint32, ttl time.Duration) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	subMap := c.values[addr]
	if subMap != nil && subMap[seq] {
		return false
	}

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
	return true
}
