package sudp

import (
	"sync"
	"time"
)

type cacheEntry struct {
	ackData []byte
}

// cache can be used to keep track of messages that have already been
// received and processed by the udp connection.
type cache struct {
	mut    *sync.Mutex
	values map[string]map[uint32]*cacheEntry
}

func newCache() *cache {
	return &cache{values: make(map[string]map[uint32]*cacheEntry), mut: &sync.Mutex{}}
}

// create adds a udp address + message seq to the cache as a composite key.
//
// After a certain duration, it is safe to assume that the sender of the message
// no longer is keeping track of the message and duplicates should no longer be received.
// Therefore, we expire the composite key in the cache to save memory.
//
// This function returns true if the composite key is successfully added to the cache.
// It will return false if the composite key is already in the cache.
func (c *cache) create(addr string, seq uint32, ttl time.Duration) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	subMap := c.values[addr]
	if subMap != nil && subMap[seq] != nil {
		return false
	}

	if subMap == nil {
		subMap = make(map[uint32]*cacheEntry)
		c.values[addr] = subMap
	}
	subMap[seq] = &cacheEntry{}
	go func() {
		timer := time.NewTimer(ttl)
		<-timer.C
		delete(subMap, seq)
	}()
	return true
}

// update the cache entry value of an existing udp address + message seq composite key.
func (c *cache) update(addr string, seq uint32, entry *cacheEntry) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	subMap := c.values[addr]
	if subMap == nil || subMap[seq] == nil {
		return false
	}

	subMap[seq] = entry
	return true
}

// get an entry from the cache based on given udp address + message seq composite key.
func (c *cache) get(addr string, seq uint32) *cacheEntry {
	subMap := c.values[addr]
	if subMap == nil {
		return nil
	}
	return subMap[seq]
}
