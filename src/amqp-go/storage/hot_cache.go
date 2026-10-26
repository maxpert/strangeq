package storage

import (
	"container/list"
	"reflect"
	"sort"
	"sync"

	"github.com/maxpert/amqp-go/protocol"
)

// hotCache is a byte-bounded collection of per-queue message maps with one
// global LRU. The front of lru is the most recently used entry.
type hotCache struct {
	mu sync.Mutex

	queueBytes  int64
	globalBytes int64
	queues      map[string]map[uint64]*hotCacheEntry
	queueUsed   map[string]int64
	globalUsed  int64
	lru         *list.List
}

type hotCacheEntry struct {
	queue   string
	message *protocol.Message
	charge  int64
	backed  bool
	lru     *list.Element
}

func newHotCache(queueBytes, globalBytes int64) *hotCache {
	return &hotCache{
		queueBytes:  queueBytes,
		globalBytes: globalBytes,
		queues:      make(map[string]map[uint64]*hotCacheEntry),
		queueUsed:   make(map[string]int64),
		lru:         list.New(),
	}
}

// Put admits m if both byte budgets can be satisfied. Existing tags retain
// their original message and charge; a durable completion can only upgrade
// their backed state.
func (c *hotCache) Put(queue string, m *protocol.Message, backed bool) bool {
	if m == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entries := c.queues[queue]; entries != nil {
		if entry, ok := entries[m.DeliveryTag]; ok {
			entry.backed = entry.backed || backed
			c.lru.MoveToFront(entry.lru)
			return true
		}
	}

	charge := cacheCharge(queue, m)
	if charge > c.queueBytes || charge > c.globalBytes {
		return false
	}

	for c.queueUsed[queue]+charge > c.queueBytes || c.globalUsed+charge > c.globalBytes {
		if !c.evictBackedLRULocked() {
			return false
		}
	}

	entries := c.queues[queue]
	if entries == nil {
		entries = make(map[uint64]*hotCacheEntry)
		c.queues[queue] = entries
	}
	entry := &hotCacheEntry{
		queue:   queue,
		message: m,
		charge:  charge,
		backed:  backed,
	}
	entry.lru = c.lru.PushFront(entry)
	entries[m.DeliveryTag] = entry
	c.queueUsed[queue] += charge
	c.globalUsed += charge
	return true
}

func (c *hotCache) Get(queue string, tag uint64) (m *protocol.Message, backed, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.queues[queue][tag]
	if !ok {
		return nil, false, false
	}
	c.lru.MoveToFront(entry.lru)
	return entry.message, entry.backed, true
}

// Delete removes an entry while holding the cache mutex, making exactly one
// concurrent caller the winner for a present queue/tag pair.
func (c *hotCache) Delete(queue string, tag uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.queues[queue][tag]
	if !ok {
		return false
	}
	c.removeLocked(entry)
	return true
}

func (c *hotCache) Range(queue string, minTag, maxTag uint64) []*protocol.Message {
	if minTag > maxTag {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entries := c.queues[queue]
	result := make([]*protocol.Message, 0, len(entries))
	for tag, entry := range entries {
		if tag >= minTag && tag <= maxTag {
			result = append(result, entry.message)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DeliveryTag < result[j].DeliveryTag
	})
	return result
}

func (c *hotCache) DeleteRange(queue string, minTag, maxTag uint64) int {
	if minTag > maxTag {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for tag, entry := range c.queues[queue] {
		if tag < minTag || tag > maxTag {
			continue
		}
		c.removeLocked(entry)
		removed++
	}
	return removed
}

func (c *hotCache) Count(queue string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queues[queue])
}

// evictBackedLRULocked removes the least recently used backed entry. Pinned
// transient entries are skipped and remain resident.
func (c *hotCache) evictBackedLRULocked() bool {
	for element := c.lru.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*hotCacheEntry)
		if entry.backed {
			c.removeLocked(entry)
			return true
		}
	}
	return false
}

func (c *hotCache) removeLocked(entry *hotCacheEntry) {
	entries := c.queues[entry.queue]
	delete(entries, entry.message.DeliveryTag)
	c.queueUsed[entry.queue] -= entry.charge
	c.globalUsed -= entry.charge
	c.lru.Remove(entry.lru)
	if len(entries) == 0 {
		delete(c.queues, entry.queue)
		delete(c.queueUsed, entry.queue)
	}
}

func cacheCharge(queue string, m *protocol.Message) int64 {
	charge := int64(512 + len(queue))
	if m == nil {
		return charge
	}
	charge += int64(len(m.Body) + len(m.BodyRef))
	charge += int64(len(m.Exchange) + len(m.RoutingKey) + len(m.ContentType) +
		len(m.ContentEncoding) + len(m.CorrelationID) + len(m.ReplyTo) +
		len(m.Expiration) + len(m.MessageID) + len(m.Type) + len(m.UserID) +
		len(m.AppID) + len(m.ClusterID))
	for key, value := range m.Headers {
		charge += int64(32 + len(key))
		charge += cacheHeaderValueCharge(reflect.ValueOf(value))
	}
	return charge
}

// cacheHeaderValueCharge returns the charge in addition to the 32 bytes
// already charged by the containing map or slice element. Containers recursively
// charge each of their own elements; strings and byte slices also charge payload.
func cacheHeaderValueCharge(value reflect.Value) int64 {
	if !value.IsValid() {
		return 0
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			return 0
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.String:
		return int64(value.Len())
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return int64(value.Len())
		}
		var charge int64
		for i := 0; i < value.Len(); i++ {
			charge += 32 + cacheHeaderValueCharge(value.Index(i))
		}
		return charge
	case reflect.Map:
		var charge int64
		iterator := value.MapRange()
		for iterator.Next() {
			charge += 32
			key := iterator.Key()
			if key.Kind() == reflect.String {
				charge += int64(key.Len())
			}
			charge += cacheHeaderValueCharge(iterator.Value())
		}
		return charge
	default:
		return 0
	}
}
