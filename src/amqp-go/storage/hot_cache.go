package storage

import (
	"container/list"
	"reflect"
	"sort"
	"sync"

	"github.com/maxpert/amqp-go/protocol"
)

// THE EFFECTIVE HOT-CACHE BUDGETS.
//
// These are the budgets the storage layer SUBSTITUTES when the engine
// configuration leaves them unset. They are exported because the CONFIG
// VALIDATION has to compare the EFFECTIVE budgets against each other: a
// configuration that names a global budget smaller than the per-queue budget
// the storage layer will actually apply makes the queue-local eviction
// unreachable, and the global eviction the only one that ever fires.
const (
	// DefaultHotCacheQueueBytes is the per-queue hot-cache byte budget used
	// when the engine configuration leaves it unset.
	//
	// The per-queue budget is 64 MiB because the hot cache REPLACES the
	// 64K-message ring, whose 80% spill threshold was 52,428 ENTRIES -- an
	// entry count, with no byte bound at all, so its capacity in bytes grew
	// with the body size. The byte budget holds the ring's 52,428 entries
	// only for small bodies, and that is the memory the 64 KiB rows win on.
	DefaultHotCacheQueueBytes int64 = 64 * 1024 * 1024 // 64 MiB per queue

	// DefaultHotCacheGlobalBytes is the global hot-cache byte budget used
	// when the engine configuration leaves it unset.
	DefaultHotCacheGlobalBytes int64 = 256 * 1024 * 1024 // 256 MiB global
)

// hotCache is a byte-bounded collection of per-queue message maps with one
// global LRU. The front of lru is the most recently used entry.
//
// TWO budgets, TWO victim pools. The per-queue budget is satisfied out of
// THAT queue's backed entries and the global budget out of the global LRU;
// they are never satisfied out of the same pool, so a queue's admission can
// never cost an unrelated queue its ready messages.
type hotCache struct {
	mu sync.Mutex

	queueBytes  int64
	globalBytes int64
	queues      map[string]map[uint64]*hotCacheEntry
	queueUsed   map[string]int64
	globalUsed  int64
	lru         *list.List

	// byMessage is the REVERSE INDEX from an admitted message pointer to the
	// entry that admitted it.
	//
	// IT EXISTS BECAUSE THE MESSAGE'S DELIVERY TAG IS MUTABLE. The cache
	// hands out the LIVE message pointer, so a caller can change
	// message.DeliveryTag after admission, and the entry's map key is the
	// tag the entry was ADMITTED under. A membership probe that used
	// m.DeliveryTag would therefore miss the entry the message already
	// occupies and admit a SECOND entry for the same message: the queue's
	// ready set would be double-counted and the byte budget charged twice.
	//
	// The reverse index makes the membership test INDEPENDENT of the mutable
	// field: a re-Put of the same pointer UPGRADES the entry it already
	// occupies, wherever the message's tag has wandered to.
	//
	// It is a CACHE over `queues`, not a second source of truth: the entry
	// itself holds the message pointer, and every removal goes through
	// removeLocked, which deletes the reverse-index entry with it.
	byMessage map[hotCacheMessageKey]*hotCacheEntry
}

// hotCacheMessageKey identifies the entry that admitted one message pointer
// in one queue. The queue is part of the key because a queue's ready set is
// its own: the same message pointer admitted in two queues is two entries.
type hotCacheMessageKey struct {
	queue   string
	message *protocol.Message
}

type hotCacheEntry struct {
	queue string
	// tag is the entry's OWNED identity: the delivery tag the entry was
	// admitted under. The cache hands out the live message pointer, so a
	// caller can mutate message.DeliveryTag after admission; every identity
	// operation (map key, deletion, range ordering) MUST use this field and
	// never the mutable message field.
	tag     uint64
	message *protocol.Message
	charge  int64
	backed  bool
	lru     *list.Element
}

// hotCacheRangeEntry pairs a message with the tag it was admitted under so
// Range can order by the owned identity instead of the mutable message field.
type hotCacheRangeEntry struct {
	tag     uint64
	message *protocol.Message
}

func newHotCache(queueBytes, globalBytes int64) *hotCache {
	return &hotCache{
		queueBytes:  queueBytes,
		globalBytes: globalBytes,
		queues:      make(map[string]map[uint64]*hotCacheEntry),
		queueUsed:   make(map[string]int64),
		lru:         list.New(),
		byMessage:   make(map[hotCacheMessageKey]*hotCacheEntry),
	}
}

// Put admits m if both byte budgets can be satisfied. Existing tags retain
// their original message and charge; a durable completion can only upgrade
// their backed state.
//
// The per-queue budget is satisfied from that queue's own backed entries and
// the global budget from the global LRU; see evictBackedLRULocked.
//
// THE MEMBERSHIP TEST IS INDEPENDENT OF THE MESSAGE'S MUTABLE FIELD. It
// probes, in this order:
//
//  1. the REVERSE INDEX (byMessage): the entry that admitted THIS message
//     pointer, whatever its delivery tag has become since. A re-Put of an
//     admitted message UPGRADES that entry -- it does not create a second
//     one, so the queue's ready set is not double-counted and the byte
//     budget is not charged twice;
//  2. the QUEUE MAP, by the tag the message CARRIES: a different pointer
//     carrying a tag the queue already holds. That entry keeps its own
//     message and its own charge, and only its backed state is upgraded.
//
// The order matters: the first probe is the one that is immune to the
// mutation, so a mutated re-Put never reaches the tag probe.
func (c *hotCache) Put(queue string, m *protocol.Message, backed bool) bool {
	if m == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// THE MESSAGE-IDENTITY PROBE COMES FIRST. See the doc comment above.
	if entry := c.byMessage[hotCacheMessageKey{queue: queue, message: m}]; entry != nil {
		entry.backed = entry.backed || backed
		c.lru.MoveToFront(entry.lru)
		return true
	}

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

	// The two budgets are SEPARATE resources and are satisfied separately.
	//
	// The per-queue budget is satisfied out of THAT QUEUE's backed entries
	// only: evicting an unrelated queue's ready message to make room for this
	// queue's admission is a cross-queue starvation bug (the evicted queue's
	// FIFO consumer then has to take a cold read for its next target).
	//
	// The global budget is satisfied out of the global LRU, which is the only
	// place where a cross-queue eviction is legitimate: the cache as a whole
	// is over its budget.
	for c.queueUsed[queue]+charge > c.queueBytes {
		if !c.evictBackedLRULocked(queue) {
			return false
		}
	}
	for c.globalUsed+charge > c.globalBytes {
		if !c.evictBackedLRULocked("") {
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
		tag:     m.DeliveryTag,
		message: m,
		charge:  charge,
		backed:  backed,
	}
	entry.lru = c.lru.PushFront(entry)
	entries[entry.tag] = entry
	c.byMessage[hotCacheMessageKey{queue: queue, message: m}] = entry
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
	tagged := make([]hotCacheRangeEntry, 0, len(entries))
	for tag, entry := range entries {
		if tag >= minTag && tag <= maxTag {
			tagged = append(tagged, hotCacheRangeEntry{tag: tag, message: entry.message})
		}
	}
	// Order by the MAP KEY (the tag the entry was admitted under), never by
	// the message's mutable DeliveryTag field: a caller that mutated the
	// handed-out message must not be able to reorder the queue's ready set.
	sort.Slice(tagged, func(i, j int) bool {
		return tagged[i].tag < tagged[j].tag
	})
	result := make([]*protocol.Message, 0, len(tagged))
	for _, entry := range tagged {
		result = append(result, entry.message)
	}
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

// TagsInRange returns the tags this cache holds for the queue within
// [minTag, maxTag], sorted ascending.
//
// IT REPORTS THE ENTRY'S OWNED TAG, never the DeliveryTag field of the
// message the cache hands out: a caller may mutate that field after
// admission, and a purge that settled entries by it would delete the wrong
// key (or nothing) and would report the wrong tag set.
func (c *hotCache) TagsInRange(queue string, minTag, maxTag uint64) []uint64 {
	if minTag > maxTag {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	tags := make([]uint64, 0, len(c.queues[queue]))
	for tag := range c.queues[queue] {
		if tag < minTag || tag > maxTag {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags
}

func (c *hotCache) Count(queue string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queues[queue])
}

// evictBackedLRULocked removes the least recently used backed entry. Pinned
// transient entries are skipped and remain resident.
//
// A non-empty queue restricts the victims to THAT queue's entries: satisfying
// a per-queue budget must never cost an unrelated queue its ready messages.
// An empty queue selects the global LRU, which is the only legitimate source
// of a cross-queue eviction (the cache as a whole is over its global budget).
func (c *hotCache) evictBackedLRULocked(queue string) bool {
	for element := c.lru.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*hotCacheEntry)
		if queue != "" && entry.queue != queue {
			continue
		}
		if entry.backed {
			c.removeLocked(entry)
			return true
		}
	}
	return false
}

func (c *hotCache) removeLocked(entry *hotCacheEntry) {
	entries := c.queues[entry.queue]
	// Delete by the OWNED tag: the message handed out to callers is live and
	// its DeliveryTag field may have been mutated since admission, which
	// would otherwise delete the wrong key and orphan the entry.
	delete(entries, entry.tag)
	// The reverse index goes with the entry: a stale entry there would make a
	// later re-Put of the removed message upgrade an entry that is no longer
	// in the queue map.
	delete(c.byMessage, hotCacheMessageKey{queue: entry.queue, message: entry.message})
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
