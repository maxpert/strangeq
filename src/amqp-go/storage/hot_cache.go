package storage

import (
	"container/list"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

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

// hotCachePutAttempts bounds the admission retry loop.
//
// An admission that cannot claim global room frees space GLOBALLY (see
// freeGlobalSpace) and tries again. Another queue can claim the freed room
// in between, so the loop is bounded: an admission that loses the race too
// often FAILS, and a failed admission is a legitimate outcome -- the message
// stays disk-only, which is the documented "oversized durable publish
// succeeds disk-only" path.
const hotCachePutAttempts = 8

// hotCacheFreeRounds bounds the number of victims the global eviction sweep
// visits before it gives up. Each round evicts from ONE queue (the one whose
// oldest backed entry is globally the oldest), so the bound only matters for
// a cache with a very large number of queues.
const hotCacheFreeRounds = 64

// hotCache is a byte-bounded collection of PER-QUEUE caches.
//
// ONE LOCK PER QUEUE. The cache used to have ONE mutex for the whole
// collection: every queue's publish, delivery and acknowledgement serialised
// on it, so the aggregate throughput FELL as the queue count grew (measured
// on the 1 KiB multi-queue shape: 124,273 msg/s at one queue, 87,003 msg/s at
// four, against 191,109 msg/s for the pre-segstore build that used a
// lock-free per-queue ring). Each queue now owns its lock, its LRU list, its
// entries and its byte accounting, so two queues never serialise on each
// other.
//
// TWO budgets, TWO victim pools. The per-queue budget is satisfied out of
// THAT queue's backed entries and the global budget out of the queues'
// entries as a whole; they are never satisfied out of the same pool, so a
// queue's admission can never cost an unrelated queue its ready messages.
//
// THE GLOBAL BUDGET IS STILL ENFORCED. It is the sum of every queue's
// resident bytes, held in an ATOMIC counter and claimed with a
// compare-and-swap, so the total resident bytes never exceed the budget, not
// even while several queues fill up at the same time.
//
// THE LOCK ORDER IS: a queue's lock, and then (never the other way round)
// the mirror lock. The global eviction sweep takes ONE queue's lock at a
// time and never holds two, so there is no lock cycle. The admitting queue
// RELEASES its own lock before it goes and frees space elsewhere.
type hotCache struct {
	// globalUsed is the sum of every queue's resident bytes: the GLOBAL
	// budget's accounting. It is accessed with the atomic operations below
	// (atomic.AddInt64 / atomic.CompareAndSwapInt64 / atomic.LoadInt64),
	// and it is the FIRST field of the struct so that it is 64-BIT ALIGNED,
	// which those operations require.
	globalUsed int64

	queueBytes  int64
	globalBytes int64

	// seq hands out the recency stamps that order the queues against EACH
	// OTHER. Every access to an entry stamps it with the next value, so the
	// stamps are totally ordered and comparable across queues. The
	// cross-queue eviction picks the queue whose oldest backed entry has
	// the SMALLEST stamp, which is exactly the globally least recently used
	// backed entry the single global LRU used to select.
	seq atomic.Uint64

	// queues maps a queue name to that queue's state.
	//
	// It is a sync.Map because EVERY operation looks the queue up and the
	// set of queues is stable: sync.Map's read path takes no lock at all,
	// so the lookup never serialises two queues against each other. (A
	// sync.RWMutex would put a shared atomic on the read path -- exactly
	// the contention shape the WAL index shards were split to remove.)
	queues sync.Map // string -> *hotCacheQueue

	// queueUsed mirrors every queue's resident byte count.
	//
	// The AUTHORITATIVE per-queue accounting lives in the queue's own state
	// (hotCacheQueue.used, under the queue's own lock); this map is the
	// observable projection of it, maintained under its own lock so that
	// the mirror can never be read half-updated. It is written on every
	// admission and every removal, so it never drifts from the
	// authoritative value.
	queueUsedMu sync.Mutex
	queueUsed   map[string]int64
}

// hotCacheQueue is ONE QUEUE's slice of the cache: its own lock, its own
// LRU list, its own entries, its own reverse index and its own byte
// accounting.
//
// EVERYTHING IN HERE IS TOUCHED ONLY UNDER mu. The cross-queue operation
// (the global eviction) takes ONE queue's lock at a time and never holds
// two, so there is no lock cycle.
type hotCacheQueue struct {
	cache *hotCache
	name  string

	mu sync.Mutex
	// entries maps the tag an entry was ADMITTED under to the entry.
	entries map[uint64]*hotCacheEntry
	// lru is this queue's LRU list: the front is the most recently used
	// entry, the back the least recently used one.
	lru *list.List
	// byMessage is the REVERSE INDEX from an admitted message pointer to
	// the entry that admitted it.
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
	// It is a CACHE over the entries map, not a second source of truth: the
	// entry itself holds the message pointer, and every removal goes
	// through removeLocked, which deletes the reverse-index entry with it.
	//
	// It is PER QUEUE, because a queue's ready set is its own: the same
	// message pointer admitted in two queues is two entries.
	byMessage map[*protocol.Message]*hotCacheEntry
	// used is this queue's resident byte count. It is the AUTHORITATIVE
	// per-queue accounting and it is guarded by mu.
	used int64

	// oldestBacked is the recency stamp of this queue's OLDEST BACKED
	// entry, or 0 when the queue holds none.
	//
	// It is ATOMIC so the cross-queue eviction can rank the queues
	// WITHOUT taking their locks: the queue whose oldest backed entry has
	// the smallest stamp holds the globally least recently used backed
	// entry, which is what the single global LRU used to select.
	oldestBacked atomic.Int64
}

// hotCacheEntry is one admitted message.
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
	// lastUsed is the recency stamp of the entry's last access. It orders
	// the queues against each other for the cross-queue eviction.
	lastUsed uint64
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
		queueUsed:   make(map[string]int64),
	}
}

// queueState returns the per-queue state for the queue, creating it if it
// does not exist yet.
//
// THE STATE IS NEVER REMOVED FROM THE MAP, not even when the queue empties.
// A goroutine that has already loaded the state would otherwise admit into
// an ORPHANED state: its bytes would be counted against the global budget
// while its entries were invisible to Get, Count and Range. The state is a
// few hundred bytes per queue name, so keeping it is the cheaper trade.
func (c *hotCache) queueState(queue string) *hotCacheQueue {
	if v, ok := c.queues.Load(queue); ok {
		return v.(*hotCacheQueue)
	}
	q := &hotCacheQueue{
		cache:     c,
		name:      queue,
		entries:   make(map[uint64]*hotCacheEntry),
		lru:       list.New(),
		byMessage: make(map[*protocol.Message]*hotCacheEntry),
	}
	actual, _ := c.queues.LoadOrStore(queue, q)
	return actual.(*hotCacheQueue)
}

// queueStateIfPresent returns the per-queue state for the queue, or nil when
// the queue has never been admitted into.
func (c *hotCache) queueStateIfPresent(queue string) *hotCacheQueue {
	if v, ok := c.queues.Load(queue); ok {
		return v.(*hotCacheQueue)
	}
	return nil
}

// Put admits m if both byte budgets can be satisfied. Existing tags retain
// their original message and charge; a durable completion can only upgrade
// their backed state.
//
// The per-queue budget is satisfied from that queue's own backed entries and
// the global budget from the queues' entries as a whole; see
// evictOldestBackedLocked and freeGlobalSpace.
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

	// THE CHARGE IS COMPUTED OUTSIDE THE LOCK: it walks the message's
	// headers, and it does not depend on the cache's state.
	charge := cacheCharge(queue, m)

	q := c.queueState(queue)
	for attempt := 0; attempt < hotCachePutAttempts; attempt++ {
		q.mu.Lock()
		hotCacheQueueLockFire(q.name)

		// THE MESSAGE-IDENTITY PROBE COMES FIRST. See the doc comment above.
		if entry := q.byMessage[m]; entry != nil {
			entry.backed = entry.backed || backed
			c.touchLocked(q, entry)
			q.mu.Unlock()
			return true
		}
		if entry, ok := q.entries[m.DeliveryTag]; ok {
			entry.backed = entry.backed || backed
			c.touchLocked(q, entry)
			q.mu.Unlock()
			return true
		}

		// THE SIZE CHECK COMES AFTER THE MEMBERSHIP PROBES. An entry that is
		// ALREADY admitted is upgraded, never re-charged: the replacement
		// message's size is irrelevant to it, and a message larger than the
		// budget must not be able to fail the upgrade of an entry that is
		// already resident.
		if charge > c.queueBytes || charge > c.globalBytes {
			q.mu.Unlock()
			return false
		}

		// The two budgets are SEPARATE resources and are satisfied separately.
		//
		// The per-queue budget is satisfied out of THAT QUEUE's backed
		// entries only: evicting an unrelated queue's ready message to make
		// room for this queue's admission is a cross-queue starvation bug
		// (the evicted queue's FIFO consumer then has to take a cold read
		// for its next target).
		for q.used+charge > c.queueBytes {
			if !q.evictOldestBackedLocked() {
				c.noteQueueUsed(queue, q.used)
				q.mu.Unlock()
				return false
			}
		}

		// The global budget is satisfied out of the queues' entries AS A
		// WHOLE: that is the only place where a cross-queue eviction is
		// legitimate (the cache as a whole is over its budget). The claim
		// is a compare-and-swap, so the total resident bytes never exceed
		// the budget, not even when several queues fill up together.
		if !c.reserveGlobal(charge) {
			// The admitting queue RELEASES its own lock before it goes and
			// frees space elsewhere, so the sweep never holds two queue
			// locks at once.
			//
			// The mirror is published BEFORE the lock is released, so it is
			// exact on every exit path of the admission.
			c.noteQueueUsed(queue, q.used)
			q.mu.Unlock()
			if !c.freeGlobalSpace(q, charge) {
				return false
			}
			continue
		}

		entry := &hotCacheEntry{
			queue:   queue,
			tag:     m.DeliveryTag,
			message: m,
			charge:  charge,
			backed:  backed,
		}
		entry.lastUsed = c.seq.Add(1)
		entry.lru = q.lru.PushFront(entry)
		q.entries[entry.tag] = entry
		q.byMessage[m] = entry
		q.used += charge
		if backed && q.oldestBacked.Load() == 0 {
			// The queue held no backed entry before this one, so this entry
			// is its oldest backed entry.
			q.oldestBacked.Store(int64(entry.lastUsed))
		}
		c.noteQueueUsed(queue, q.used)
		q.mu.Unlock()
		return true
	}
	return false
}

func (c *hotCache) Get(queue string, tag uint64) (m *protocol.Message, backed, ok bool) {
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return nil, false, false
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	entry, ok := q.entries[tag]
	if !ok {
		return nil, false, false
	}
	c.touchLocked(q, entry)
	return entry.message, entry.backed, true
}

// Delete removes an entry while holding the queue's lock, making exactly one
// concurrent caller the winner for a present queue/tag pair.
func (c *hotCache) Delete(queue string, tag uint64) bool {
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return false
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	entry, ok := q.entries[tag]
	if !ok {
		return false
	}
	c.removeLocked(q, entry)
	c.noteQueueUsed(queue, q.used)
	return true
}

func (c *hotCache) Range(queue string, minTag, maxTag uint64) []*protocol.Message {
	if minTag > maxTag {
		return nil
	}

	q := c.queueStateIfPresent(queue)
	if q == nil {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	tagged := make([]hotCacheRangeEntry, 0, len(q.entries))
	for tag, entry := range q.entries {
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

	q := c.queueStateIfPresent(queue)
	if q == nil {
		return 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	removed := 0
	for tag, entry := range q.entries {
		if tag < minTag || tag > maxTag {
			continue
		}
		c.removeLocked(q, entry)
		removed++
	}
	if removed > 0 {
		c.noteQueueUsed(queue, q.used)
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

	q := c.queueStateIfPresent(queue)
	if q == nil {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	tags := make([]uint64, 0, len(q.entries))
	for tag := range q.entries {
		if tag < minTag || tag > maxTag {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags
}

func (c *hotCache) Count(queue string) int {
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)
	return len(q.entries)
}

// reserveGlobal claims charge bytes of the global budget.
//
// THE CLAIM IS ATOMIC. Two queues that fill up at the same time can both
// see room, so the room is claimed with a compare-and-swap: the sum of every
// queue's resident bytes can never exceed the global budget.
func (c *hotCache) reserveGlobal(charge int64) bool {
	for {
		used := atomic.LoadInt64(&c.globalUsed)
		if used+charge > c.globalBytes {
			return false
		}
		if atomic.CompareAndSwapInt64(&c.globalUsed, used, used+charge) {
			return true
		}
	}
}

// globalRoom reports whether the global budget has room for charge bytes.
func (c *hotCache) globalRoom(charge int64) bool {
	return atomic.LoadInt64(&c.globalUsed)+charge <= c.globalBytes
}

// freeGlobalSpace evicts backed entries until the global budget has room for
// charge bytes again.
//
// THE ADMITTING QUEUE'S LOCK IS NOT HELD HERE (the caller released it), and
// the sweep takes ONE queue's lock at a time: it never holds two queue locks
// at once, so there is no lock cycle.
//
// The victim is the queue whose OLDEST BACKED entry is globally the oldest --
// the same entry the single global LRU used to select -- so the cross-queue
// eviction still evicts the globally least recently used message.
func (c *hotCache) freeGlobalSpace(admitting *hotCacheQueue, charge int64) bool {
	for round := 0; round < hotCacheFreeRounds; round++ {
		if c.globalRoom(charge) {
			return true
		}
		victim := c.oldestBackedQueue(admitting)
		if victim == nil {
			break
		}
		victim.mu.Lock()
		hotCacheQueueLockFire(victim.name)
		for !c.globalRoom(charge) {
			if !victim.evictOldestBackedLocked() {
				break
			}
		}
		c.noteQueueUsed(victim.name, victim.used)
		victim.mu.Unlock()
	}

	// LAST RESORT: the admitting queue's own entries.
	//
	// The sweep above deliberately spares the admitting queue, so a cache
	// whose WHOLE global budget is held by that one queue could never free
	// anything and every further admission would fail -- the cache would
	// freeze at its budget with every later message disk-only. The global
	// eviction may evict from ANY queue (that is what makes it global), so
	// the admitting queue is evicted from too, but only after every other
	// queue has been tried.
	if !c.globalRoom(charge) {
		admitting.mu.Lock()
		hotCacheQueueLockFire(admitting.name)
		for !c.globalRoom(charge) {
			if !admitting.evictOldestBackedLocked() {
				break
			}
		}
		c.noteQueueUsed(admitting.name, admitting.used)
		admitting.mu.Unlock()
	}
	return c.globalRoom(charge)
}

// oldestBackedQueue returns the queue (other than the excluded one) whose
// oldest backed entry is globally the oldest, or nil when no other queue
// holds a backed entry.
//
// It reads the per-queue stamps WITHOUT taking any queue's lock: the stamps
// are atomic and a stale stamp can only name a queue whose oldest backed
// entry has just become newer, which costs at most one extra eviction round.
func (c *hotCache) oldestBackedQueue(exclude *hotCacheQueue) *hotCacheQueue {
	var best *hotCacheQueue
	var bestStamp uint64
	c.queues.Range(func(_, value any) bool {
		q := value.(*hotCacheQueue)
		if q == exclude {
			return true
		}
		stamp := uint64(q.oldestBacked.Load())
		if stamp == 0 {
			return true
		}
		if best == nil || stamp < bestStamp {
			best = q
			bestStamp = stamp
		}
		return true
	})
	return best
}

// evictOldestBackedLocked removes the queue's least recently used BACKED
// entry. Pinned transient entries are skipped and remain resident.
//
// The victim's recency is this queue's own LRU order, so satisfying a
// per-queue budget never costs an unrelated queue its ready messages.
func (q *hotCacheQueue) evictOldestBackedLocked() bool {
	for element := q.lru.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*hotCacheEntry)
		if !entry.backed {
			continue
		}
		q.cache.removeLocked(q, entry)
		return true
	}
	return false
}

// removeLocked removes an entry from its queue and from the global
// accounting. The queue's lock must be held.
func (c *hotCache) removeLocked(q *hotCacheQueue, entry *hotCacheEntry) {
	wasOldest := entry.lastUsed == uint64(q.oldestBacked.Load())
	// Delete by the OWNED tag: the message handed out to callers is live and
	// its DeliveryTag field may have been mutated since admission, which
	// would otherwise delete the wrong key and orphan the entry.
	delete(q.entries, entry.tag)
	// The reverse index goes with the entry: a stale entry there would make a
	// later re-Put of the removed message upgrade an entry that is no longer
	// in the queue map.
	delete(q.byMessage, entry.message)
	q.used -= entry.charge
	atomic.AddInt64(&c.globalUsed, -entry.charge)
	q.lru.Remove(entry.lru)
	if wasOldest {
		q.refreshOldestBackedLocked()
	}
}

// touchLocked marks the entry as the most recently used entry of its queue.
//
// The recency stamp is what orders the queues against each other for the
// cross-queue eviction, so it is refreshed on every access.
func (c *hotCache) touchLocked(q *hotCacheQueue, entry *hotCacheEntry) {
	wasOldest := entry.lastUsed == uint64(q.oldestBacked.Load())
	entry.lastUsed = c.seq.Add(1)
	q.lru.MoveToFront(entry.lru)
	if wasOldest {
		q.refreshOldestBackedLocked()
	}
}

// refreshOldestBackedLocked recomputes the queue's oldest backed stamp by
// walking the LRU from the tail.
//
// It is called ONLY when the oldest backed entry may have changed, because
// the walk skips the PINNED transient entries and would otherwise cost one
// transient skip per access.
func (q *hotCacheQueue) refreshOldestBackedLocked() {
	var stamp uint64
	for element := q.lru.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*hotCacheEntry)
		if entry.backed {
			stamp = entry.lastUsed
			break
		}
	}
	q.oldestBacked.Store(int64(stamp))
}

// noteQueueUsed publishes the queue's resident byte count into the mirror
// map.
//
// The mirror is the observable projection of the queue's own accounting, so
// it is written from under the queue's lock (the queue's lock is always
// taken BEFORE the mirror lock) and it is written on every change, so it
// never drifts from the authoritative value.
func (c *hotCache) noteQueueUsed(queue string, used int64) {
	c.queueUsedMu.Lock()
	if used == 0 {
		delete(c.queueUsed, queue)
	} else {
		c.queueUsed[queue] = used
	}
	c.queueUsedMu.Unlock()
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
