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

	// admissionEpoch is this queue's ADMISSION EPOCH: the counter that says
	// "the set of messages this queue's hot cache may admit has not changed
	// since the reader captured it".
	//
	// WHY IT EXISTS. The delivery reader admits what it reads (the
	// write-through admission), and the per-tag shard lock
	// (coldRemovalShards) makes the reader's (acknowledgement re-check +
	// admission) atomic with the acknowledgement's (removal +
	// acknowledgement). The RANGE REMOVAL — the range purge and the queue
	// deletion — settles a BAND of tags in the COLD tiers, and it does NOT
	// take that per-tag lock: it cannot, it settles many tags at once.
	// So a reader that was between its cold read and its admission could
	// admit a tag the range removal had just settled: the message would
	// come back to the hot cache, be counted by the queue's reported message
	// count, and be served from there again.
	//
	// THE EPOCH CLOSES THAT WINDOW. The reader captures the epoch
	// immediately before its cold read; the range removal bumps the epoch
	// under this queue's lock; the admission validates the captured epoch
	// UNDER THE SAME LOCK, so an admission that raced a removal is refused.
	//
	// IT IS BUMPED BY EVERY RANGE REMOVAL (DeleteRange), INCLUDING AN EMPTY
	// RANGE. An empty range is not a no-op: the in-flight reader may be
	// about to admit a tag the just-settled band covers, and the band is
	// settled in the COLD tiers whether or not the hot tier held a copy of
	// it.
	//
	// IT IS GUARDED BY mu, so the check and the admission it guards are
	// atomic with respect to the bump.
	admissionEpoch uint64

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
	// durable reports whether the entry's message may be SERVED.
	//
	// IT IS NOT THE SAME THING AS `backed`. `backed` says "this body is
	// BACKED BY A READABLE COLD COPY IN THE WAL OR THE SEGMENT TIER, so
	// evicting the entry is safe (the body can be read back from the cold
	// tiers)". `durable` says "this message is in a state where handing it
	// to a consumer is allowed".
	//
	// THE COLD COPY IS NOT ALWAYS THE WAL. A segment-only cold read is
	// admitted with backed=true even though its WAL copy may already have
	// been reclaimed: the segment tier is a cold copy too, and the body is
	// readable from it. That is SAFE for the current eviction, readback and
	// acknowledgement behaviour — the entry may be evicted, and the body is
	// read back from whichever cold tier still holds it.
	//
	// A NOT-yet-durable entry is a DURABLE message whose WAL record has been
	// WRITTEN but whose covering fdatasync has NOT returned yet. Such an
	// entry must behave exactly like a message that is not in the WAL yet: it
	// is not served (Get), not counted (Count) and not listed (Range).
	//
	// The write-side admission creates it (the body is in the file, so it is
	// backed, but it is not durable yet); the DURABILITY POINT — the
	// completion, after the covering fdatasync — marks it durable
	// (MarkDurableEntry). A transient message (one that lives only in RAM)
	// is durable by construction: there is nothing to fsync.
	//
	// IT IS AN ATOMIC because the durability point marks it WITHOUT taking
	// the queue's lock: the completion runs on the WAL's committer
	// goroutine, which is the broker's serialisation point for every
	// publish, and a second queue-lock acquisition there would contend with
	// the consumers' delivery and acknowledgement paths on the same queue.
	durable atomic.Bool
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

// captureAdmission captures the queue's ADMISSION EPOCH and its state.
//
// IT IS TAKEN IMMEDIATELY BEFORE A COLD READ, by the delivery reader, so the
// admission that follows the read can prove that nothing settled the queue's
// messages while the read was in flight (see hotCacheQueue.admissionEpoch).
//
// THE STATE IS CREATED IF IT DOES NOT EXIST YET, and the hot cache NEVER
// REMOVES a queue's state, so the returned pointer is the state every later
// operation on this queue name uses: the epoch this reader captures is the
// epoch the range removals bump, and the removals find the state (they look
// it up by name).
func (c *hotCache) captureAdmission(queue string) (uint64, *hotCacheQueue) {
	q := c.queueState(queue)
	q.mu.Lock()
	epoch := q.admissionEpoch
	q.mu.Unlock()
	return epoch, q
}

// bumpAdmissionEpoch advances the queue's admission epoch, invalidating every
// admission that captured the old one.
//
// IT IS USED BY THE RANGE PURGE, BEFORE it settles the cold tiers, so a
// reader that captured the epoch before the purge cannot admit a message the
// purge is about to settle — not even in the window between the cold-tier
// settlement and the hot tier's DeleteRange.
//
// IT IS A NO-OP FOR A QUEUE THAT HAS NO STATE: there is nothing to invalidate
// (a reader that captured an epoch would have created the state).
func (c *hotCache) bumpAdmissionEpoch(queue string) {
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return
	}
	q.mu.Lock()
	q.admissionEpoch++
	q.mu.Unlock()
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
			// Put is the DURABLE admission: the message it admits is either
			// already fsynced in the WAL or a transient message that lives
			// only in RAM. Either way it may be served.
			entry.durable.Store(true)
			c.touchLocked(q, entry)
			q.mu.Unlock()
			return true
		}
		if entry, ok := q.entries[m.DeliveryTag]; ok {
			entry.backed = entry.backed || backed
			entry.durable.Store(true)
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
		entry.durable.Store(true)
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

// PutBestEffort admits m if the byte budgets have room for it WITHOUT
// EVICTING anything.
//
// It is the admission used by the two RAM-holder entry points:
//
//   - the publish path, the moment the WAL write returns (the bytes are in
//     the file, so the body is re-readable and the entry is backed), and
//   - the DELIVERY path, on a cache miss, for the message it just read back.
//
// IT IS NEVER CALLED BY A PROBE OR A PEEK: those reads use GetMessage, which
// does not admit (see the F8 contract in storage/cold_read_count_test.go).
//
// A FAILED admission is a legitimate outcome: the message stays disk-only
// (the documented "oversized durable publish succeeds disk-only" path) and
// NOTHING ELSE CHANGES. In particular the admission never displaces a
// resident message to make room for itself: the cache's budget is spent on
// whatever is actually in use, and the eviction rules (which victim, in which
// order) are the existing ones, driven by the callers that own them.
//
// DURABLE SAYS WHETHER THE ENTRY MAY BE SERVED.
//
//   - The publish path admits with durable=FALSE: the record's bytes are in
//     the file, but the covering fdatasync has NOT returned yet, so the
//     message is not durable and must not be handed to a consumer. The
//     completion (the durability point) marks the entry durable.
//   - The delivery path admits with durable=TRUE: the message it just read
//     came out of the WAL's index, and the index is published only after the
//     covering fdatasync, so a WAL read can only ever find DURABLE messages.
//
// The membership probes and the upgrades are the same as Put's: an already
// admitted message is upgraded (never re-charged), and the identity probe is
// the reverse index, so a mutated DeliveryTag cannot admit a second entry.
func (c *hotCache) PutBestEffort(queue string, m *protocol.Message, backed bool, durable bool) bool {
	entry, ok := c.PutBestEffortEntry(queue, m, backed, durable)
	return ok && entry != nil
}

// PutBestEffortEntry is PutBestEffort plus the ENTRY it admitted.
//
// IT RETURNS THE ENTRY SO THE CALLER CAN MARK IT DURABLE LATER WITHOUT
// TAKING THE QUEUE'S LOCK AGAIN (see MarkDurableEntry). The publish path
// needs exactly that: the write-side admission happens on the WAL's
// committer goroutine (the broker's serialisation point for every publish)
// and the durability point happens in the same goroutine's completion, so a
// second queue-lock acquisition there would contend with the consumers'
// delivery and acknowledgement paths on the same queue.
//
// The returned entry is the entry the message OCCUPIES. It stays valid if
// it is evicted in the meantime (an evicted entry is never reused), so
// marking it durable after an eviction is a harmless no-op.
func (c *hotCache) PutBestEffortEntry(queue string, m *protocol.Message, backed bool, durable bool) (*hotCacheEntry, bool) {
	if m == nil {
		return nil, false
	}

	// THE CHARGE IS COMPUTED OUTSIDE THE LOCK: it walks the message's
	// headers, and it does not depend on the cache's state.
	charge := cacheCharge(queue, m)

	q := c.queueState(queue)
	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)

	entry := c.putBestEffortLocked(q, queue, m, charge, backed, durable)
	if entry == nil {
		return nil, false
	}
	return entry, true
}

// putBestEffortLocked is the body of PutBestEffortEntry, with the queue's
// lock ALREADY HELD.
//
// IT IS FACTORED OUT so the delivery path can VALIDATE its capture (the
// queue's incarnation and the admission epoch) under the SAME lock and then
// admit, with no window in between: a validation that happens outside the
// lock could be invalidated by a range removal before the admission lands.
//
// IT DOES NOT TAKE THE LOCK AND IT MUST NOT: the caller holds it.
func (c *hotCache) putBestEffortLocked(q *hotCacheQueue, queue string, m *protocol.Message, charge int64, backed bool, durable bool) *hotCacheEntry {
	if entry := q.byMessage[m]; entry != nil {
		entry.backed = entry.backed || backed
		if durable {
			entry.durable.Store(true)
		}
		c.touchLocked(q, entry)
		return entry
	}
	if entry, ok := q.entries[m.DeliveryTag]; ok {
		entry.backed = entry.backed || backed
		if durable {
			entry.durable.Store(true)
		}
		c.touchLocked(q, entry)
		return entry
	}

	if charge > c.queueBytes || charge > c.globalBytes {
		return nil
	}
	// NO ROOM, NO ADMISSION: the queue's budget is full and this admission
	// does not evict.
	if q.used+charge > c.queueBytes {
		return nil
	}
	// The global budget is claimed with the same atomic compare-and-swap the
	// evicting admission uses, so the total resident bytes never exceed it.
	if !c.reserveGlobal(charge) {
		return nil
	}

	entry := &hotCacheEntry{
		queue:   queue,
		tag:     m.DeliveryTag,
		message: m,
		charge:  charge,
		backed:  backed,
	}
	if durable {
		entry.durable.Store(true)
	}
	entry.lastUsed = c.seq.Add(1)
	entry.lru = q.lru.PushFront(entry)
	q.entries[entry.tag] = entry
	q.byMessage[m] = entry
	q.used += charge
	if backed && q.oldestBacked.Load() == 0 {
		q.oldestBacked.Store(int64(entry.lastUsed))
	}
	c.noteQueueUsed(queue, q.used)
	return entry
}

// MarkDurableEntry marks the entry DURABLE: the message it holds may now be
// served.
//
// IT TAKES NO LOCK. The entry's durability flag is an ATOMIC, so the
// durability point — the completion, which runs on the WAL's committer
// goroutine — can mark the entry without taking the queue's lock a second
// time. That matters: the committer goroutine is the broker's serialisation
// point for every publish, and a second queue-lock acquisition there would
// contend with the consumers' delivery and acknowledgement paths.
//
// Marking an EVICTED entry is a harmless no-op: an evicted entry is never
// reused, and it is no longer in the queue's map.
func (c *hotCache) MarkDurableEntry(entry *hotCacheEntry) bool {
	if entry == nil {
		return false
	}
	entry.durable.Store(true)
	return true
}

// Get returns the DURABLE entry admitted under tag.
//
// A NOT-yet-durable entry is NOT served: it behaves exactly like a message
// that is not in the WAL yet (its record is written but its covering
// fdatasync has not returned).
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
	if !entry.durable.Load() {
		return nil, false, false
	}
	c.touchLocked(q, entry)
	return entry.message, entry.backed, true
}

// GetResident returns the entry admitted under tag WHETHER OR NOT IT IS
// DURABLE.
//
// It is the removal path's residency probe: a settlement must find (and
// remove) an entry that is resident even when the entry is not yet durable,
// because the removal is what settles the message.
func (c *hotCache) GetResident(queue string, tag uint64) (m *protocol.Message, backed, ok bool) {
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

// Range returns the DURABLE messages admitted in [minTag, maxTag], ordered by
// the tag the entry was admitted under.
//
// A NOT-yet-durable entry is NOT listed: it behaves exactly like a message
// that is not in the WAL yet.
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
		if tag >= minTag && tag <= maxTag && entry.durable.Load() {
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

// DeleteRange removes the entries admitted in [minTag, maxTag] and advances
// the queue's ADMISSION EPOCH.
//
// THE EPOCH IS ADVANCED ON EVERY CALL, INCLUDING AN EMPTY RANGE (a range
// that matched no entry) and a DEGENERATE RANGE (minTag > maxTag). The
// range removal is what settles a BAND of tags in the COLD tiers, and it
// does not take the per-tag shard lock, so it is the range removal — not
// the acknowledgement — that has to invalidate the deliveries racing it.
//
// AN EMPTY RANGE MUST ADVANCE IT TOO: the in-flight reader may be about to
// admit a tag the just-settled band covers, and the band is settled in the
// COLD tiers whether or not the hot tier held a copy of it. A reader that
// captured the old epoch is refused, and the refusal needs no rollback: the
// admission is validated BEFORE it reserves anything and before it mutates
// anything.
//
// THE ORDERING IS ACCEPTABLE AND DOCUMENTED: a reader that holds the queue's
// lock and wins the race against a deletion admits the message, and the
// deletion's subsequent DeleteRange removes that admission. The removal and
// the admission are serialized by this queue's lock, so exactly one of them
// observes the other.
func (c *hotCache) DeleteRange(queue string, minTag, maxTag uint64) int {
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// THE ADMISSION EPOCH IS ADVANCED FIRST, BEFORE ANY ENTRY IS REMOVED, so
	// a reader parked between its capture and its admission is refused even
	// if it is admitted while this removal is running.
	q.admissionEpoch++

	hotCacheQueueLockFire(q.name)

	if minTag > maxTag {
		return 0
	}

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
//
// A NOT-yet-durable entry is NOT reported: it behaves exactly like a message
// that is not in the WAL yet, and the WAL tier does not report it either.
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
	for tag, entry := range q.entries {
		if tag < minTag || tag > maxTag || !entry.durable.Load() {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	return tags
}

// Count returns the number of DURABLE entries this cache holds for the queue.
//
// A NOT-yet-durable entry is NOT counted: it behaves exactly like a message
// that is not in the WAL yet, so the queue's reported message count never
// includes a message the broker has not made durable.
func (c *hotCache) Count(queue string) int {
	q := c.queueStateIfPresent(queue)
	if q == nil {
		return 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	hotCacheQueueLockFire(q.name)
	count := 0
	for _, entry := range q.entries {
		if entry.durable.Load() {
			count++
		}
	}
	return count
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

// globalUsedBytes reports the resident hot-cache bytes. It is the source of the
// amqp_hot_cache_bytes gauge.
func (c *hotCache) globalUsedBytes() int64 {
	return atomic.LoadInt64(&c.globalUsed)
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
