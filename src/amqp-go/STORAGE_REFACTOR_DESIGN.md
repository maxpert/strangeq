# RabbitMQ-Style Storage Architecture Design

## Problem Statement

Current storage implementation has critical issues:
1. **Unbounded memory growth** - `queue.Messages []` slice grows forever
2. **No lazy loading** - All messages kept in RAM regardless of size
3. **Synchronous disk writes** - Every durable publish = immediate disk I/O
4. **No memory pressure handling** - Can OOM with large queues
5. **No message index** - Cannot separate hot metadata from cold bodies

## RabbitMQ Architecture (Target)

```
┌─────────────────────────────────────────────────────────┐
│  Queue Storage Layers                                    │
├─────────────────────────────────────────────────────────┤
│ 1. Message Index (Always in RAM)                        │
│    - Metadata: deliveryTag, size, state, disk offset    │
│    - Size: ~64 bytes per message                        │
│    - Fast lookups, FIFO ordering maintained             │
│                                                          │
│ 2. Message Cache (Bounded RAM)                          │
│    - LRU cache of recent message bodies                 │
│    - Configurable size (default: 128MB per queue)       │
│    - Hot path messages stay cached                      │
│                                                          │
│ 3. Disk Persistence (BadgerDB)                          │
│    - Message bodies paged out under memory pressure     │
│    - Batched async writes (flush every 100ms)           │
│    - Lazy loading on cache miss                         │
│                                                          │
│ 4. Memory Manager                                        │
│    - Monitors memory usage per queue                    │
│    - Triggers paging at 90% threshold                   │
│    - Evicts cold messages (LRU policy)                  │
└─────────────────────────────────────────────────────────┘
```

## New Data Structures

### 1. Message Index Entry
```go
type MessageIndex struct {
    DeliveryTag  uint64
    RoutingKey   string        // Hot metadata
    Exchange     string         // Hot metadata
    Size         uint32         // Body size
    State        MessageState   // READY, UNACKED, PAGED
    CacheKey     string        // Key for cache lookup
    DiskOffset   uint64        // BadgerDB key/offset
    Timestamp    int64         // For LRU eviction
    Priority     uint8         // AMQP priority
}

type MessageState uint8
const (
    StateReady    MessageState = 0  // In cache, ready to deliver
    StateUnacked  MessageState = 1  // Delivered, awaiting ACK
    StatePaged    MessageState = 2  // Body on disk, needs load
)
```

### 2. Message Cache (LRU)
```go
type MessageCache struct {
    mu          sync.RWMutex
    cache       map[string]*CacheEntry  // deliveryTag -> entry
    lruList     *list.List              // Doubly-linked for O(1) LRU
    maxBytes    int64                   // Max cache size (e.g., 128MB)
    currentSize int64                   // Current size in bytes
}

type CacheEntry struct {
    Message    *protocol.Message
    Size       uint32
    Element    *list.Element  // Position in LRU list
    LastAccess int64          // Timestamp
}
```

### 3. Refactored Queue
```go
type Queue struct {
    Name         string
    Durable      bool

    // NEW: Index layer (always in RAM)
    Index        []*MessageIndex      // FIFO ordered
    IndexMap     map[uint64]int       // deliveryTag -> index position

    // NEW: Bounded cache
    Cache        *MessageCache

    // NEW: Paging state
    State        QueueState
    MemoryUsage  int64
    MemoryLimit  int64

    // Storage backend
    Store        MessageStore

    Mutex        sync.RWMutex
}

type QueueState uint8
const (
    StateNormal  QueueState = 0  // All messages in cache
    StatePaging  QueueState = 1  // Moving messages to disk
    StatePaged   QueueState = 2  // Most messages on disk
)
```

### 4. Memory Manager
```go
type MemoryManager struct {
    queues           map[string]*Queue
    totalMemory      int64
    maxMemory        int64            // Global limit
    pagingThreshold  float64          // 0.90 (90%)
    normalThreshold  float64          // 0.80 (80% - hysteresis)
    checkInterval    time.Duration    // 1 second
}
```

## Key Workflows

### Publishing Flow (Write Path)

```
1. Message arrives → broker.PublishMessage()
   ↓
2. Create MessageIndex entry (64 bytes in RAM)
   ↓
3. Add message body to Cache (if space available)
   ↓
4. Append index to queue.Index []
   ↓
5. [ASYNC] Batch write to disk buffer
   ↓
6. [ASYNC] Flush disk buffer every 100ms
   ↓
7. Notify consumers
```

**Changes:**
- ✅ Index always created (small footprint)
- ✅ Body cached if space available (bounded)
- ✅ Disk writes batched (not synchronous)
- ✅ Fast path remains fast

### Consumption Flow (Read Path)

```
1. Consumer requests message
   ↓
2. Get index entry from queue.Index[0]
   ↓
3. Check cache: cache.Get(deliveryTag)
   ├─ HIT:  Return cached message body ✓
   └─ MISS: Load from disk → Add to cache → Return
   ↓
4. Update index state → StateUnacked
   ↓
5. Send to consumer channel
   ↓
6. On ACK: Delete index entry + evict from cache
```

**Changes:**
- ✅ Index provides metadata without loading body
- ✅ Cache hit = zero disk I/O (hot path)
- ✅ Cache miss = lazy load from BadgerDB
- ✅ LRU keeps hot messages cached

### Memory Pressure Handling (Paging)

```
MemoryManager (background loop, every 1s):
  ↓
1. Calculate total memory: Σ queue.MemoryUsage
   ↓
2. If usage > 90% threshold:
   ↓
3. For each queue (sorted by size desc):
   - Set queue.State = StatePaging
   - Evict oldest cached messages (LRU)
   - Mark indices as StatePaged
   - Update queue.MemoryUsage
   ↓
4. If usage < 80% (hysteresis):
   - Set queue.State = StateNormal
   - Allow cache to fill naturally
```

**Changes:**
- ✅ Proactive memory management
- ✅ Prevents OOM
- ✅ Hysteresis avoids thrashing
- ✅ Per-queue limits + global limit

## Performance Characteristics

### Memory Usage

**Before (Current):**
```
Queue with 1M messages (1KB each):
- Memory: 1M * 1KB = 1GB (all in RAM)
- Risk: OOM on large queues
```

**After (New Architecture):**
```
Queue with 1M messages (1KB each):
- Index:  1M * 64B = 64MB (always in RAM)
- Cache:  128MB limit (hot messages)
- Disk:   ~872MB (paged out)
- Total RAM: 192MB (5x improvement!)
```

### Latency Impact

**Cache Hit (Hot Path):**
- Before: slice access = 1ns
- After:  cache lookup = 10ns (hash map + LRU update)
- **Overhead: 9ns (negligible)**

**Cache Miss (Cold Path):**
- Before: N/A (always in RAM)
- After:  BadgerDB read = 10-50μs (one-time cost)
- **Amortized: 100-500ns per message (batched reads)**

**Publish Latency:**
- Before: sync write = 50-500μs (disk I/O)
- After:  batched write = 1-10μs (in-memory buffer)
- **Improvement: 50-100x faster**

## Implementation Phases

### Phase 1: Message Index + Cache Foundation
**Files:**
- `protocol/message_index.go` - MessageIndex struct
- `protocol/message_cache.go` - LRU cache implementation
- `protocol/structures.go` - Update Queue struct

**Changes:**
- Add Index + Cache fields to Queue
- Maintain backward compatibility (keep Messages slice)
- Dual-write mode (both old + new)

**Testing:**
- All existing tests pass
- Cache hit rate > 95% in benchmarks

### Phase 2: Lazy Loading (Read Path)
**Files:**
- `broker/message_loading.go` - Lazy load logic
- `storage/badger_message_store.go` - Batch read support

**Changes:**
- Check cache before accessing message body
- Load from disk on cache miss
- Update LRU on access

**Testing:**
- Cache miss loads correctly
- Performance: hot path unchanged

### Phase 3: Async Batched Writes (Write Path)
**Files:**
- `storage/write_buffer.go` - Batched write buffer
- `storage/badger_message_store.go` - Async flush

**Changes:**
- Buffer writes for 100ms or 1000 messages
- Flush in background goroutine
- fsync on publisher confirms only

**Testing:**
- No data loss on crash (WAL)
- Publish latency < 10μs

### Phase 4: Memory Manager + Paging
**Files:**
- `broker/memory_manager.go` - Memory monitoring
- `broker/paging.go` - Eviction logic

**Changes:**
- Monitor memory every 1 second
- Evict when > 90% threshold
- Implement paging state machine

**Testing:**
- Memory usage stays bounded
- No OOM under load
- Eviction doesn't impact hot path

## Migration Strategy

### Backward Compatibility
- Keep `queue.Messages` slice for 1-2 releases
- Run in "dual-write" mode initially
- Gradual migration with feature flag

### Configuration
```go
type QueueConfig struct {
    // NEW
    MaxMemoryPerQueue int64  // Default: 128MB
    CacheEnabled      bool   // Default: true
    PageToDisk        bool   // Default: true (if durable)

    // Existing
    Durable           bool
    AutoDelete        bool
}
```

### Rollout Plan
1. **Release 1:** Index + Cache (opt-in via config)
2. **Release 2:** Lazy loading enabled by default
3. **Release 3:** Memory manager enabled
4. **Release 4:** Remove old Messages slice

## Success Metrics

1. **Memory Usage:** 5-10x reduction for large queues
2. **Publish Latency:** 50-100x improvement (async writes)
3. **Cache Hit Rate:** > 95% for normal workloads
4. **No OOM:** Memory stays bounded under load
5. **Zero Breaking Changes:** All tests pass

## Open Questions

1. **Global vs Per-Queue Limits?**
   - Proposal: Both (global 2GB, per-queue 128MB)

2. **Cache Eviction Policy?**
   - Proposal: LRU (simple, effective)
   - Alternative: LRU-K, ARC (more complex)

3. **Batched Write Flush Interval?**
   - Proposal: 100ms or 1000 messages
   - Trade-off: latency vs throughput

4. **Disk Format?**
   - Keep current BadgerDB schema
   - Or optimize for sequential reads?

## Next Steps

1. Review this design with team
2. Create feature flag: `ENABLE_MESSAGE_INDEX=true`
3. Implement Phase 1 with tests
4. Benchmark and iterate
5. Roll out gradually with monitoring
