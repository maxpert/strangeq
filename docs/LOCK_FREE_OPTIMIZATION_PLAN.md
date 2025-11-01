# Lock-Free Optimization Plan: Beat RabbitMQ Performance

## Goal
Achieve 75-125K msg/s consumption throughput (1.15x - 1.9x vs RabbitMQ's 65K msg/s) through lock-free architecture.

## Current State (Baseline)
- **Publish**: 184K msg/s (already 2.7x faster than RabbitMQ!)
- **Consume**: 25K msg/s (2.6x slower than RabbitMQ's 65K msg/s)
- **Bottleneck**: 100% on consumer delivery, NOT queue storage

## Root Cause Analysis
1. **Shared Mutable State with Locks**
   - 20 drain goroutines compete for `b.Mutex.RLock()`
   - Queue mutex locked for every batch retrieval
   - Channel mutex for prefetch checks
   - Even read locks serialize at cache-line level

2. **Push Model (Current) vs Pull Model (Natural)**
   - Current: Queue PUSHES messages → drain goroutine → round-robin → consumer
   - Ideal: Consumer PULLS from queue using own cursor
   - Push requires coordination (locks, round-robin)
   - Pull is naturally conflict-free

3. **Data Copying & GC Pressure**
   - Message copied: storage → cache → delivery → frame buffer
   - Multiple allocations per message
   - CPU cycles wasted on memcpy

4. **Algorithmic Inefficiencies**
   - Round-robin requires atomic counter per message
   - Batch retrieval still O(n) with GetAll()
   - Prefetch checking locks channel mutex per consumer

## Revolutionary Architecture: Lock-Free Consumer Cursors

### Core Concept: Invert the Flow
```
Current (PUSH):
  Queue → Drain Goroutine → Round-Robin → Consumer
           ↑
      Locks everything!

New (PULL):
  Consumer₁ → Own Cursor → Atomic Read → Lock-Free Ring Buffer
  Consumer₂ → Own Cursor → Atomic Read → Lock-Free Ring Buffer
  Consumer₃ → Own Cursor → Atomic Read → Lock-Free Ring Buffer
           ↑
       NO LOCKS!
```

### Key Technologies
- **Lock-Free Ring Buffer**: Use `github.com/hedzr/go-ringbuf` (MPMC, generic support)
- **Atomic Operations**: CAS (Compare-And-Swap) for all state updates
- **sync.Map**: Lock-free map for broker state (consumers, queues)
- **Per-Consumer Goroutines**: Each consumer pulls independently

## Implementation Phases

### ✅ Phase 0: Research & Planning (COMPLETED)
- [x] Research lock-free queue libraries
- [x] Identify hedzr/go-ringbuf as best fit (MPMC, v2 with generics)
- [x] Create optimization plan document
- [x] Define success criteria (beat RabbitMQ)

### 🔄 Phase 1: Per-Consumer Pull Model (IN PROGRESS)
**Goal**: Replace push-based drain with pull-based consumer cursors
**Expected**: 2x improvement (25K → 50K msg/s)

#### Task List:
- [ ] Create `ConsumerCursor` struct with atomic position tracking
- [ ] Implement `pullLoop()` goroutine per consumer
- [ ] Replace drain goroutines with consumer-initiated pulls
- [ ] Use atomic operations for prefetch checking
- [ ] Remove all queue-level locking from consumption path
- [ ] Add proper cleanup mechanism for stopped consumers
- [ ] Update consumer registration to launch pull goroutine
- [ ] Update consumer cancellation to stop pull goroutine
- [ ] Benchmark: measure improvement

**Files to Modify**:
- `broker/broker.go`: Remove drain goroutines, add cursor management
- `protocol/structures.go`: Add ConsumerCursor struct
- `server/basic_handlers.go`: Update consume/cancel handlers

### ⏳ Phase 2: Lock-Free Ring Buffer Queue
**Goal**: Replace mutex-based queue with lock-free ring buffer
**Expected**: 2.5x total improvement (25K → 62K msg/s)

#### Task List:
- [ ] Add dependency: `go get github.com/hedzr/go-ringbuf/v2`
- [ ] Create `LockFreeQueue` wrapper around hedzr ringbuf
- [ ] Implement lock-free `Publish()` using ring buffer
- [ ] Implement lock-free `Consume()` using atomic cursors
- [ ] Implement lock-free `Ack()` with tail advancement
- [ ] Replace Queue.IndexManager with ring buffer
- [ ] Remove all Queue.Mutex operations
- [ ] Add bounded capacity handling (queue full)
- [ ] Benchmark: measure improvement

**Files to Create**:
- `broker/lockfree_queue.go`: Lock-free queue implementation

**Files to Modify**:
- `protocol/structures.go`: Update Queue struct
- `broker/broker.go`: Use LockFreeQueue

### ⏳ Phase 3: Lock-Free Broker State
**Goal**: Replace all maps with sync.Map, eliminate broker mutex
**Expected**: 2.6x total improvement (25K → 65K msg/s) - **MATCHES RabbitMQ**

#### Task List:
- [ ] Replace `Broker.Queues` with sync.Map
- [ ] Replace `Broker.Consumers` with sync.Map
- [ ] Replace `Broker.QueueConsumers` with sync.Map
- [ ] Replace `Broker.ConsumerRR` with sync.Map
- [ ] Remove `Broker.Mutex` entirely
- [ ] Update all map operations to use sync.Map API
- [ ] Update DeclareQueue to use sync.Map
- [ ] Update RegisterConsumer to use sync.Map
- [ ] Benchmark: measure improvement

**Files to Modify**:
- `broker/broker.go`: Replace all maps with sync.Map
- All broker method calls that access maps

### ⏳ Phase 4: Zero-Copy Message Delivery
**Goal**: Eliminate message copying, use pointer references
**Expected**: 3x total improvement (25K → 75K msg/s) - **BEATS RabbitMQ**

#### Task List:
- [ ] Implement pre-allocated frame buffers per consumer
- [ ] Use unsafe.Pointer for message references in ring buffer
- [ ] Implement zero-copy frame building (direct pointer writes)
- [ ] Add buffer pooling with sync.Pool
- [ ] Avoid message body copying during delivery
- [ ] Reuse delivery buffers per consumer
- [ ] Benchmark: measure improvement

**Files to Create**:
- `server/zerocopy_delivery.go`: Zero-copy delivery implementation

**Files to Modify**:
- `server/basic_handlers.go`: Use zero-copy delivery
- `protocol/structures.go`: Add buffer pools

### ⏳ Phase 5: Advanced Optimizations
**Goal**: CPU pinning, NUMA awareness, batch network writes
**Expected**: 4-5x total improvement (25K → 100-125K msg/s) - **CRUSHES RabbitMQ**

#### Task List:
- [ ] Research Go runtime.LockOSThread() for CPU pinning
- [ ] Implement NUMA-aware memory allocation (if applicable)
- [ ] Add batch frame writing to network (reduce syscalls)
- [ ] Optimize string operations (zero-allocation)
- [ ] Profile with pprof and eliminate remaining hotspots
- [ ] Add CPU affinity for consumer goroutines
- [ ] Benchmark: final measurements

**Files to Create**:
- `broker/cpu_affinity.go`: CPU pinning utilities

## Performance Projections

| Phase | Optimization | Expected | vs Baseline | vs RabbitMQ |
|-------|-------------|----------|-------------|-------------|
| Baseline | Current async drain | 25K msg/s | 1.0x | 0.38x |
| Phase 1 | Per-consumer pull | 50K msg/s | 2.0x | 0.77x |
| Phase 2 | Lock-free ring buffer | 62K msg/s | 2.5x | 0.95x |
| Phase 3 | Lock-free broker state | 65K msg/s | 2.6x | **1.0x** ✅ |
| Phase 4 | Zero-copy delivery | 75K msg/s | 3.0x | **1.15x** 🎯 |
| Phase 5 | Advanced optimizations | 100-125K msg/s | 4-5x | **1.5-1.9x** 🚀 |

## Validation & Testing

### Correctness Requirements
- [ ] Message ordering maintained (FIFO per queue)
- [ ] Exactly-once acknowledgment (no double-ack)
- [ ] No memory leaks (proper cleanup)
- [ ] No race conditions (verified with -race)
- [ ] All existing tests pass

### Performance Requirements
- [ ] Baseline benchmark: 25K msg/s (current)
- [ ] Phase 1 target: 50K msg/s
- [ ] Phase 2 target: 62K msg/s
- [ ] Phase 3 target: 65K msg/s (match RabbitMQ)
- [ ] Phase 4 target: 75K msg/s (beat RabbitMQ)
- [ ] Phase 5 target: 100-125K msg/s (crush RabbitMQ)

### Testing Strategy
1. **Unit Tests**: Each atomic operation independently
2. **Race Detector**: `go test -race ./...`
3. **Stress Tests**: 1000 producers × 1000 consumers
4. **Correctness**: Message count validation
5. **Continuous Benchmarking**: After each phase
6. **Comparison**: Head-to-head with RabbitMQ

## Risk Mitigation

### Technical Risks
- **Complexity**: Lock-free algorithms are hard to get right
  - Mitigation: Incremental implementation, extensive testing
- **Race Conditions**: CAS operations can have subtle bugs
  - Mitigation: Race detector, fuzz testing, code review
- **Memory Model**: Go's memory model interactions with atomics
  - Mitigation: Study Go memory model, use proven patterns

### Project Risks
- **Time Estimate**: 5 weeks full-time effort
  - Mitigation: Incremental phases, can stop after any phase
- **Backward Compatibility**: API changes may break users
  - Mitigation: Internal refactoring only, keep public API stable
- **Regression**: New bugs introduced
  - Mitigation: Comprehensive test suite, gradual rollout

## Success Criteria

### Minimum Success (Phase 3)
- ✅ Match RabbitMQ performance (65K msg/s)
- ✅ All tests passing
- ✅ No race conditions
- ✅ Stable under load

### Stretch Goal (Phase 4-5)
- 🎯 Beat RabbitMQ by 15-90% (75-125K msg/s)
- 🎯 Zero memory leaks under continuous load
- 🎯 Sub-microsecond latency per message
- 🎯 Horizontal scaling validated

## Dependencies

### External Libraries
```bash
go get github.com/hedzr/go-ringbuf/v2  # Lock-free MPMC ring buffer
```

### Go Version
- Minimum: Go 1.19 (generics support)
- Recommended: Go 1.21+ (improved sync.Map performance)

## Monitoring & Observability

### Metrics to Track
- [ ] Messages consumed per second
- [ ] Average delivery latency (p50, p95, p99)
- [ ] Lock contention (before/after)
- [ ] GC pause time
- [ ] Allocations per message
- [ ] CPU utilization per consumer

### Benchmarking Commands
```bash
# Current baseline
cd benchmark && ./perftest -producers 20 -consumers 20 -duration 60s -size 1024 -prefetch 100

# With Go profiling
go test -bench=. -cpuprofile=cpu.prof -memprofile=mem.prof
go tool pprof cpu.prof
```

## Documentation Updates

### Files to Update
- [ ] `README.md`: Add lock-free architecture section
- [ ] `PERFORMANCE.md`: Update with new benchmarks
- [ ] `ARCHITECTURE.md`: Explain lock-free design
- [ ] `CHANGELOG.md`: Document performance improvements

## Next Steps

1. **Start Phase 1**: Implement per-consumer pull model
2. **Benchmark**: Measure 2x improvement
3. **Validate**: Run all tests with race detector
4. **Proceed**: If successful, move to Phase 2

---

*Last Updated: 2025-10-31*
*Status: Phase 1 IN PROGRESS*
