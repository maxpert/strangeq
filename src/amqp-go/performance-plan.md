# Performance Optimization: Logging and Mutex Contention Reduction

## Completed Work

### Phase 1: Logging Bottleneck Elimination (COMPLETED)

**Date**: November 2025

**Problem Identified**:
Zap logging was causing severe mutex contention in hot paths, accounting for 98% of all mutex wait time (1,185s out of 1,207s total contention).

**Root Cause Analysis**:
- Zap serializes ALL logging operations through a single mutex
- Log.Info() calls in hot paths (ACK/NACK/REJECT, consumer delivery, queue operations) were called on EVERY message
- Example: Message acknowledgment logged on EVERY message consumed, causing severe serialization

**Optimizations Implemented**:

#### Phase 1.1: Initial sync.Map Conversion
- Replaced `activeConsumers` map with `sync.Map` for lock-free consumer lookups
- Result: Small improvement but logging still dominated

#### Phase 1.2: First Logging Pass
- Converted initial set of Info logs to Debug in hot paths
- Result: 5x improvement (baseline → 138K msg/s consume)

#### Phase 1.3: Comprehensive Logging Audit (THIS PHASE)
Systematically analyzed and converted 17 critical log statements from `Log.Info` to `Log.Debug`:

**Critical Hot Paths (called on EVERY message)**:
- `server/basic_handlers.go:821` - Message acknowledged
- `server/basic_handlers.go:890` - Message rejected
- `server/basic_handlers.go:961` - Message negatively acknowledged

**High-Frequency Consumer Operations**:
- `server/basic_handlers.go:338` - Basic consume requested
- `server/basic_handlers.go:369` - Creating consumer with bounded channel
- `server/basic_handlers.go:411` - Consumer registered
- `server/basic_handlers.go:480` - Consumer cancelled

**Consumer Delivery Loop**:
- `server/consumer_delivery.go:21` - Starting consumer delivery loop
- `server/consumer_delivery.go:55` - Added consumer to delivery loop
- `server/consumer_delivery.go:110` - Consumer channel closed

**Queue Operations**:
- `server/queue_handlers.go:43` - Queue declared
- `server/queue_handlers.go:82` - Queue bound
- `server/queue_handlers.go:124` - Queue unbound
- `server/queue_handlers.go:161` - Queue deleted

**Exchange Operations**:
- `server/exchange_handlers.go:43` - Exchange declared
- `server/exchange_handlers.go:84` - Exchange deleted
- `server/exchange_handlers.go:114` - Exchange unbound

**Results**:

| Metric | Before Phase 1 | After Phase 1.3 | Improvement |
|--------|---------------|-----------------|-------------|
| Publish throughput | ~30K msg/s | 173-188K msg/s | **6.3x** |
| Consume throughput | ~24K msg/s | 118-133K msg/s | **5.5x** |
| Mutex contention | 1,207s total | 155-302s total | **62-87% reduction** |
| Zap logging contention | 1,185s (98%) | 0s (eliminated) | **100% eliminated** |

**Key Insight**:
The server is now so fast it hits OS-level TCP buffer limits at 50p/50c (100 connections):
- 171,148 packets dropped by kernel (errno ENOBUFS)
- Server sustained 187K msg/s before TCP buffer exhaustion
- This is a *good problem* - server throughput exceeds OS network stack capacity

**Benchmarks**:
- 20 producers / 20 consumers: **173K msg/s publish, 118K msg/s consume** (stable)
- 50 producers / 50 consumers: **188K msg/s publish, 133K msg/s consume** (hits TCP limits after 9 seconds)

**Current Bottleneck** (as shown in mutex profile):
```
155.78s total contention:
  99.19s (63.67%) - StorageBroker.AcknowledgeMessage (lock in ACK path)
  16.07s (10.31%) - Queue.Poll (message retrieval)
   8.47s (5.43%)  - Queue.Put (message enqueue)
```

The next major optimization target is `StorageBroker.AcknowledgeMessage` (Phase 1.4: defer lock optimization).

---

## Future Optimization Roadmap

### Phase 1.4: Defer Lock Optimization (NOT STARTED)
**Target**: Optimize defer locks in ACK paths

**Locations identified**:
- `broker/storage_broker.go` - ACK/NACK/REJECT handlers (3 locations)

**Approach**:
- Replace `defer mu.Unlock()` with explicit unlock before return
- Reduces defer overhead in hot paths
- Expected: 10-15% improvement

**Estimated Impact**: Additional 15-20K msg/s throughput

---

### Phase 2: Exchange Routing Cache (NOT STARTED)
**Target**: Add lock-free exchange lookup cache using sync.Map

**Current Issue**:
- Exchange lookups on every publish acquire mutex
- Contention visible in profiles (though minor compared to ACK path)

**Approach**:
```go
type Broker struct {
    exchanges     map[string]*Exchange
    exchangeCache sync.Map  // lock-free read cache
    mu            sync.RWMutex
}

func (b *Broker) GetExchange(name string) *Exchange {
    // Fast path: check cache
    if ex, ok := b.exchangeCache.Load(name); ok {
        return ex.(*Exchange)
    }

    // Slow path: lookup and cache
    b.mu.RLock()
    ex := b.exchanges[name]
    b.mu.RUnlock()

    if ex != nil {
        b.exchangeCache.Store(name, ex)
    }
    return ex
}
```

**Estimated Impact**: 20-30% improvement in publish throughput

---

### Phase 3: Message Batching (NOT STARTED)
**Target**: Batch multiple messages in single transactions

**Benefits**:
- Amortize lock acquisition costs
- Reduce syscalls
- Better disk I/O for persistent storage

**Estimated Impact**: 2-3x improvement in high-load scenarios

---

### Phase 4: Lock-Free Queue Implementation (NOT STARTED)
**Target**: Replace bounded channel queue with lock-free ring buffer

**Current Issue**:
- Queue.Poll/Queue.Put show 24.54s contention (16% of total)
- Using `github.com/Workiva/go-datastructures/queue` which has internal locks

**Approach**:
- Consider MPSC (multi-producer single-consumer) lock-free queue
- Or use Go channels directly (already lock-free internally)

**Estimated Impact**: 30-50% improvement in consumer throughput

---

## Performance Targets

**Current Performance** (Phase 1.3 complete):
- ✅ 173K msg/s publish (20p/20c stable)
- ✅ 118K msg/s consume (20p/20c stable)
- ✅ 188K msg/s publish (50p/50c peaks, hits OS limits)
- ✅ 62-87% reduction in mutex contention
- ✅ 100% elimination of logging bottleneck

**Future Targets** (all phases complete):
- 🎯 300K+ msg/s publish throughput
- 🎯 250K+ msg/s consume throughput
- 🎯 <100s total mutex contention (90%+ reduction from baseline)
- 🎯 <5ms p99 latency
- 🎯 Linear scaling with CPU cores

---

## Profiling and Observability

### Integrated Telemetry Endpoint
The server now includes an integrated HTTP telemetry endpoint that provides both Prometheus metrics and pprof profiling in a single service:

**Endpoint**: `http://localhost:9419` (default)

**Available Routes**:
- `/metrics` - Prometheus metrics (message rates, queue depths, etc.)
- `/health` - Health check endpoint
- `/debug/pprof/` - pprof index (when profiling enabled)
- `/debug/pprof/profile` - CPU profile
- `/debug/pprof/heap` - Heap profile
- `/debug/pprof/mutex` - Mutex contention profile
- `/debug/pprof/block` - Blocking profile

**Usage**:
```bash
# Enable telemetry endpoint
./amqp-server --enable-telemetry --telemetry-port 9419

# Collect CPU profile (30 seconds)
curl -o cpu.prof http://localhost:9419/debug/pprof/profile?seconds=30

# Collect mutex profile
curl -o mutex.prof http://localhost:9419/debug/pprof/mutex

# View profiles
go tool pprof -http=:8080 mutex.prof
```

**Configuration**:
- Profiling is disabled by default (production-safe)
- Enable with `--enable-telemetry` flag
- Customize port with `--telemetry-port` flag
- All pprof profiles available when enabled

---

## Methodology and Tools

### Profiling Workflow
1. **Run with profiling enabled**: `./amqp-server --enable-telemetry`
2. **Generate load**: Use perftest or production traffic
3. **Capture profiles**: curl endpoints during load
4. **Analyze**: `go tool pprof -top -cum mutex.prof`

### Key Metrics to Monitor
- **Mutex contention**: Total wait time, dominant functions
- **Throughput**: msg/s publish and consume rates
- **Latency**: p50, p95, p99 end-to-end
- **CPU**: Usage per core, context switches
- **Memory**: Allocations per operation, GC pauses

### Benchmark Scripts
- `scripts/run_benchmark.sh` - Automated profiling runs
- Captures CPU, heap, mutex, block, and allocation profiles
- Saves timestamped results for comparison

---

## Lessons Learned

1. **Logging is expensive**: Structured logging (Zap) has hidden costs
   - Even "cheap" log statements cause mutex contention
   - Use Debug level for hot paths, Info for cold paths

2. **Profile, don't guess**: Mutex profiling revealed the real bottleneck
   - Assumptions about performance are usually wrong
   - Measure before and after every optimization

3. **Lock-free data structures matter**: sync.Map for read-heavy workloads
   - 10-100x better than mutex-protected maps for concurrent reads
   - Slower for writes, but acceptable trade-off

4. **OS limits are real**: TCP buffer exhaustion at high throughput
   - macOS default: 128KB buffers, 8MB total
   - Tune with `sysctl kern.ipc.maxsockbuf` if needed

5. **Incremental optimization works**: Phased approach with measurements
   - Each phase builds on previous improvements
   - Clear metrics at every step prevent regressions

---

## References

- **Mutex Profiling**: https://go.dev/blog/pprof
- **sync.Map Performance**: https://golang.org/pkg/sync/#Map
- **TCP Tuning**: Stevens, "Unix Network Programming"
- **Go Performance**: https://dave.cheney.net/high-performance-go-workshop

---

## Maintenance Notes

**Changed Files** (Phase 1.3):
- `server/basic_handlers.go` - 7 log conversions
- `server/consumer_delivery.go` - 3 log conversions
- `server/queue_handlers.go` - 4 log conversions
- `server/exchange_handlers.go` - 3 log conversions
- `cmd/amqp-server/main.go` - Integrated telemetry endpoint
- `metrics/server.go` - Added pprof routes

**Testing**:
- All existing tests pass
- Benchmarks show consistent improvement
- No functional regressions observed

**Deployment**:
- Change is backward compatible
- Debug logs can be enabled with `--log-level debug`
- Telemetry endpoint off by default (production-safe)
