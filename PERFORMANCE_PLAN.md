# Phase 16: Performance Tuning Plan

Compiled from 6 parallel investigation agents covering: disruptor hot path, memory allocations, lock contention, EngineConfig tuning, consumer delivery path, and storage I/O patterns.

---

## Investigation Summary

| Investigation | Key Finding | Impact |
|--------------|-------------|--------|
| Disruptor hot path | Backpressure defeated (no-op consumer), config ignored (hardcoded values) | Ring buffer provides zero flow control |
| Memory allocations | ~9-11GB remaining savings; body pre-allocation is #1 win | ~4.6GB from one fix |
| Lock contention | `ds.mutex` write lock on every publish serializes all queues | #1 throughput limiter |
| EngineConfig tuning | 6 dead config fields, 80MB/queue memory, 10s metrics walk | Misleading config + excessive memory |
| Consumer delivery | `reflect.Select` allocations, double delivery loop, missing WriteMutex | Correctness + perf |
| Storage I/O | File handle churn, segment metadata loss, checkpoint data loss window | 3 correctness bugs + I/O optimizations |

---

## Priority Classification

### P0: Correctness Bugs (must fix before performance work)

| # | Bug | File:Line | Description |
|---|-----|-----------|-------------|
| C1 | Missing `conn.WriteMutex` in `sendBasicDeliver` | `basic_handlers.go:806` | Delivery writes bypass mutex → frame corruption race with heartbeats |
| C2 | Double `consumerDeliveryLoop` | `server.go:172 + 312` | Two goroutines compete for same consumer channels → message stealing, 2x overhead |
| C3 | `deliveryIndex` global key collision | `storage_broker.go:55` | Keyed by `msgID` (per-queue counter) → wrong consumer on ACK with multiple queues |
| C4 | Segment format loses AMQP metadata | `segment_manager.go:642` | `serializeSegmentMessage` stores only offset+body → consumers get incomplete messages from cold storage |
| C5 | Checkpoint data loss window | `wal_manager.go:1210` | WAL deleted before segment fsync → crash loses messages |
| C6 | Segment index not loaded on startup | `segment_manager.go` | No `loadIndexFromDisk` → cold-storage messages unreachable after restart |
| C7 | `queueConsumers` non-atomic RMW | `storage_broker.go:527-530` | Concurrent register/unregister can lose consumers |
| C8 | WAL `ackChan` silent drops | `wal_manager.go:237` | Non-blocking send drops ACKs when full → WAL files never cleaned → disk leak |

### P1: High-Impact Performance Fixes

| # | Issue | File:Line | Est. Impact |
|---|-------|-----------|-------------|
| P1 | `ds.mutex` write lock on every publish | `disruptor_storage.go:233` | **Largest single throughput win** — replace with `sync.Map` |
| P2 | Pre-allocate `pendingMsg.Body` to `BodySize` | `basic_handlers.go:132` | **~4.6GB allocation savings** |
| P3 | Wire ring buffer config (currently hardcoded) | `disruptor_storage.go:16-20` | ✅ DONE — Config `RingBufferSize`/`SpillThresholdPercent` wired from EngineConfig |
| P4 | Cache WAL file handles for reads | `wal_manager.go:851` | Eliminate 2 syscalls + 4 allocs per WAL read |
| P5 | Pool `ContentHeader.Serialize()` | `content.go:235` | ✅ DONE — AppendUint16/64 + exact pre-alloc, 1 alloc (was 2-3) |
| P6 | Reduce `AvailableChannelBuffer` 10M → 100K | `config.go:77` | 80MB → 800KB per queue (100x memory reduction) |
| P7 | Cache `reflect.SelectCase` slices | `consumer_delivery.go:86` | Eliminate 2 allocs per loop iteration |
| P8 | Fix durable message spill bypass | `disruptor_storage.go:325` | Durable messages always enter ring buffer even above threshold |
| P9 | Pool WAL `done` channels | `wal_manager.go:211` | ✅ DONE — sync.Pool for chan error, defensive drain |
| P10 | Pool `allFrames` combined buffer | `basic_handlers.go:779` | 1 fewer large alloc per delivered message |

### P2: Medium-Impact Improvements

| # | Issue | File:Line | Est. Impact |
|---|-------|-----------|-------------|
| M1 | Replace `reflect.Select` with fan-in channel | `consumer_delivery.go` | ✅ DONE — O(1) select, per-consumer forwarders, consumer removal detection |
| M2 | Batch segment ACK marking | `segment_manager.go:179` | ✅ DONE — ackChan + batchAckLoop, single bitmapMutex per batch |
| M3 | Use `pread` (ReadAt) in WAL reads | `wal_manager.go:863` | Halve syscalls per WAL read |
| M4 | Use `fdatasync` instead of `Sync` | `wal_manager.go:568` | ✅ DONE — syscall.Fdatasync on Linux, Sync fallback on other platforms |
| M5 | Batch segment writes during checkpoint | `segment_manager.go:290` | ✅ DONE — Single mutex + single write per batch, sealSegment returns error |
| M6 | Fix dead config fields (6 fields) | multiple | Wire or remove: `ConsumerSelectTimeoutMS`, `ConsumerMaxBatchSize`, `ExpiredMessageCheckIntervalMS`, `OffsetCleanup*`, `Fsync` |
| M7 | Reduce `WALBatchTimeoutMS` 10 → 5 | `config.go:85` | Halve tail latency for lone durable messages |
| M8 | Reduce `SegmentSize` 1GB → 256MB | `config.go:90` | 4x faster compaction, smaller I/O spikes |
| M9 | Fix system metrics filesystem walk | `system_metrics.go:18` | Change 10s → 60s, or incremental tracking |
| M10 | Pool `BasicDeliverMethod.Serialize()` | `methods.go:2121` | ✅ DONE — AppendUint64 + pre-alloc, 1 alloc (was 2) |
| M11 | Eliminate WAL temp byte slices | `wal_manager.go:628` | Write directly into buf via `binary.BigEndian` |
| M12 | Remove dead pools | `buffer_pool.go:45,86` | Cleanup: `byteSlicePool`, `smallBufferPool` |

### P3: Architectural Improvements (longer term)

| # | Issue | Description |
|---|-------|-------------|
| A1 | Fix or remove disruptor | No-op consumer defeats backpressure — replace with atomic-indexed ring or implement real consumer tracking |
| A2 | Move ACK processing off frame processor goroutine | Eliminate head-of-line blocking between publish fsync and ACK processing |
| A3 | Replace `semaphore.Weighted` with channel-based credits | ~2x faster flow control |
| A4 | Batch TCP writes in `sendBatchedDeliveries` | N→1 syscalls per consumer batch |
| A5 | mmap sealed segments for reads | Zero-syscall reads from cold storage |
| A6 | Per-queue delivery index | Fix correctness + reduce global sync.Map contention |
| A7 | Consumer-discovery signal instead of polling | Eliminate O(channels) re-scan per delivery iteration |

---

## Execution Plan

### Commit 1: Correctness fixes (P0: C1-C8)
- Fix missing WriteMutex, double delivery loop, deliveryIndex collision
- Fix segment metadata, checkpoint data loss, segment index loading
- Fix queueConsumers race, WAL ackChan drops
- **Tests**: Each fix gets a spec-driven test reproducing the bug
- **Review**: Full review cycle

### Commit 2: Lock contention + config wiring (P1: P1, P3, P6, P8)
- Replace `ds.mutex` with `sync.Map` for queue rings
- Wire `RingBufferSize`/`SpillThresholdPercent` from EngineConfig
- Reduce `AvailableChannelBuffer` default to 100K
- Fix durable message spill bypass
- **Tests**: Concurrent publish benchmark, config wiring tests, spill behavior tests

### Commit 3: Memory allocation optimizations (P1: P2, P5, P9, P10 + P2: M10, M11, M12)
- Pre-allocate body in `processHeaderFrame`
- Pool ContentHeader.Serialize() and BasicDeliverMethod.Serialize()
- Pool WAL done channels and temp byte slices
- Pool allFrames buffer in sendBasicDeliver
- Remove dead pools
- **Tests**: Allocation benchmarks (allocs/op before vs after)

### Commit 4: Consumer delivery + I/O improvements (P1: P4, P7 + P2: M1-M5)
- Cache WAL file handles
- Cache reflect.SelectCase slices (or replace with fan-in)
- Batch segment ACK marking
- Use pread in WAL reads
- Use fdatasync on Linux
- Batch segment writes during checkpoint
- **Tests**: WAL read benchmark, consumer delivery benchmark

### Commit 5: Config tuning + cleanup (P2: M6-M9)
- Wire or remove dead config fields
- Reduce WALBatchTimeoutMS, SegmentSize defaults
- Fix system metrics interval
- **Tests**: Config validation tests for all wired fields

---

## Expected Outcomes

| Metric | Current | Target | Source |
|--------|---------|--------|--------|
| Total allocations | ~39 GB | ~28 GB | P2 + P5 + P9 + P10 + M10 + M11 |
| Per-queue memory | 82 MB | ~1 MB | P6 |
| Publish throughput (single queue) | Limited by `ds.mutex` | 2-5x improvement | P1 |
| Consumer delivery latency | 5-20µs (reflect.Select) | 1-5µs (fan-in) | M1 |
| WAL read latency (miss path) | 5 syscalls | 1 pread | P4 + M3 |
| fsync latency | `Sync()` | `fdatasync()` (10-20% faster) | M4 |
| Tail latency (durable) | 10ms | 5ms | M7 |
