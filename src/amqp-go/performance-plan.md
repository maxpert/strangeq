# Performance Improvement Plan: Phase 11

## Overview

This document outlines comprehensive performance optimizations at the socket, parsing, and command/method dispatch levels. The goal is to achieve 10-100x throughput improvements for high-load scenarios.

## Current Performance Baseline

**Current Protocol Performance (Phase 10):**
- Frame operations: 24-65M ops/sec
- Method serialization: 7-29M ops/sec
- Throughput: 3-6 GB/s (protocol layer)
- Zero-allocation writes achieved

**Current Server Performance (from benchmarks):**
- Memory storage: ~320-370 ns/op
- Badger storage: 14ms-66μs per operation
- Message publishing (memory): 368 ns/op
- Message publishing (badger): 66μs/op

**Performance Bottlenecks Identified:**
1. Socket I/O: Synchronous, blocking operations
2. Parsing: Repeated byte extraction, no caching
3. Dispatch: Multiple function calls, no optimization
4. Locking: Contention in broker operations
5. Memory: Allocations in hot paths
6. I/O: No batching, immediate writes

---

## Phase 11.1: Socket-Level Optimizations

### 11.1.1 TCP Socket Tuning
**Goal**: Reduce latency and improve throughput

**Optimizations:**
- Enable `TCP_NODELAY` to disable Nagle's algorithm
  - Reduces latency from ~200ms to <1ms for small frames
  - Critical for interactive workloads
- Configure socket buffer sizes
  - Set `SO_RCVBUF` and `SO_SNDBUF` to 256KB (from default 64KB)
  - Reduces syscalls by allowing larger reads/writes
- Enable socket keepalive
  - Set `SO_KEEPALIVE` with appropriate intervals
  - Detect dead connections faster

**Implementation:**
```go
// socket_tuning.go
func ConfigureSocket(conn net.Conn) error {
    tcpConn := conn.(*net.TCPConn)

    // Disable Nagle's algorithm for low latency
    if err := tcpConn.SetNoDelay(true); err != nil {
        return err
    }

    // Set socket buffer sizes
    if err := tcpConn.SetReadBuffer(256 * 1024); err != nil {
        return err
    }
    if err := tcpConn.SetWriteBuffer(256 * 1024); err != nil {
        return err
    }

    // Enable keepalive
    if err := tcpConn.SetKeepAlive(true); err != nil {
        return err
    }
    if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
        return err
    }

    return nil
}
```

**Expected Impact:**
- 20-40% latency reduction for small frames
- 10-15% throughput improvement
- Faster dead connection detection

---

### 11.1.2 Buffered I/O
**Goal**: Reduce syscall overhead

**Current Issue:**
- Direct `conn.Read()` and `conn.Write()` calls
- Each frame triggers a syscall
- Inefficient for small frames

**Optimization:**
```go
// buffered_io.go
type BufferedConnection struct {
    conn   net.Conn
    reader *bufio.Reader
    writer *bufio.Writer
}

func NewBufferedConnection(conn net.Conn) *BufferedConnection {
    return &BufferedConnection{
        conn:   conn,
        reader: bufio.NewReaderSize(conn, 64*1024),  // 64KB read buffer
        writer: bufio.NewWriterSize(conn, 64*1024),  // 64KB write buffer
    }
}

func (bc *BufferedConnection) ReadFrame() (*Frame, error) {
    return ReadFrame(bc.reader)  // Use buffered reader
}

func (bc *BufferedConnection) WriteFrame(frame *Frame) error {
    if err := WriteFrameOptimized(bc.writer, frame); err != nil {
        return err
    }
    // Don't flush immediately - batch writes
    return nil
}

func (bc *BufferedConnection) Flush() error {
    return bc.writer.Flush()
}
```

**Batching Strategy:**
- Flush after processing multiple frames
- Flush on heartbeat intervals
- Flush before blocking operations

**Expected Impact:**
- 30-50% reduction in syscalls
- 2-3x throughput for small frames
- Lower CPU usage

---

### 11.1.3 Zero-Copy I/O (Advanced)
**Goal**: Eliminate memory copies

**Optimization:**
```go
// zero_copy_io.go
// Use io.ReaderFrom and io.WriterTo interfaces
func (f *Frame) WriteTo(w io.Writer) (int64, error) {
    // Already implemented in frame_optimized.go
    // Extend to use sendfile() syscall on Unix
}

// For Linux: use splice() and sendfile()
func sendFrameDirect(sockFd int, data []byte) error {
    // Use unix.Sendfile() for zero-copy
    return unix.Sendfile(sockFd, pipeFd, nil, len(data))
}
```

**Platform-Specific:**
- Linux: `splice()`, `sendfile()`
- BSD/macOS: `sendfile()`
- Windows: `TransmitFile()`

**Expected Impact:**
- 10-20% CPU reduction
- 15-25% throughput improvement for large frames
- Lower memory pressure

---

### 11.1.4 Connection Pooling (Per-Channel Buffers)
**Goal**: Reuse buffers per channel

**Optimization:**
```go
// channel_buffers.go
type ChannelBuffers struct {
    readBuf  []byte
    writeBuf *bytes.Buffer
}

var channelBufferPool = sync.Pool{
    New: func() interface{} {
        return &ChannelBuffers{
            readBuf:  make([]byte, 0, 4096),
            writeBuf: &bytes.Buffer{},
        }
    },
}

func getChannelBuffers() *ChannelBuffers {
    return channelBufferPool.Get().(*ChannelBuffers)
}

func putChannelBuffers(cb *ChannelBuffers) {
    if cap(cb.readBuf) <= 64*1024 {
        cb.readBuf = cb.readBuf[:0]
        cb.writeBuf.Reset()
        channelBufferPool.Put(cb)
    }
}
```

**Expected Impact:**
- 20-30% fewer allocations
- Better cache locality
- Reduced GC pressure

---

## Phase 11.2: Parsing Optimizations

### 11.2.1 Fast Path Method Dispatch
**Goal**: Eliminate repeated parsing and function call overhead

**Current Issue:**
```go
// Current: Multiple function calls
classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
switch classID {
    case 60:  // Basic class
        return s.processBasicMethod(conn, frame.Channel, methodID, frame.Payload[4:])
}
```

**Optimization:**
```go
// method_dispatch.go

// Pre-compute dispatch key (classID << 16 | methodID)
type DispatchKey uint32

func makeDispatchKey(classID, methodID uint16) DispatchKey {
    return DispatchKey(uint32(classID)<<16 | uint32(methodID))
}

// Hot path constants
const (
    KeyBasicPublish  = DispatchKey(0x003C0028) // 60.40
    KeyBasicDeliver  = DispatchKey(0x003C003C) // 60.60
    KeyBasicAck      = DispatchKey(0x003C0050) // 60.80
    KeyBasicConsume  = DispatchKey(0x003C0014) // 60.20
)

// Fast dispatch using map or switch
func (s *Server) dispatchMethodFast(conn *protocol.Connection, frame *protocol.Frame) error {
    // Parse once
    classID := uint16(frame.Payload[0])<<8 | uint16(frame.Payload[1])
    methodID := uint16(frame.Payload[2])<<8 | uint16(frame.Payload[3])
    key := makeDispatchKey(classID, methodID)

    // Hot path: inline most common operations
    switch key {
    case KeyBasicPublish:
        return s.handleBasicPublishInline(conn, frame)
    case KeyBasicAck:
        return s.handleBasicAckInline(conn, frame)
    case KeyBasicDeliver:
        return s.handleBasicDeliverInline(conn, frame)
    default:
        // Cold path: use regular dispatch
        return s.processMethodFrame(conn, frame)
    }
}
```

**Expected Impact:**
- 40-60% faster dispatch for hot paths
- Reduced function call overhead
- Better branch prediction

---

### 11.2.2 Method Parsing Cache
**Goal**: Cache parsed method structures

**Optimization:**
```go
// method_cache.go
type MethodCache struct {
    // Ring buffer of recently parsed methods
    entries [256]MethodCacheEntry
    index   uint8
}

type MethodCacheEntry struct {
    hash   uint64
    method interface{}
}

func (mc *MethodCache) Get(payload []byte) interface{} {
    hash := hashPayload(payload)
    idx := hash % 256

    if mc.entries[idx].hash == hash {
        return mc.entries[idx].method
    }
    return nil
}

func (mc *MethodCache) Put(payload []byte, method interface{}) {
    hash := hashPayload(payload)
    idx := hash % 256
    mc.entries[idx] = MethodCacheEntry{
        hash:   hash,
        method: method,
    }
}

// Use FNV-1a hash for speed
func hashPayload(data []byte) uint64 {
    // Fast hash implementation
}
```

**Expected Impact:**
- 30-50% faster for repeated patterns
- Useful for consumer deliveries
- Minimal memory overhead (2KB per connection)

---

### 11.2.3 SIMD-Optimized Parsing (Advanced)
**Goal**: Use CPU vector instructions for parsing

**Optimization:**
```go
// simd_parse.go
// Use github.com/klauspost/cpuid for CPU detection

//go:build amd64
func parseFrameHeaderSIMD(data []byte) (frameType byte, channel uint16, size uint32) {
    // Use SSE2/AVX2 instructions for parallel byte operations
    // Unroll loops, prefetch data
}

// Fallback to standard parsing on other architectures
```

**Expected Impact:**
- 2-3x faster frame header parsing
- Only on modern CPUs (x86_64 with AVX2)
- Complexity: High

---

## Phase 11.3: Command-Level Optimizations

### 11.3.1 Lock-Free Message Queue
**Goal**: Eliminate mutex contention in hot paths

**Current Issue:**
```go
// broker.go: Mutex contention
b.Mutex.Lock()
queue.Messages = append(queue.Messages, message)
b.Mutex.Unlock()
```

**Optimization:**
```go
// lockfree_queue.go
// Use github.com/golang-collections/go-datastructures/queue

type LockFreeQueue struct {
    queue *queue.Queue  // MPSC lock-free queue
}

func (q *LockFreeQueue) Enqueue(msg *protocol.Message) {
    q.queue.Put(msg)
}

func (q *LockFreeQueue) Dequeue() (*protocol.Message, bool) {
    items, err := q.queue.Get(1)
    if err != nil || len(items) == 0 {
        return nil, false
    }
    return items[0].(*protocol.Message), true
}
```

**Expected Impact:**
- 3-10x better concurrency
- Eliminates lock contention
- Better scaling with CPU cores

---

### 11.3.2 Message Batching
**Goal**: Process multiple messages together

**Optimization:**
```go
// message_batch.go
type MessageBatch struct {
    messages []*protocol.Message
    size     int
}

const MaxBatchSize = 100
const MaxBatchDelay = 1 * time.Millisecond

func (s *Server) batchPublish(conn *protocol.Connection, channel uint16) {
    batch := &MessageBatch{
        messages: make([]*protocol.Message, 0, MaxBatchSize),
    }

    timer := time.NewTimer(MaxBatchDelay)
    defer timer.Stop()

    for {
        select {
        case msg := <-pendingMessages:
            batch.messages = append(batch.messages, msg)
            if len(batch.messages) >= MaxBatchSize {
                s.processBatch(batch)
                batch.messages = batch.messages[:0]
            }
        case <-timer.C:
            if len(batch.messages) > 0 {
                s.processBatch(batch)
                batch.messages = batch.messages[:0]
            }
            timer.Reset(MaxBatchDelay)
        }
    }
}

func (s *Server) processBatch(batch *MessageBatch) {
    // Process all messages in one transaction
    // Write all frames in one buffered write
    // Reduces lock acquisitions
}
```

**Expected Impact:**
- 5-10x throughput for high-load scenarios
- Lower latency variance
- Better CPU cache utilization

---

### 11.3.3 Inline Fast Paths
**Goal**: Avoid function calls for common operations

**Optimization:**
```go
// inline_handlers.go

//go:inline
func (s *Server) handleBasicPublishInline(conn *protocol.Connection, frame *protocol.Frame) error {
    // Inline the entire publish path
    // No function calls, direct field access
    // Manual bounds checking elimination

    payload := frame.Payload[4:]

    // Parse publish method (inlined)
    exchange := readShortString(payload, 0)
    routingKey := readShortString(payload, len(exchange)+1)

    // Skip broker lookup for default exchange
    if exchange == "" {
        // Direct delivery to queue
        return s.deliverToQueueDirect(routingKey, frame.Channel)
    }

    // Regular path
    return s.handleBasicPublish(conn, frame)
}
```

**Expected Impact:**
- 20-30% faster for common paths
- Reduced instruction cache misses
- Better compiler optimizations

---

### 11.3.4 Consumer Delivery Pipeline
**Goal**: Optimize message delivery to consumers

**Optimization:**
```go
// consumer_pipeline.go

type ConsumerPipeline struct {
    consumers []*protocol.Consumer
    msgChan   chan *protocol.Message
    batchSize int
}

func (cp *ConsumerPipeline) Start() {
    go func() {
        batch := make([]*protocol.Message, 0, cp.batchSize)

        for {
            // Collect messages
            select {
            case msg := <-cp.msgChan:
                batch = append(batch, msg)
                if len(batch) >= cp.batchSize {
                    cp.deliverBatch(batch)
                    batch = batch[:0]
                }
            case <-time.After(1 * time.Millisecond):
                if len(batch) > 0 {
                    cp.deliverBatch(batch)
                    batch = batch[:0]
                }
            }
        }
    }()
}

func (cp *ConsumerPipeline) deliverBatch(batch []*protocol.Message) {
    // Round-robin or priority-based delivery
    // Batch frame writes
    // Reduce lock contention
}
```

**Expected Impact:**
- 3-5x consumer throughput
- Lower delivery latency
- Better fairness

---

## Phase 11.4: Storage I/O Optimizations

### 11.4.1 Write-Ahead Log (WAL)
**Goal**: Batch disk writes

**Optimization:**
```go
// wal.go
type WriteAheadLog struct {
    buffer    []byte
    offset    int
    file      *os.File
    syncChan  chan struct{}
}

func (wal *WriteAheadLog) Append(data []byte) error {
    // Buffer writes
    copy(wal.buffer[wal.offset:], data)
    wal.offset += len(data)

    if wal.offset >= len(wal.buffer)*3/4 {
        return wal.Sync()
    }
    return nil
}

func (wal *WriteAheadLog) Sync() error {
    // Batch write to disk
    _, err := wal.file.Write(wal.buffer[:wal.offset])
    if err != nil {
        return err
    }

    // fsync
    if err := wal.file.Sync(); err != nil {
        return err
    }

    wal.offset = 0
    return nil
}
```

**Expected Impact:**
- 10-100x write throughput
- Reduced fsync calls
- Better durability guarantees

---

### 11.4.2 Badger Optimization
**Goal**: Tune Badger for AMQP workloads

**Optimization:**
```go
// badger_tuning.go
func NewOptimizedBadgerDB(path string) (*badger.DB, error) {
    opts := badger.DefaultOptions(path).
        WithValueLogFileSize(256 * 1024 * 1024).  // 256MB value logs
        WithNumMemtables(3).                        // More memtables
        WithNumLevelZeroTables(5).                  // More L0 tables
        WithNumLevelZeroTablesStall(10).            // Higher stall threshold
        WithNumCompactors(4).                       // More compactors
        WithValueLogLoadingMode(options.FileIO).    // mmap for reads
        WithMaxCacheSize(512 * 1024 * 1024)         // 512MB cache

    return badger.Open(opts)
}
```

**Expected Impact:**
- 2-5x write throughput
- 50-70% read latency reduction
- Better compaction performance

---

## Phase 11.5: Concurrency Optimizations

### 11.5.1 Per-Channel Goroutines
**Goal**: Parallelize channel processing

**Current**: Single goroutine per connection
**Optimization**: One goroutine per channel

```go
// channel_workers.go
type ChannelWorker struct {
    channelID uint16
    frameChan chan *protocol.Frame
}

func (s *Server) handleConnectionParallel(conn *protocol.Connection) {
    // Frame distributor
    go func() {
        for {
            frame, err := ReadFrameOptimized(conn.Conn)
            if err != nil {
                return
            }

            // Distribute to channel worker
            worker := conn.ChannelWorkers[frame.Channel]
            worker.frameChan <- frame
        }
    }()

    // Workers for each channel
    for _, worker := range conn.ChannelWorkers {
        go worker.processFrames(s, conn)
    }
}
```

**Expected Impact:**
- Linear scaling with CPU cores
- Better cache utilization
- Reduced head-of-line blocking

---

### 11.5.2 Lock-Free Routing Table
**Goal**: Concurrent reads for exchange routing

**Optimization:**
```go
// lockfree_routing.go
import "sync/atomic"

type LockFreeRoutingTable struct {
    // Use atomic.Value for lock-free reads
    table atomic.Value  // map[string]*Exchange
}

func (rt *LockFreeRoutingTable) Get(name string) *Exchange {
    table := rt.table.Load().(map[string]*Exchange)
    return table[name]
}

func (rt *LockFreeRoutingTable) Update(name string, exchange *Exchange) {
    old := rt.table.Load().(map[string]*Exchange)
    new := make(map[string]*Exchange, len(old)+1)
    for k, v := range old {
        new[k] = v
    }
    new[name] = exchange
    rt.table.Store(new)
}
```

**Expected Impact:**
- Zero contention for reads
- 10-100x better read throughput
- Slower writes (acceptable trade-off)

---

## Phase 11.6: Memory Optimizations

### 11.6.1 Object Pooling
**Goal**: Reuse message objects

**Optimization:**
```go
// object_pools.go
var messagePool = sync.Pool{
    New: func() interface{} {
        return &protocol.Message{
            Body: make([]byte, 0, 1024),
        }
    },
}

func getMessage() *protocol.Message {
    return messagePool.Get().(*protocol.Message)
}

func putMessage(msg *protocol.Message) {
    msg.Body = msg.Body[:0]
    msg.Exchange = ""
    msg.RoutingKey = ""
    messagePool.Put(msg)
}
```

**Expected Impact:**
- 40-60% fewer allocations
- Lower GC overhead
- Better memory locality

---

### 11.6.2 Arena Allocators
**Goal**: Batch allocations

**Optimization:**
```go
// arena.go
type Arena struct {
    buf    []byte
    offset int
}

func (a *Arena) Alloc(size int) []byte {
    if a.offset+size > len(a.buf) {
        a.buf = make([]byte, max(size, 64*1024))
        a.offset = 0
    }

    result := a.buf[a.offset : a.offset+size]
    a.offset += size
    return result
}

// Use per-goroutine arenas
var arenaPool = sync.Pool{
    New: func() interface{} {
        return &Arena{
            buf: make([]byte, 64*1024),
        }
    },
}
```

**Expected Impact:**
- 3-5x faster allocations
- Better cache utilization
- Reduced GC pressure

---

## Phase 11.7: Monitoring and Profiling

### 11.7.1 Performance Metrics
**Goal**: Measure optimization impact

**Metrics to Track:**
- Frame read/write latency (p50, p95, p99)
- Method dispatch latency
- Lock contention (mutex wait time)
- CPU usage per subsystem
- Memory allocations per operation
- GC pause times
- Throughput (messages/sec, MB/sec)

**Implementation:**
```go
// metrics_detailed.go
type PerformanceMetrics struct {
    FrameReadLatency    metrics.Histogram
    FrameWriteLatency   metrics.Histogram
    DispatchLatency     metrics.Histogram
    LockWaitTime        metrics.Counter
    Allocations         metrics.Counter
    Throughput          metrics.Meter
}
```

---

### 11.7.2 Continuous Benchmarking
**Goal**: Track performance regressions

**Setup:**
```bash
# benchstat for comparing results
go test -bench=. -benchmem -count=10 > new.txt
benchstat baseline.txt new.txt

# pprof for profiling
go test -bench=BenchmarkHighLoad -cpuprofile=cpu.prof -memprofile=mem.prof
go tool pprof -http=:8080 cpu.prof
```

---

## Implementation Priority

### Phase 1 (High Impact, Low Complexity):
1. **11.1.1**: TCP socket tuning (1 day)
2. **11.1.2**: Buffered I/O (2 days)
3. **11.2.1**: Fast path method dispatch (3 days)
4. **11.3.1**: Lock-free queue (3 days)

**Expected Gain**: 3-5x throughput improvement

### Phase 2 (High Impact, Medium Complexity):
1. **11.3.2**: Message batching (3 days)
2. **11.4.2**: Badger optimization (2 days)
3. **11.6.1**: Object pooling (2 days)
4. **11.5.1**: Per-channel goroutines (4 days)

**Expected Gain**: Additional 2-3x improvement (cumulative 6-15x)

### Phase 3 (Medium Impact, High Complexity):
1. **11.1.3**: Zero-copy I/O (5 days)
2. **11.4.1**: Write-ahead log (5 days)
3. **11.5.2**: Lock-free routing table (3 days)

**Expected Gain**: Additional 1.5-2x improvement (cumulative 9-30x)

### Phase 4 (Advanced):
1. **11.2.3**: SIMD parsing (7 days)
2. **11.6.2**: Arena allocators (4 days)
3. **11.3.3**: Inline fast paths (5 days)

**Expected Gain**: Additional 1.2-1.5x improvement (cumulative 10-45x)

---

## Success Criteria

**Performance Targets:**
- [ ] Message throughput: 1M+ messages/sec (single node, memory storage)
- [ ] Persistent throughput: 100K+ messages/sec (Badger storage)
- [ ] Latency p99: < 10ms (end-to-end)
- [ ] CPU efficiency: < 10% CPU at 100K msg/sec
- [ ] Memory usage: < 500MB for 1M queued messages
- [ ] Zero data loss under load

**Quality Targets:**
- [ ] All existing tests pass
- [ ] New benchmarks added for each optimization
- [ ] Performance regression tests in CI
- [ ] Documentation updated

---

## Risk Mitigation

**Risks:**
1. **Complexity**: Some optimizations add significant complexity
   - Mitigation: Implement behind feature flags, fallback to standard path
2. **Compatibility**: Platform-specific optimizations may break portability
   - Mitigation: Build tags, fallback implementations
3. **Correctness**: Lock-free algorithms can introduce subtle bugs
   - Mitigation: Extensive testing, fuzzing, race detector
4. **Maintenance**: Optimized code harder to understand
   - Mitigation: Comprehensive documentation, benchmarks

---

## Benchmarking Plan

**Benchmark Scenarios:**
1. **High-throughput publishing**: 1M small messages
2. **Consumer throughput**: 100K consumers
3. **Mixed workload**: 50% publish, 50% consume
4. **Large messages**: 1MB payloads
5. **High concurrency**: 10K concurrent connections
6. **Persistence**: Durable messages with Badger
7. **Network latency**: Simulated WAN conditions

**Comparison Baselines:**
- RabbitMQ (Erlang)
- ActiveMQ (Java)
- Apache Pulsar (Java)
- NATS (Go)

---

## Estimated Timeline

**Total Duration**: 12-16 weeks

- **Phase 1**: Weeks 1-2 (Quick wins)
- **Phase 2**: Weeks 3-5 (Major improvements)
- **Phase 3**: Weeks 6-9 (Advanced optimizations)
- **Phase 4**: Weeks 10-12 (Cutting-edge features)
- **Testing & Validation**: Weeks 13-14
- **Documentation & Cleanup**: Weeks 15-16

---

## References

1. **Socket Programming**:
   - Unix Network Programming (Stevens)
   - TCP/IP Illustrated (Stevens)

2. **Lock-Free Programming**:
   - The Art of Multiprocessor Programming (Herlihy)
   - Lock-Free Data Structures (Michael & Scott)

3. **Performance Optimization**:
   - High Performance Go (Dave Cheney)
   - Systems Performance (Brendan Gregg)

4. **AMQP Performance**:
   - RabbitMQ Internals
   - Kafka: The Definitive Guide

---

## Conclusion

This comprehensive plan provides a roadmap to achieve 10-100x performance improvements across multiple dimensions. The phased approach allows for incremental progress with measurable results at each stage.

**Key Innovations:**
- Zero-copy I/O where possible
- Lock-free data structures for hot paths
- Intelligent batching and buffering
- Per-channel parallelism
- Optimized storage I/O

**Expected Final Performance:**
- **1M+ msg/sec** throughput (memory storage)
- **100K+ msg/sec** throughput (persistent storage)
- **< 10ms** p99 latency
- **< 100MB** memory for 100K messages
- **10x-100x** better than current baseline

This positions the AMQP server as a high-performance, production-ready message broker competitive with industry leaders.
