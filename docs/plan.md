# Plan: Implement AMQP 0.9.1 Server in Go

## Goal
Create a Go package `github.com/maxpert/amqp-go` that implements an AMQP 0.9.1 server based on the specification: https://www.rabbitmq.com/resources/specs/amqp0-9-1.extended.xml

## Current Status (Updated: 2026-06-23)
**Phase 15 - TLS + Authorization: IN PROGRESS** 🚧

### Phase 15 - Commit B6: Documentation (COMPLETE) ✅
- ✅ `docs/AUTHORIZATION.md`: permission model, operation→permission mapping, auth file format, loopback restriction, backward compatibility, configuration, error handling
- ✅ `config.sample.yaml`: authorizationenabled comment with link to docs
- ✅ `README.md`: link to authorization guide

### Phase 15 - Commit B5: Authorization Integration Test (COMPLETE) ✅
- ✅ **10 end-to-end integration tests** with real `amqp091-go` client against live server
  - Admin can declare exchanges/queues (configure=.*)
  - Producer cannot declare (configure=^$) but can publish (write=.*)
  - Consumer cannot publish (write=^$) — channel.Close from server via NotifyClose
  - Consumer can consume (read=.*)
  - Restricted user can access own resources (^app1-.*), cannot access others (^app2-.*)
  - Guest loopback allowed from localhost, refused from non-loopback
  - Reserved exchange name (amq.custom) refused for non-passive declare
- ✅ Tests use 5 users with different permission triples: admin, producer, consumer, restricted, guest

### Phase 15 - Commit B4: Per-Operation Authorization Checks (COMPLETE) ✅
- ✅ **Authorize helper** (`server/authz.go`): checks `AuthorizationEnabled`, `conn.User`, `s.Authenticator`; fail-closed on nil
- ✅ **sendChannelClose**: sends `channel.close` on correct channel ID (not channel 0); marks channel closed
- ✅ **authzChannelError**: sends 403 (access-refused) + returns error to stop handler processing
- ✅ **All 11 AMQP operations checked** per RabbitMQ permission table:
  - exchange.declare → configure on exchange
  - exchange.delete → configure on exchange
  - exchange.unbind → write on destination + read on source
  - queue.declare → configure on queue
  - queue.delete → configure on queue
  - queue.bind → write on queue + read on exchange
  - queue.unbind → write on queue + read on exchange
  - basic.publish → write on exchange
  - basic.consume → read on queue
  - basic.get → read on queue
- ✅ **Reserved `amq.` name enforcement** (spec "reserved" rule): non-passive declare of `amq.*` → 403; passive allowed; server-generated `amq.gen.*` exempt
- ✅ **22 tests pass** with `-race`: all operations (allowed + refused), reserved names (exchange + queue, non-passive refused + passive allowed), auth-disabled
- ✅ **Code review fixes**: channel.close on correct channel (CRITICAL), queue.unbind + exchange.unbind authz added (MAJOR), nil Authenticator check (MINOR), passive queue reserved-name test (MINOR)

### Phase 15 - Commit B3: Vhost Access Check + Loopback Restriction (COMPLETE) ✅
- ✅ **Vhost permission check at connection.open** (AMQP 0.9.1 spec "security" rule)
  - When `AuthorizationEnabled`: checks user's `VHostPermissions` for the requested vhost
  - Fail-closed: refuses connection if `conn.User` is nil when authorization is enabled (MAJOR-2 fix)
  - Returns 402 (invalid-path) on vhost access denied
- ✅ **Loopback restriction** (RabbitMQ guest restriction)
  - Users with `LoopbackOnly=true` can only connect from localhost
  - `isLoopbackConn()` checks `RemoteAddr()` is TCP loopback; denies non-TCP conservatively
  - Returns 403 (access-refused) on loopback violation
  - Enforced both with and without authorization enabled
- ✅ **`conn.User` field** added to `protocol.Connection` (as `interface{}` to avoid import cycle)
  - Set during `handleConnectionStartOK` after successful authentication
  - Documented as set-once, immutable after handshake
- ✅ **Single `RLock` for both vhost + loopback checks** (MAJOR-1 fix: prevents data race with concurrent `RefreshUser`)
- ✅ **Named error constants** (`amqperrors.InvalidPath`, `amqperrors.AccessRefused`) instead of magic numbers
- ✅ **8 tests pass** with `-race`: allowed, refused, auth-disabled, nil-user fail-closed, empty-permissions, loopback allowed/refused, non-guest non-loopback

### Phase 15 - Commit B1: Permission Model Redesign (COMPLETE) ✅
- ✅ **Redesigned Permission/Operation types** in `interfaces/server.go` to RabbitMQ's configure/write/read regex triple model
  - `Permission` struct: `Configure`, `Write`, `Read` regex pattern fields (was: `Resource`/`Action`/`Pattern`)
  - `Permission.Matches(action, resourceName)` method for regex matching with safe failure
  - `Permission` doc: unanchored patterns, `^$` empty-string edge case documented
  - Compiled regex caching via `sync.Map` for hot-path performance (no per-message recompilation)
  - `Operation` struct: `Action` (OperationAction), `ResourceType` (ResourceType), `Resource`, `VHost`
  - `OperationAction` enum: `ActionConfigure`, `ActionWrite`, `ActionRead`
  - `ResourceType` enum: `ResourceExchange`, `ResourceQueue`, `ResourceVHost`
  - `VHostPermission` struct: ties a permission triple to a specific vhost
  - `User` struct: `VHostPermissions []VHostPermission` replaces `Permissions []Permission`; added `Tags`, `LoopbackOnly`; `sync.RWMutex` for concurrent Authorize/RefreshUser safety
  - `NormalizeExchangeName()` + `DefaultExchangeName` constant: maps empty default exchange to "amq.default" for permission checks
- ✅ **Implemented `FileAuthenticator.Authorize()`** in `auth/file_auth.go` (was stub `return nil`)
  - Finds user's VHostPermission for the operation's vhost; refuses if none exists
  - Normalizes default exchange name (empty → "amq.default") for permission checks
  - Matches resource name against the appropriate regex pattern (configure/write/read)
  - Returns typed `*amqperrors.AuthError` (code 403) on refusal — `errors.IsAccessRefused()` works
  - Holds `user.RLock()` during check to prevent concurrent RefreshUser data races
- ✅ **Backward compatibility**: old `permissions` field faithfully migrated — each legacy Action mapped to corresponding permission field; missing actions default to `^$` (deny); warning logged
- ✅ **Updated `auth/anonymous.go`**: ANONYMOUS mechanism now creates guest user with full VHostPermissions on "/" + `Tags: ["administrator"]`
- ✅ **Updated `auth/auth_test.go`**: MockAuthenticator uses new VHostPermissions format
- ✅ **Slices cloned** in `GetUser`/`Authenticate` via `slices.Clone()` to avoid shared backing arrays
- ✅ **44 auth tests pass** with `-race` (9 Permission.Matches, 2 NormalizeExchangeName, 33 Authorize tests covering all AMQP 0.9.1 operations, typed errors, concurrent RefreshUser, faithful legacy migration)

### Phase 15 - Commit B1.1: Code Review Fixes + Spec Compliance (COMPLETE) ✅
- ✅ **Code review (2 rounds, 0 remaining issues)**: Fixed 4 MAJOR + 6 MINOR issues from automated review
  - M1: Typed `*AuthError` (403) instead of `fmt.Errorf`
  - M2: Compiled regex cache (`sync.Map`) for hot-path performance
  - M3: `sync.RWMutex` on `User` for concurrent Authorize/RefreshUser safety
  - M4: Faithful legacy migration (not allow-all escalation)
  - m1-m6: LoopbackOnly wired through, unanchored regex documented, slices cloned, tests fixed
- ✅ **Spec compliance: Server-generated queue names** (AMQP 0.9.1 "default-name" rule)
  - `protocol.GenerateQueueName()`: generates `amq.gen.<random>` names (RabbitMQ convention)
  - `handleQueueDeclare`: generates name when empty, sets `Channel.CurrentQueue`
  - `protocol.Channel.CurrentQueue` field: tracks last declared queue per channel
- ✅ **Spec compliance: "Current queue" resolution** (AMQP 0.9.1 "queue-known" rule)
  - `resolveQueueName()` helper: resolves empty queue name to `Channel.CurrentQueue`
  - Applied to `handleQueueBind`, `handleQueueUnbind`, `handleQueueDelete`
  - Returns error (spec: 502 syntax error) when no queue was previously declared on channel
- ✅ **13 new tests** (7 protocol/queue_name_test.go + 6 server/queue_name_handler_test.go)
  - GenerateQueueName: prefix, uniqueness, valid chars, length
  - Channel.CurrentQueue: default, set after named declare, set after generated declare
  - Handler: empty name generates server name, sets CurrentQueue, resolves for bind/delete, errors when no current queue
- ✅ **Full test suite passes** (all packages, `-race`, 0 failures)

**Phase 14 - Log Compaction + Bug Fixes: COMPLETE** ✅

### Phase 14 - Post-Review Bug Fixes (Code Review Pass):
- ✅ **Fix: `readMessageFromFile` body deserialization** — Sequential scan path (WAL fallback when offset not in index) incorrectly returned body bytes including the 4-byte bodyLen prefix. Fixed by reading `bodyLen` field before slicing body, matching `serializeMessage` format. Test: `TestWAL_SequentialScan_CorrectBody`
- ✅ **Fix: `openNextSegment` deadlock** — `writeMessage` held `qs.mutex` while calling `openNextSegment`, which also tried to acquire `qs.mutex` — a non-reentrant deadlock. Split into `openNextSegment()` (acquires mutex) and `openNextSegmentLocked()` (precondition: caller holds mutex). `writeMessage` now calls `openNextSegmentLocked()`. Test: `TestSegment_RolloverDuringWrite`
- ✅ **Fix: `writeIndexToDisk` ignores write errors** — All `binary.Write` calls now check error return values and propagate failures, preventing silent index corruption on disk-full or I/O errors
- ✅ **Fix: `checkpointLoop` interval race** — Goroutine started before `SetSegmentManager()` was called, causing it to read `segmentMgr == nil` and always use the 5-minute default. Added `CheckpointInterval` to `WALConfig` (mapped from `EngineConfig.SegmentCheckpointIntervalMS`); `checkpointLoop` now uses `qw.cfg.CheckpointInterval` directly, removing the dependency on segmentMgr at goroutine start
- **58 tests pass**, 0 data races (verified with `-race`)

### Phase 14 - Log Compaction (Storage Reliability):
- ✅ **Commit 1: Fix roaring64 bitmap and WAL offset tracking bug**
  - Replaced `roaring.Bitmap` → `roaring64.Bitmap` in WAL and segment managers (removes uint32 truncation at ~4.29B messages)
  - Fixed `tryDeleteOldFiles()` offset iteration bug: now tracks actual message offsets per file via `roaring64.Bitmap` instead of iterating [minOffset, maxOffset] range
  - Removed dead `ackedMsgs`/`minOffset`/`maxOffset` fields from `walFileInfo`, replaced with `offsets *roaring64.Bitmap`
  - Added `currentFileOffsets` tracking in `QueueWAL` to capture offsets during `flushBatch()` and pass to `rollFile()`
  - Updated `readMessageSequential()` to use bitmap `Contains()` instead of range check
  - 5 new tests in `wal_compaction_test.go`: large offsets, sparse offsets, partial ACK, false positive regression, large offset recovery
- ✅ **Commit 2: Wire EngineConfig to storage layer constructors**
  - Added `WALConfig` struct with `DefaultWALConfig()` and `NewWALManagerWithConfig()` constructor
  - Added `SegmentConfig` struct with `DefaultSegmentConfig()` and `NewSegmentManagerWithConfig()` constructor
  - Added `WALConfigFromEngine()` and `SegmentConfigFromEngine()` to map `EngineConfig` → storage configs
  - Added `NewDisruptorStorageWithEngineConfig()` that passes config through to WAL/Segment constructors
  - Updated `server/builder.go` to call `NewDisruptorStorageWithEngineConfig()` with `EngineConfig`
  - All hardcoded constants (batch size, timeout, file size, channel buffer, cleanup/compaction/checkpoint intervals) now respect config
  - 5 new tests: custom batch size, custom compaction threshold, engine→WAL mapping, engine→segment mapping, default fallback
- ✅ **Commit 3: Fix segment deletedCount tracking**
  - Added `acknowledgeInSegment()` method that finds the segment containing an offset and increments its `deletedCount`
  - Called from `Acknowledge()` after updating the ACK bitmap
  - Checks both current segment (via currentIndex) and sealed segments (via segment.index)
  - Fixed `sealSegment()` to copy currentIndex entries to segment.index for sealed-segment reads
  - Fixed `sealSegment()` to reopen file as read-only for sealed-segment ReadAt support
  - Fixed pre-existing CRC verification bug in `readSegmentMessageAt()` — was verifying CRC over wrong data slice
  - 4 new tests: deletedCount increment, compaction triggers, compaction preserves unacked, regression test
- ✅ **Commit 4: Implement WAL → Segment checkpointing**
  - Implemented `performCheckpoint()`: scans old WAL files, filters out ACKed messages, writes unACKed to segments, deletes WAL file, cleans offset index
  - Added `CheckpointBatch()` on SegmentManager for bulk message writes during checkpoint
  - Only operates on old/rolled files — never touches the active WAL file
  - Updated `checkpointLoop` to use configurable interval from SegmentConfig
  - 5 new tests: moves to segments, deletes old files, skips ACKed, active file untouched, offset index cleanup
- ✅ **Commit 5: Add time-based WAL retention**
  - Added `createdAt time.Time` field to `walFileInfo` for tracking file age
  - Added retention check in `tryDeleteOldFiles()`: files older than `RetentionPeriod` are force-deleted
  - Added `forceCheckpointFile()` method: best-effort checkpoint of unACKed messages to segments before deletion
  - Fixed segment `openNextSegment()` to use `O_RDWR` instead of `O_WRONLY` so checkpointed messages are readable from current segment
  - Retention disabled by default (`RetentionPeriod: 0`); enabled via config
  - 4 new tests: expired file deletion, recent file preservation, checkpoint-before-delete, disabled-when-zero
- ✅ **Commit 6: Integration test and plan update**
  - Full lifecycle integration test (`log_compaction_integration_test.go`): write → WAL roll → checkpoint → segment → compaction → recovery
  - `TestLogCompaction_FullLifecycle`: 300 durable messages across 3 queues, ACK 80%, checkpoint, verify unACKed readable, segment compaction, simulate restart
  - `TestLogCompaction_RetentionWithCheckpoint`: time-based retention force-checkpoints unACKed messages to segments before deletion
  - `TestLogCompaction_LargeOffsetLifecycle`: uint64 offsets > uint32 max through full WAL → segment pipeline
  - `TestLogCompaction_TierFallback`: validates Ring Buffer → WAL → Segment three-tier read fallback

**Phase 13 - Memory Allocation Optimizations: COMPLETE** ✅

### Phase 13 - Memory Allocation Optimizations:
- ✅ **Buffer Pool Integration**: Eliminated allocations in frame serialization hot path
  - Implemented `MarshalBinaryPooled()` using tiered buffer pools from Phase 12
  - Updated `sendBasicDeliver` to use pooled buffers for method/header/body frames
  - Automatic buffer lifecycle management with defer statements
  - Zero-allocation frame encoding for repeated operations
- ✅ **ReadFrame Optimization**: Switched to pooled header buffers across hot paths
  - Replaced `ReadFrame` with `ReadFrameOptimized` in server connection handling
  - Connection handshake (lines 194, 228, 240): 3 hot path optimizations
  - Main frame reading loop (line 298): Continuous optimization in reader goroutine
  - Reduces header buffer allocations by reusing pooled 7-byte buffers
- ✅ **Timer Reuse**: Eliminated repeated timer allocations in consumer delivery loop
  - Single timer created outside loop with `time.NewTimer()`
  - Proper Reset() pattern with channel draining before reuse
  - Eliminated 450MB of timer allocations in hot loop
  - Maintains 500μs select timeout without allocation overhead
- ✅ **Allocation Reduction Achieved**: 26.5% total allocation savings
  - **Before**: 52.77 GB total allocations, 25.28GB in sendBasicDeliver
  - **After**: 38.79 GB total allocations, 16.68GB in sendBasicDeliver
  - **Savings**: 13.98 GB eliminated (26.5% reduction overall)
  - **sendBasicDeliver**: 8.6 GB saved (34% reduction from 25.28GB → 16.68GB)
- ✅ **Performance Maintained**: Zero throughput regression
  - Throughput: 130-140k msg/s sustained (same as pre-optimization)
  - Peak memory: 459 MB (3% improvement from 473MB)
  - All tests passing: Integration + Protocol test suites

**Technical Implementation:**
- **MarshalBinaryPooled** (protocol/frame_optimized.go:118-157):
  ```go
  func (f *Frame) MarshalBinaryPooled() ([]byte, error)
  // Usage: data, err := frame.MarshalBinaryPooled()
  //        defer PutBufferForSize(&data)
  ```
  - Uses `GetBufferForSize()` to select appropriate pool (1KB/64KB/131KB)
  - Caller responsible for returning buffer with `PutBufferForSize()`
  - Automatic fallback for oversized frames (rejected by pool on return)

- **Hot Path Updates** (server/basic_handlers.go:705-763):
  ```go
  methodFrameData, _ := frame.MarshalBinaryPooled()
  defer protocol.PutBufferForSize(&methodFrameData)

  headerFrameData, _ := headerFrame.MarshalBinaryPooled()
  defer protocol.PutBufferForSize(&headerFrameData)

  for _, bodyFrame := range bodyFrames {
      bodyFrameData, _ := bodyFrame.MarshalBinaryPooled()
      allFrames = append(allFrames, bodyFrameData...)
      protocol.PutBufferForSize(&bodyFrameData) // Immediate return
  }
  ```

- **ReadFrame Optimization** (server/server.go:194, 228, 240, 298):
  ```go
  // Before: frame, err := protocol.ReadFrame(conn.Conn)
  // After:  frame, err := protocol.ReadFrameOptimized(conn.Conn)
  ```
  - Uses pooled 7-byte header buffer from `frameHeaderPool`
  - Reduces allocations from 4 → 3 per read operation

- **Timer Reuse Pattern** (server/consumer_delivery.go:32-82):
  ```go
  timeout := time.NewTimer(selectTimeout)
  defer timeout.Stop()

  for {
      // Reset timer for reuse
      if !timeout.Stop() {
          select { case <-timeout.C: default: }
      }
      timeout.Reset(selectTimeout)

      // Use timeout.C in reflect.Select()...
  }
  ```

**Profiling Results Comparison:**

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Allocations | 52.77 GB | 38.79 GB | -13.98 GB (-26.5%) |
| sendBasicDeliver | 25.28 GB (47.92%) | 16.68 GB (43.00%) | -8.6 GB (-34%) |
| ReadFrame headers | ~6.5 GB | 5.41 GB | ~1.09 GB (-16.8%) |
| Timer allocations | 450 MB | Eliminated | -450 MB (-100%) |
| Peak Memory | 473 MB | 459 MB | -14 MB (-3%) |
| Throughput | 130-140k msg/s | 130-140k msg/s | No regression ✅ |

**Code Locations:**
- `protocol/frame_optimized.go:118-157`: MarshalBinaryPooled implementation
- `protocol/buffer_pool.go:103-202`: Tiered buffer pools (from Phase 12)
- `server/basic_handlers.go:705-763`: Pooled serialization in sendBasicDeliver
- `server/server.go:194,228,240,298`: ReadFrameOptimized usage
- `server/consumer_delivery.go:32-82`: Timer reuse pattern

**Remaining Optimization Opportunities:**
- Message body pooling for publish operations (~4.6GB in processBodyFrame)
- Content header serialization pooling (~2.93GB in processCompleteMessage)
- Method serialization pooling (~1.87GB in handleBasicPublish)
- Further allocation reduction potential: ~9.4GB (24% additional savings possible)

### Phase 12 - AMQP Frame Fragmentation:
**Status: COMPLETE** ✅
- ✅ **AMQP 0.9.1 Spec Compliance**: Implemented proper body frame fragmentation
  - Large message bodies now split into multiple frames respecting maxFrameSize (128KB)
  - Fixes protocol violation for messages > 128KB (previously sent in single oversized frame)
  - Supports full 16MB message size as configured
- ✅ **FragmentBodyIntoFrames Helper**: New protocol function for frame splitting
  - Calculates optimal frame count based on body size and maxFrameSize
  - Pre-allocates slice with correct capacity (zero reallocation)
  - Handles edge cases: empty bodies, exact maxFrameSize, oversized messages
  - Respects 8-byte frame overhead per fragment
- ✅ **Tiered Buffer Pools**: Created sync.Pool infrastructure for future optimizations
  - Frame serialization pool (1KB - method/header frames)
  - Medium body pool (64KB - small messages)
  - Large frame pool (131KB - maxFrameSize chunks)
  - Get/Put helpers with automatic size-based pool selection
  - 64KB cap limit prevents memory waste from oversized buffers
- ✅ **Atomic Multi-Frame Transmission**: Maintained single-write semantics
  - Pre-calculates total size for all fragments
  - Single buffer allocation for combined write
  - Method frame + Header frame + N body fragments sent atomically
  - No partial message transmission
- ✅ **Zero Performance Regression**: Benchmarked with 20 connections/20 producers
  - Before: 135-140k msg/s sustained
  - After: 130-140k msg/s sustained (equivalent performance)
  - Fragmentation logic has zero overhead for small messages (<128KB)
  - 99% of workload unaffected (typical message size: 1-64KB)
- ✅ **All Tests Passing**: Full test suite passes with no regressions
  - Integration tests: 44.2s
  - Protocol tests: 0.7s
  - All existing functionality preserved

**Technical Details:**
- **Frame Fragmentation Logic**: `FragmentBodyIntoFrames(channelID, body, maxFrameSize)`
  - For 16MB message with 128KB maxFrameSize: creates ~122 fragments
  - Each fragment ≤ (maxFrameSize - 8 bytes) for frame headers
  - Empty bodies return nil (method + header frames only)
- **Buffer Pool Architecture**:
  - Tiered pools prevent contention (small/medium/large)
  - Automatic pool selection based on buffer capacity
  - Returns oversized buffers to GC (> cap limit)
  - Ready for future MarshalBinary optimization
- **Code Locations**:
  - `protocol/methods.go:2431-2470`: FragmentBodyIntoFrames implementation
  - `protocol/buffer_pool.go:103-202`: Tiered buffer pools
  - `server/basic_handlers.go:718-757`: sendBasicDeliver with fragmentation

**Performance Impact:**
- Small messages (< 128KB): Zero overhead
- Large messages (> 128KB): Minimal overhead (frame header per 128KB chunk)
- Memory: Same peak RSS (473MB)
- Throughput: Equivalent to pre-fragmentation
- Compliance: Now AMQP 0.9.1 spec compliant for all message sizes

**Next Optimization Opportunities (deferred):**
- Buffer pooling in MarshalBinary (25GB potential savings)
- ReadFrame header buffer pooling (5.58GB savings)
- Timer creation optimization (450MB savings)

### Phase 11 - Consumer Flow Control Refactor:
**Status: COMPLETE** ✅
- ✅ **Semaphore-Based Prefetch Control**: Replaced atomic counters with `golang.org/x/sync/semaphore.Weighted`
  - Blocks consumer goroutines when at prefetch limit (eliminates CPU spinning)
  - Acquire permit before pulling from available channel (guarantees capacity)
  - Release permit on ACK/NACK/Reject (automatic backpressure)
- ✅ **Three-Stage Pipeline Architecture**:
  - Stage 1: Acquire semaphore permit (blocks if at limit)
  - Stage 2: Pull message ID from available channel
  - Stage 3: Deliver with blocking send (TCP backpressure)
- ✅ **FIFO Message Ordering**: Messages never put back in normal operation
  - Put-backs only on shutdown/cancellation (rare events)
  - Preserves strict message ordering under normal load
- ✅ **Blocking Send to Consumer.Messages**: Changed from non-blocking to blocking
  - Provides natural TCP backpressure when consumer is slow
  - No more tight loops retrying delivery
- ✅ **Fixed Consumer Buffer Size**: Changed from dynamic (1.5x prefetch) to fixed (100)
  - Separates concerns: semaphore = prefetch limit, channel = TCP buffer
  - Reduces memory usage and complexity
- ✅ **Prefetch Capacity Limits**:
  - Limited prefetch: Use exact prefetch count
  - Unlimited prefetch (0): Cap at 2000 (RabbitMQ quorum queue limit)
- ✅ **Clean Shutdown**: Proper semaphore cancellation before stopCh
  - Cancel context first to unblock waiting goroutines
  - Then close stopCh for cleanup
- ✅ **Zero CPU Spinning**: Two scenarios fixed:
  - Prefetch limit reached: Goroutine blocks on semaphore acquisition
  - Consumer channel full: Goroutine blocks on channel send
- ✅ **All Tests Passing**: Full test suite passes with no regressions
  - 43.1s integration tests
  - 25.5s storage tests
  - All broker tests passing

**Performance Improvements:**
- Eliminates CPU pegging under bad connections/slow consumers
- Goroutines sleep efficiently instead of spinning
- Memory usage reduced (fixed 100-size buffers vs dynamic 1.5x)
- Cleaner separation of flow control (semaphore) and buffering (channel)

**Code Changes:**
- `broker/storage_broker.go`: ConsumerState with semaphore, rewritten poll loop
- `server/basic_handlers.go`: Fixed consumer buffer size
- Removed unused `tryDeliverNext` function
- Updated all ACK handlers to release semaphore permits

### Phase 10 - Performance Optimization:
**Status: COMPLETE** ✅

### Phase 9 - Protocol Testing Enhancement:
- ✅ **Protocol Test Coverage**: Increased from 2.5% to 50.7% (20x improvement!)
- ✅ **Frame Testing**: Comprehensive tests for all 4 frame types (Method, Header, Body, Heartbeat)
- ✅ **Method Serialization Tests**: 36 AMQP method tests across 6 classes
  - Connection class: 8 methods tested (Start, StartOK, Tune, TuneOK, Open, OpenOK, Close, CloseOK)
  - Channel class: 4 methods tested (Open, OpenOK, Close, CloseOK)
  - Exchange class: 4 methods tested (Declare, DeclareOK, Delete, DeleteOK)
  - Queue class: 6 methods tested (Declare, DeclareOK, Bind, BindOK, Delete, DeleteOK)
  - Basic class: 14 methods tested (Qos, QosOK, Consume, ConsumeOK, Cancel, CancelOK, Publish, Deliver, Get, GetOK, GetEmpty, Ack, Reject, Nack)
  - Transaction class: 6 methods tested (Select, SelectOK, Commit, CommitOK, Rollback, RollbackOK)
- ✅ **Field Table Testing**: Multiple data types (strings, numbers, booleans, mixed)
- ✅ **String Encoding**: Edge cases including empty, unicode, and special characters
- ✅ **Content Header Testing**: All 14 AMQP basic properties tested with round-trip validation
- ✅ **AMQP Spec Compliance**: Frame format, protocol header, string encoding, field tables validated
- ✅ **Fuzz Testing**: 8 comprehensive fuzz tests with 1.6M+ executions, zero crashes
  - Frame marshaling/unmarshaling
  - ReadFrame
  - Field table decoding
  - Content header parsing
  - Method deserialization
  - String encoding/decoding
  - Round-trip validation
- ✅ **RabbitMQ Interoperability Tests**: 9 comprehensive integration tests with real RabbitMQ
  - Protocol header exchange
  - Full connection handshake (Start, StartOK, Tune, TuneOK, Open, OpenOK)
  - Channel operations
  - Queue and exchange declaration
  - Message publishing with content headers
  - Message consumption (Basic.Get)
  - Heartbeat frames
  - Multiple concurrent channels
  - RabbitMQ docker-compose setup for interoperability testing
  - Comprehensive documentation (RABBITMQ_TESTS.md)
  - Automated test runner script (run-rabbitmq-tests.sh)
- ✅ **Parser Functions**: Added ParseConnectionStart and ParseConnectionTune for complete handshake support
- 📊 **Final Coverage**: 50.7% (20x improvement from starting 2.5%)
- 📝 **Test Files Created**: 6 new test files, 2,854 lines of test code, 129+ test cases
- ✅ **Zero Crashes**: All fuzz tests passed with 1.6M+ executions, all RabbitMQ tests skip gracefully

### Phase 10 - Performance Optimization:
- ✅ **Comprehensive Protocol Benchmarks**: 40+ benchmarks covering all protocol operations
  - Frame marshaling/unmarshaling (15-17 ns/op)
  - Method serialization (34-140 ns/op)
  - Field table operations (118-462 ns/op)
  - String encoding/decoding (0.7-106 ns/op)
  - Content header operations (22-153 ns/op)
  - Concurrent operations
  - Frame size performance (3-6 GB/s throughput)
- ✅ **Buffer Pooling System**: sync.Pool-based buffer management
  - 46% faster buffer operations
  - 100% fewer allocations for pooled operations
  - Separate pools for different buffer sizes
  - Automatic capacity limits (64KB max)
- ✅ **Optimized Frame Operations**:
  - WriteFrameOptimized: 20ns/op, **0 allocs** (100% reduction)
  - ReadFrameOptimized: 43ns/op, 85 B/op, **3 allocs** (25% reduction from 4)
  - MarshalBinaryOptimized: Precise pre-allocation
  - UnmarshalBinaryOptimized: Payload slice reuse
- ✅ **Performance Analysis**:
  - Hot path operations: 30-40 ns (BasicPublish/Deliver)
  - Frame processing: 24-65 million ops/sec
  - Message throughput: 3-6 GB/s
  - Zero-cost string encoding (0.72 ns, 0 allocs)
- ✅ **Comprehensive Documentation**: PERFORMANCE.md with full analysis
  - Detailed benchmark results
  - Optimization strategies
  - Performance comparison with industry standards
  - Recommendations for high-performance use cases
- 📊 **Key Metrics**:
  - Frame Marshal: 15.4 ns/op, 1 alloc
  - BasicPublish: 34.9 ns/op, 3 allocs (critical path)
  - WriteFrame: 20.0 ns/op, 0 allocs (optimized)
  - Buffer Pooling: 7.5 ns/op, 0 allocs (vs 13.8 ns, 1 alloc)
- ✅ **Zero Crashes**: All tests and benchmarks passing
- ✅ **Code Readability Improvements**:
  - Added comprehensive package-level documentation to buffer_pool.go and frame_optimized.go
  - Defined `FrameEnd` constant (0xCE) for frame end marker to replace magic numbers
  - Enhanced function documentation with performance characteristics
  - Improved error messages with detailed formatting
  - Fixed benchmark naming (100B, 1024B instead of Unicode characters)
  - Added descriptive comments to all benchmark functions
  - Applied `gofmt` formatting to all modified files
  - All tests passing after readability improvements

**Performance Files Added:**
- `protocol/buffer_pool.go` - Buffer pooling infrastructure
- `protocol/frame_optimized.go` - Optimized frame operations
- `protocol/protocol_bench_test.go` - Comprehensive benchmarks
- `protocol/protocol_bench_optimized_test.go` - Optimization comparison benchmarks
- `protocol/PERFORMANCE.md` - Complete performance documentation

**Code Quality & CI/CD Improvements:**
- ✅ **Linting Fixes**: Resolved all 11 go vet errors (mutex lock copying)
  - Added `Copy()` methods to Exchange and Queue structs for safe value copying
  - Fixed mutex copying in broker/storage_broker.go (2 locations)
  - Fixed mutex copying in storage/memory_stores.go (6 locations)
  - Fixed mutex copying in server/recovery_manager.go (4 locations)
  - Fixed IPv6 address handling in protocol/rabbitmq_interop_test.go
- ✅ **CI/CD Workflow Update**: Adapted for Go 1.25.1 compatibility
  - Replaced golangci-lint with standard Go tools (go fmt, go vet)
  - Added explicit formatting checks that fail on unformatted code
  - Maintained same quality standards with native Go tooling
  - All checks passing in GitHub Actions

**Test Files Added:**
- `protocol/frame_comprehensive_test.go` - Complete frame testing suite
- `protocol/methods_test.go` - Comprehensive method serialization tests
- `protocol/content_test.go` - Content header and property testing
- `protocol/compliance_test.go` - AMQP 0.9.1 spec compliance validation
- `protocol/fuzz_test.go` - Robustness testing with fuzzing
- `protocol/rabbitmq_interop_test.go` - RabbitMQ interoperability tests
- `protocol/RABBITMQ_TESTS.md` - RabbitMQ test documentation
- `protocol/docker-compose.yml` - Docker setup for RabbitMQ
- `protocol/run-rabbitmq-tests.sh` - Automated test runner
- `PROTOCOL_COVERAGE_PLAN.md` - Detailed 7-phase testing plan
- `protocol/TESTING_PLAN.md` - AMQP 0.9.1 compliance testing strategy

**Previous Phase 8 - Testing and Refinement: COMPLETED** ✅

### Previous Phase 8 Achievements:
- ✅ **Comprehensive Test Suite**: All 12 packages with passing tests (30+ test cases total)
- ✅ **Integration Test Coverage**: Authentication, messaging, transactions, storage backends
- ✅ **Performance Benchmarks**: Complete benchmark suite showing 3M+ ops/sec for memory storage
- ✅ **Storage Backend Testing**: Validated memory and Badger storage implementations
- ✅ **Cross-Package Validation**: All packages building and testing successfully
- ✅ **Test Documentation**: Comprehensive test results documented in commits
- ✅ **Quality Assurance**: Fixed outdated tests, added missing dependencies

### Previous Phase 7 Achievements:
- ✅ **SASL Authentication**: Pluggable authentication framework with PLAIN and ANONYMOUS mechanisms
- ✅ **File-Based Auth Backend**: JSON-based user storage with bcrypt password hashing
- ✅ **Connection Security**: Authentication enforcement during AMQP handshake with proper error handling
- ✅ **Integration Tests**: Comprehensive tests with real AMQP clients validating all auth scenarios
- ✅ **Security Documentation**: Complete README documentation with examples and best practices
- ✅ **Mechanism Registry**: Extensible registry system for adding new authentication mechanisms
- ✅ **Transaction Support**: Full AMQP transaction support with atomic operations (from earlier in Phase 7)
- ✅ **Prometheus Metrics**: Comprehensive metrics collection system with Prometheus integration
- ✅ **Metrics Server**: HTTP server exposing /metrics endpoint on port 9419 (standard AMQP exporter port)
- ✅ **Monitoring Documentation**: Complete documentation with Prometheus/Grafana integration examples and alerting rules

### Previous Phase 6 Achievements:
- ✅ **Message Durability**: Full AMQP-compliant message persistence with DeliveryMode=2 support
- ✅ **Durable Entities**: Auto-restore durable exchanges, queues, and bindings on server startup  
- ✅ **Acknowledgment Persistence**: Pending acknowledgment tracking and recovery across restarts
- ✅ **Atomic Operations**: Transaction-based atomic operations using Badger transactions
- ✅ **Storage Validation**: Integrity validation and auto-repair of storage corruption on startup
- ✅ **Recovery System**: Comprehensive recovery manager with detailed statistics and error handling
- ✅ **Performance Benchmarks**: Complete performance test suite with memory vs persistent storage comparison
- ✅ **Production Documentation**: Comprehensive README.md with all supported AMQP commands and performance results

### Previous Phase 5 Achievements:
- ✅ **Storage Abstraction**: Implemented comprehensive storage interface system with specialized stores
- ✅ **Badger Integration**: High-performance persistent storage with 375x faster writes than bbolt
- ✅ **Storage Factory**: Configurable backend selection supporting memory and badger with extensibility
- ✅ **Storage-Backed Broker**: Complete broker implementation using persistent storage for all operations
- ✅ **Unified Architecture**: Seamless compatibility between in-memory and storage-backed brokers
- ✅ **Message Persistence**: TTL-based message lifecycle management with background cleanup
- ✅ **Production Ready**: Thread-safe operations, comprehensive error handling, and full test coverage

### Previous Phase 4 Achievements:
- ✅ **Package Restructuring**: Converted monolithic structure to modular library architecture
- ✅ **Interface-Based Design**: Created comprehensive interfaces for all pluggable components
- ✅ **Configuration System**: Implemented flexible configuration with validation and JSON persistence
- ✅ **Builder Pattern**: Created fluent API for type-safe server construction with adapters
- ✅ **Error Handling**: Built AMQP-compliant error system with proper Go error handling
- ✅ **Lifecycle Management**: Implemented server lifecycle control with hooks and state management

### Previous Phase 3 Achievements:
- ✅ **Core AMQP functionality working**: Message publishing and consumption implemented
- ✅ **Race condition resolved**: Messages published before consumers register are properly queued
- ✅ **Full AMQP 0.9.1 protocol compliance**: Fixed critical protocol format violation affecting all message parsing
- ✅ **Standard client compatibility**: All AMQP client libraries (rabbitmq/amqp091-go) now work properly
- ✅ **Complete message support**: Handles all message types including empty messages, JSON, binary data, and Unicode
- ✅ **Message routing operational**: Default exchange behavior and direct queue delivery working
- ✅ **Full message transmission**: Server correctly processes and delivers complete message content
- ✅ **Consumer management**: Proper consumer registration, delivery loops, and acknowledgments
- ✅ **Broker architecture**: Comprehensive message queuing and delivery system implemented
- ✅ **Protocol methods**: `basic.publish`, `basic.consume`, `basic.ack`, etc. fully implemented
- ✅ **Connection stability fixed**: Resolved connection reset issues through atomic frame transmission
- ✅ **Protocol debugging**: Comprehensive analysis identifying universal frame format violation
- ✅ **Content header serialization**: Fixed double-encoding issues and ContentHeader.Serialize() implementation
- ✅ **Comprehensive testing**: Created test suite covering multiple payload sizes, content types, and edge cases

### Library Architecture:
- ✅ **Modular Design**: Clean separation between library (`server/`) and application (`cmd/amqp-server/`)
- ✅ **Interface-Driven**: Pluggable components for storage, broker, authentication, and logging
- ✅ **Type-Safe Configuration**: Builder patterns with validation and JSON persistence
- ✅ **Comprehensive Error Handling**: AMQP 0.9.1 compliant error types with Go's error handling conventions
- ✅ **Server Lifecycle Management**: State transitions, hooks, health monitoring, and graceful shutdown
- ✅ **Backward Compatibility**: All existing functionality preserved with improved architecture

**Next Phase:** Phase 11 - Production Hardening (TLS, Authorization, Performance Tuning)

## Completed Release Preparation:
- ✅ **GitHub Actions Workflows**: Multi-platform release builds for macOS (arm64, amd64), Linux (amd64, arm64, 386)
- ✅ **Continuous Integration**: Automated testing, linting, and code quality checks
- ✅ **Security Scanning**: CodeQL integration for vulnerability detection
- ✅ **Dependency Management**: Dependabot configuration for automated updates
- ✅ **Project Documentation**: LICENSE (MIT), CHANGELOG, CONTRIBUTING, SECURITY, RELEASE guides
- ✅ **Issue Templates**: Bug reports and feature requests
- ✅ **PR Template**: Standardized pull request process
- ✅ **Code Quality**: golangci-lint configuration with comprehensive rules
- ✅ **Versioning**: Semantic versioning with automated binary generation

## Phases

### Phase 1: Project Setup and Core Protocol Foundation
- [x] Initialize Go module `github.com/maxpert/amqp-go`
- [x] Define core data structures (Connection, Channel, Exchange, Queue, Message) matching the spec
- [x] Implement binary frame reader/writer based on the spec (using `encoding/binary`)
- [x] Parse the XML spec to auto-generate protocol constants/methods (optional but helpful)
- [x] Implement basic TCP server accepting connections on standard AMQP ports (5672, 5671)
- [x] Handle the initial handshake (connection negotiation, protocol headers)
- [x] Implement the `connection.*` and `channel.*` methods (open, close, basic flow)
- [x] Simple in-memory storage for connections and channels (using `sync.Map` or similar)
- [x] Write unit tests for core protocol parsing and connection/channel management
- [x] Implement proper logging using zap logger

### Phase 1.5: Housekeeping and Deployment Preparation
- [x] Create build script in root directory
- [x] Set up bin directory for binary output
- [x] Clean up current directory from junk files (logs, binaries, temp files)
- [x] Add command-line flags for log levels and output files
- [x] Add daemonize flag for server deployment
- [x] Create proper server binary with configuration options

### Phase 2: Exchange and Queue Management
- [x] Implement `exchange.declare` and `exchange.delete` methods
- [x] Implement `queue.declare`, `queue.bind`, `queue.unbind`, `queue.delete` methods
- [x] Implement core exchange types (direct, fanout, topic, headers) with routing logic
- [x] Basic in-memory queue implementation (FIFO)
- [x] Basic binding mechanism
- [x] Write tests for exchange and queue operations

### Phase 3: Message Publishing and Consumption
- [x] Implement `basic.publish` method (accepting message content and routing)
- [x] Implement `basic.consume`, `basic.cancel`, `basic.get` methods
- [x] Implement message delivery logic (to consumers)
- [x] Implement basic acknowledgments (`basic.ack`, `basic.nack`, `basic.reject`)
- [x] Handle pre-fetching and flow control
- [x] Write tests for publish/consume with acknowledgments
- [x] Fix race condition for messages published before consumers register
- [x] Implement broker message queuing and delivery system
- [x] Create consumer delivery loop with proper message routing
- [x] Fix connection reset issues through atomic frame transmission
- [x] Resolve content header double-encoding and serialization issues
- [x] Implement comprehensive debugging and logging infrastructure
- [x] Create comprehensive integration test suite with multiple payload types
- [x] Identify root cause of universal protocol format violation
- [x] Fix universal AMQP protocol format violation affecting all frame parsing
- [x] Fix protocol compatibility issues during connection cleanup (channel.close implementation)
- [x] Fix empty message handling for zero-length message bodies
- [x] Complete integration test passes with standard AMQP clients

### Phase 4: Library Refactoring and Reusability Foundation
- [x] **Package Restructuring**: Move from `package main` to proper library structure ✅
  - [x] Separate server implementation from main application
  - [x] Move internal packages to public where appropriate
  - [x] Create `cmd/` directory for CLI applications
  - [x] Add `examples/` directory with usage examples
- [x] **Interface-Based Design**: Create abstractions for all pluggable components ✅
  - [x] Define `Storage` interface for message and metadata persistence
  - [x] Define `MessageStore` interface for message durability
  - [x] Define `MetadataStore` interface for exchanges, queues, bindings
  - [x] Define `TransactionStore` interface for transactional operations
  - [x] Define `Broker` interface for custom message routing implementations
  - [x] Define `Logger` interface to decouple from zap dependency  
  - [x] Define `ConnectionHandler` interface for custom connection handling
  - [x] Define `Authenticator` interface for pluggable auth mechanisms
- [x] **Configuration System**: Replace hardcoded values with flexible config ✅
  - [x] Create `Config` struct with all server options
  - [x] Implement `ServerInfo` configuration for product/version details
  - [x] Add `NetworkConfig` for connection settings
  - [x] Add `StorageConfig` for storage backend options
  - [x] Add `SecurityConfig` for authentication and TLS options
- [x] **Builder Pattern**: Implement fluent API for server creation ✅
  - [x] `NewServer()` with chainable configuration methods
  - [x] `WithAddress()`, `WithLogger()`, `WithStorage()`, `WithBroker()` builders
  - [x] `Build()` method for final server construction
- [x] **Error Handling**: Define library-specific error types ✅
  - [x] Create AMQP-specific error types and codes
  - [x] Add storage-specific error types and recovery strategies
  - [x] Provide detailed error context and debugging information
  - [x] Allow customizable error handling strategies
- [x] **Lifecycle Management**: Provide proper server lifecycle control ✅
  - [x] `Start()`, `Stop()`, `Shutdown(graceful)` methods
  - [x] `Health()` status checking
  - [x] Graceful connection termination
  - [x] Resource cleanup and memory management

### Phase 5: Storage Implementation with Abstraction ✅ **COMPLETED**
- [x] **Storage Implementations**: Create multiple storage backends using defined interfaces
  - [x] Implement in-memory storage (default, non-persistent) with thread-safe operations
  - [x] Implement badger storage backend with TTL cleanup and performance optimization
  - [x] Create storage factory pattern for backend selection with automatic subdirectory creation
  - [x] Implement specialized interfaces (MessageStore, MetadataStore, TransactionStore) for modularity
- [x] **Storage Integration**: Integrate storage with broker and server using interfaces
  - [x] Refactor broker to use Storage interfaces instead of in-memory maps with StorageBroker implementation
  - [x] Implement storage lifecycle management through configuration system with validation
  - [x] Add storage initialization through builder pattern with automatic storage-backed broker creation
  - [x] Write storage interface tests and benchmarks with comprehensive coverage
  - [x] Create UnifiedBroker interface for compatibility between storage-backed and in-memory brokers
  - [x] Implement broker adapters for seamless integration with existing server architecture
- ✅ **Key Features Implemented**:
  - Badger-based persistent storage with 375x faster writes than bbolt
  - Thread-safe storage operations with proper error handling
  - Comprehensive test suite with both unit tests and benchmarks
  - Factory pattern supporting memory/badger backends with easy extensibility
  - Message TTL cleanup with background cleanup processes
  - Full server integration with automatic storage backend selection
  - Backward compatibility maintained through adapter pattern

### Phase 6: Persistence and Reliability ✅ **COMPLETED**
- [x] **Message Durability**: Implement persistent messages using storage interfaces
  - [x] Implement message persistence flags and durability (DeliveryMode=2 AMQP compliant)
  - [x] Implement durable exchanges and queues using MetadataStore interface
  - [x] Ensure message durability across server restarts with automatic recovery
  - [x] Implement message acknowledgment persistence with pending ack tracking
- [x] **Recovery and Consistency**: Handle server restarts and failures
  - [x] Implement server restart recovery using storage interfaces with RecoveryManager
  - [x] Handle partially written transactions and rollback with atomic operations
  - [x] Implement storage corruption detection and repair with validation system
  - [x] Write integration tests covering persistence and recovery scenarios
  - [x] Implement comprehensive performance benchmarks for all storage operations
  - [x] Create production-ready documentation with performance results and supported commands

### Phase 7: Advanced Features and Security
- [x] **Transactions**: Implement transactional operations using pluggable architecture ✅ **COMPLETED**
  - [x] Implement AMQP transactions (tx.select, tx.commit, tx.rollback) with full protocol support
  - [x] Create pluggable transaction architecture with interfaces for extensibility
  - [x] Implement transaction executor integration with unified broker system
  - [x] Add comprehensive transaction statistics and monitoring
  - [x] Implement channel-based transaction isolation with thread-safe operations
  - [x] Create complete test suite with 14 test cases covering all scenarios
  - [x] Integrate with server builder for automatic transaction manager configuration
  - [x] Support atomic storage operations for transactional guarantees
- [x] **Security and Access Control**: Add authentication and authorization using interfaces ✅ **COMPLETED**
  - [x] Design and implement SASL authentication framework with Authenticator interface ✅
  - [x] Implement PLAIN authentication mechanism (username/password) ✅
  - [x] Implement file-based user credential storage with bcrypt password hashing ✅
  - [x] Implement ANONYMOUS authentication mechanism (development/testing only) ✅
  - [x] Integrate authentication with connection handshake (connection.start-ok) ✅
  - [x] Enforce authentication during handshake with proper error handling ✅
  - [x] Add authentication configuration through SecurityConfig ✅
  - [x] Add security integration tests for PLAIN and ANONYMOUS mechanisms ✅
  - [x] Add comprehensive integration tests with actual AMQP clients ✅
  - [x] Create mechanism registry and adapter pattern for extensibility ✅
  - [x] Document authentication configuration in README.md ✅
- [x] **Monitoring and Management**: Add operational features ✅ **COMPLETED**
  - [x] Implement proper daemonization with process forking and daemon mode ✅
  - [x] Implement Prometheus metrics collection system with comprehensive metric types ✅
  - [x] Create metrics package with MetricsCollector interface and Collector implementation ✅
  - [x] Add HTTP server for /metrics endpoint on port 9419 with health check ✅
  - [x] Integrate metrics with server lifecycle (connections, channels, queues, messages, transactions) ✅
  - [x] Write tests for metrics collection with all metric types ✅
  - [x] Document Prometheus integration with Grafana dashboard queries and alerting rules ✅
  - [x] Create complete metrics server example showing integration ✅

### Phase 8: Testing and Refinement ✅ **COMPLETED**
- [x] **Comprehensive Testing**: Validate all interface implementations ✅
  - [x] Integration tests using all storage backends (memory, badger) ✅
  - [x] Cross-storage compatibility tests ✅
  - [x] Performance benchmarking across different storage backends ✅
  - [x] All 12 packages with comprehensive test coverage ✅
  - [x] 30+ test cases covering authentication, messaging, transactions, storage ✅
- [x] **Quality and Documentation**: Prepare for production use ✅
  - [x] Fixed outdated integration tests ✅
  - [x] Added missing test dependencies ✅
  - [x] Documented performance benchmarks in README ✅
  - [x] Validated all packages build successfully ✅
  - [x] GitHub Actions release workflow with multi-platform builds ✅
  - [x] Complete release documentation (CHANGELOG, RELEASE.md, CONTRIBUTING.md) ✅
  - [x] Security policy and best practices documentation ✅
  - [x] Issue and PR templates for community contributions ✅
  - [x] MIT License ✅
  - [x] Code quality tools (golangci-lint, CodeQL) ✅
  - [ ] Additional usage examples for different scenarios (optional)
  - [ ] Integration guides for popular frameworks (future enhancement)
  - [ ] Best practices documentation for interface implementations (future enhancement)

## Outstanding Questions

1.  **Persistence Strategy:** Which embedded database/library should be used for initial implementation? `bbolt` or `badger`?
A. Pick the most well maintained and high performing

2.  **Spec Parsing:** Should the XML spec be parsed at runtime to generate constants/methods, or should structures be manually defined based on the spec?
A. Follow what makes performance better

3.  **Concurrency Model:** How should connections/channels be managed using goroutines and channels? What are the locking strategies for shared state (exchanges, queues)?
A. Pick memory optimized high performance strategy

4.  **Client Compatibility:** Which existing AMQP Go clients will be used for testing compatibility throughout development?
A. Pick the most well maintained

5.  **Authentication:** What is the initial scope for authentication (e.g., hardcoded users, file-based, plugin system)?
A. Filebased

6.  **TLS/SSL:** How will TLS certificates be configured (file paths, options)?
A. file paths

7.  **Clustering/Federation:** Is clustering a goal for the initial release, or a future phase?
A. Keep it simple but write code structured for future phases keep it extensible as much as possible

8.  **AMQP 1.0:** Is there any intention to support AMQP 1.0, or strictly 0.9.1?
A. In future yes

9.  **Performance Targets:** Are there specific performance benchmarks or goals to aim for (e.g., messages/second, concurrent connections)?
A. Messages/second

10. **Logging:** What logging library and approach will be used?
A. Pick no overhead performance libraries like zerolog. Search for best ones available

## Dependencies
- Standard Go libraries (`net`, `encoding/binary`, `sync`, `context`, etc.)
- Potential embedded DB library (e.g., `go.etcd.io/bbolt`, `github.com/dgraph-io/badger`)
- Optional: XML parsing library if spec parsing is automated
- Test dependencies: Standard `testing` package, potentially `testify` for assertions
- Client libraries for testing (e.g., `github.com/streadway/amqp`, `github.com/rabbitmq/amqp091-go`)

## Initial Steps
1.  Create the repository and initial module structure.
2.  Download and examine the XML spec file.
3.  Decide on persistence and spec parsing strategies (Q1, Q2).
4.  Start with Phase 1: Project setup and core protocol structures.