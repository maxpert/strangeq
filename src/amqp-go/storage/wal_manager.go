package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	// WAL settings
	DefaultWALBatchSize    = 1000                  // Messages per batch
	DefaultWALBatchTimeout = 10 * time.Millisecond // Legacy batch flush timeout (no longer gates flushing; see WALConfig.BatchTimeout)
	DefaultWALFileSize     = 512 * 1024 * 1024     // 512 MB per WAL file
	WALFileExtension       = ".wal"

	// maxRetainedWALBatchBytes bounds how large the batch writer's persistent,
	// reused serialization buffer may grow before it is released back to the GC
	// after a flush. The buffer is normally retained across flushes (grow-once)
	// to avoid re-allocating the batch accumulator every group commit; this cap
	// guards against a rare, unusually large batch permanently pinning a very
	// large buffer. It is comfortably above the steady-state batch size of the
	// worst benchmarked cell (64KB body × ~1000-record batch ≈ 64 MB) so the
	// common case keeps its reuse win, while an outlier batch is not retained.
	maxRetainedWALBatchBytes = 128 * 1024 * 1024 // 128 MB

	// Shared WAL message (record) format (v4, ITER4). The framing envelope is
	// unchanged from earlier versions; the message payload is the presence-bitmap
	// record that persists the ENTIRE protocol.Message (all 14 optional
	// properties, not the pre-v4 subset). The identical payload is produced by the
	// shared appendMessagePayload/parseMessagePayload codec and carried by both
	// the WAL and the per-queue segments. See the v4 layout comment on
	// appendMessagePayload for the byte-level detail.
	//
	//   record: [4 CRC32][4 length][1 recordType][<message payload v4>]
	//   payload: [2 flags][queue u8-len][8 offset][exch u8-len][rk u8-len]
	//            [1 deliveryMode][1 bodyKind][body|ref][optional fields in flag order]

	// WAL file-level header (SQ-4): written once at the start of every NEW WAL
	// file. It lets recovery detect the on-disk format version before parsing
	// records. The magic is deliberately not a valid start of a record (a real
	// record begins with a 4-byte CRC then a 4-byte length); a first-byte
	// collision with an actual CRC value is astronomically unlikely, so a byte
	// prefix compare is a safe way to sniff the version / reject a foreign file.
	WALMagic      = "SQWAL"           // 5-byte file magic
	WALHeaderSize = len(WALMagic) + 1 // magic + 1 version byte = 6 bytes
	// Pre-v4 format versions. ITER4 is a deliberate CLEAN BREAK: these versions
	// (and headerless v0 files) are no longer parseable and are rejected loudly
	// with ErrUnsupportedWALVersion (see walFileDataStart). They are retained as
	// named constants so the rejection is self-documenting and testable.
	WALVersion1 = uint8(1) // SQ-4: versioned files, no per-record type tag
	WALVersion2 = uint8(2) // SQ-8: per-record type tag, no message extensions
	WALVersion3 = uint8(3) // W2: per-record type tag + Headers/Expiration/EnqueueUnixMilli extension block
	// WALVersion4 (ITER4): per-record type tag + presence-bitmap message payload
	// that persists the full basic.properties set. This build reads and writes
	// ONLY v4; there is no backwards compatibility with v0/v1/v2/v3.
	WALVersion4      = uint8(4)
	WALFormatVersion = WALVersion4 // current file format version

	// Per-record type tag. Every v4 record carries a 1-byte record-type tag
	// immediately after the [CRC][length] framing, so non-message records
	// (transaction-boundary markers) can be interleaved with message records in
	// the same file:
	//   v4: [CRC][len][1-byte recordType][<message payload v4>]
	WALRecordTypeMessage    = uint8(0) // a message record
	WALRecordTypeTxBoundary = uint8(1) // transaction-boundary marker (SQ-8)
	// WALRecordTypeBodyBlock (ITER5) carries a large message body ONCE for a
	// durable fan-out to N>=2 queues; the N per-queue message records each carry a
	// body REFERENCE (bodyKindReference) to this block's file-relative offset
	// instead of an inline copy. Block and refs are written in ONE group-commit
	// batch / ONE fsync / ONE file, so they are inseparable (co-location invariant,
	// §3). The block is NOT a deliverable message: its offset is never added to the
	// offset index or a file's offset set, so it never participates in the ack /
	// file-delete predicate — it rides its file's fate. Payload:
	//   [fanoutHint u32][bodyLen u32][body bodyLen bytes]
	// fanoutHint is the N at write time — advisory only (metrics/assertions);
	// correctness never reads it (the authoritative refcount is |unacked refs|).
	WALRecordTypeBodyBlock = uint8(2)

	// Transaction-boundary marker kinds. A committed transaction is written as
	// one contiguous, single-fsync unit: Begin, the transaction's message
	// records, then Commit. Recovery buffers records seen after a Begin and only
	// emits them once the matching Commit is read; a Begin with no Commit (a
	// crash mid-transaction, or a torn tail) is discarded — this is what makes
	// transaction commit all-or-nothing across a crash (SQ-8). Marker payload is
	// [1-byte kind][8-byte epoch].
	WALTxMarkerBegin      = uint8(0)
	WALTxMarkerCommit     = uint8(1)
	walTxMarkerPayloadLen = 9 // kind(1) + epoch(8)
)

// ErrUnsupportedWALVersion indicates a WAL file this build cannot parse: either
// it begins with the SQWAL magic but carries a non-v4 format version, or it is a
// pre-v4 headerless file (no magic). ITER4 is a clean break — recovery surfaces
// this as a hard error rather than silently skipping the file, since silently
// ignoring an unreadable file could drop durable messages.
var ErrUnsupportedWALVersion = errors.New("unsupported WAL file format version")

// ErrShortStringOverflow indicates a shortstr field (AMQP basic-property string)
// exceeds the 255-byte maximum encodable in the v4 record's 1-byte length prefix.
// The encoder returns this rather than truncating a length into a u8 (which would
// silently corrupt the record); the batch writer nacks the offending record. This
// should be unreachable in practice because the wire decoder already bounds every
// basic-property string to 255 bytes — it is a defense-in-depth guard.
var ErrShortStringOverflow = errors.New("shortstr field exceeds 255 bytes")

// v4 message-payload presence bitmap (u16, big-endian). Each optional field owns
// one bit; a set bit means the field is present (differs from its Go zero) and its
// value follows, in canonical order, in the record. The bit VALUES mirror AMQP's
// own basic-properties property-flags word (protocol/content.go:32-45) so the
// storage record reuses the exact mental model the wire codec already implements;
// they are defined here as storage-local constants to avoid coupling the storage
// layer to the wire constants. 0x1000 (AMQP FlagDeliveryMode) is intentionally
// unused: deliveryMode is a structural field, not an optional one. 0x0001 is a
// reserved continuation bit (MUST be 0 in v4); if a future version needs more than
// 14 optional fields it sets 0x0001 and appends a second u16 flags word, keeping
// the common case at a flat 2 bytes.
const (
	msgFlagContentType     = uint16(0x8000)
	msgFlagContentEncoding = uint16(0x4000)
	msgFlagHeaders         = uint16(0x2000)
	msgFlagPriority        = uint16(0x0800)
	msgFlagCorrelationID   = uint16(0x0400)
	msgFlagReplyTo         = uint16(0x0200)
	msgFlagExpiration      = uint16(0x0100)
	msgFlagMessageID       = uint16(0x0080)
	msgFlagTimestamp       = uint16(0x0040)
	msgFlagType            = uint16(0x0020)
	msgFlagUserID          = uint16(0x0010)
	msgFlagAppID           = uint16(0x0008)
	msgFlagClusterID       = uint16(0x0004)
	msgFlagEnqueue         = uint16(0x0002) // EnqueueUnixMilli (WAL-local, no AMQP wire analogue)
	msgFlagContinuation    = uint16(0x0001) // reserved; MUST be 0 in v4

	// bodyKind discriminator (1 byte) — the ITER4 body inline|reference union
	// seam. ITER4 always writes/reads bodyKindInline; bodyKindReference is fully
	// specified (u16-length-prefixed opaque locator) so ITER5's shared-body store
	// plugs in with no format change, and the ITER4 decoder length-skips the
	// reference arm structurally so an ITER5-written file never panics recovery.
	bodyKindInline    = uint8(0x00)
	bodyKindReference = uint8(0x01)
)

// WALConfig holds configurable parameters for the WAL manager
type WALConfig struct {
	BatchSize int
	// BatchTimeout is retained for configuration compatibility but no longer
	// gates flushing: the batch writer flushes as soon as writeChan is drained
	// (drain-then-flush group commit), so a lone synchronous writer is never
	// stalled waiting for a batch ticker. See batchWriterLoop.
	BatchTimeout       time.Duration
	FileSize           int64
	ChannelBuffer      int
	CleanupInterval    time.Duration
	RetentionPeriod    time.Duration // for time-based retention
	CheckpointInterval time.Duration // how often WAL checkpoints old files to segments

	// SyncDisabled, when true, skips the group-commit fdatasync in flushBatch
	// (the async publish durability barrier). Zero value (false) = fsync ON, the
	// durable default, so DefaultWALConfig() and zero-value WALConfig{} stay
	// durable. Set only via WALConfigFromEngine from the user's Storage.Fsync
	// flag. It does NOT relax WriteTxAtomic, which always fdatasyncs directly —
	// transactions remain durable even when this is set.
	SyncDisabled bool

	// CRCDisabled, when true, skips CRC32 computation on the write path (writes
	// zero in the CRC field) and skips verification on the read path (when the
	// stored CRC is zero). Zero value (false) = CRC ON, the safe default, so
	// DefaultWALConfig() and zero-value WALConfig{} keep integrity checks. Set
	// via WALConfigFromEngine from the user's Storage.CRCCheck flag. The record
	// format is unchanged: the CRC field is always present. The read path
	// handles mixed files by verifying non-zero CRCs and skipping zero ones.
	CRCDisabled bool

	// SharedBodyMaxPinAge (ITER5 cold-tail hardening, §2) bounds how long a rolled
	// WAL file that carries a shared BodyBlock may be kept out of checkpoint before
	// it is force-re-inlined (N copies) and deleted. 0 disables the age backstop
	// and relies on RetentionPeriod (which, if also 0, imposes no age bound — a
	// permanently-unacked reference then pins its BodyBlock file, holding exactly
	// ONE body copy, never the N-copy blowup). When >0, performCheckpoint stops
	// skipping a shared-body file once it is older than this and re-inlines it via
	// the normal checkpoint path.
	SharedBodyMaxPinAge time.Duration
}

// DefaultWALConfig returns a WALConfig with production defaults
func DefaultWALConfig() WALConfig {
	return WALConfig{
		BatchSize:          DefaultWALBatchSize,
		BatchTimeout:       DefaultWALBatchTimeout,
		FileSize:           DefaultWALFileSize,
		ChannelBuffer:      10000,
		CleanupInterval:    5 * time.Second,
		RetentionPeriod:    0, // disabled by default
		CheckpointInterval: 5 * time.Minute,
	}
}

// WALMetrics interface for metrics collection
type WALMetrics interface {
	UpdateWALSize(bytes float64)
	RecordWALWrite()
	RecordWALFsync(duration float64)
	RecordWALWriteError()
}

// WALManager manages a single shared Write-Ahead Log for ALL queues
// Shared WAL architecture (RabbitMQ quorum queue style):
// - One WAL serves all queues (more efficient fsync)
// - Entries include queue name for routing
// - Single set of background goroutines
type WALManager struct {
	dataDir   string
	sharedWAL *QueueWAL // Single WAL for all queues
	mu        sync.RWMutex
	metrics   WALMetrics
	cfg       WALConfig
}

// QueueWAL manages a SHARED Write-Ahead Log with lock-free write path
// Note: Despite the name, this now serves ALL queues (naming kept for compatibility)
type QueueWAL struct {
	name    string // "shared" for the single shared WAL
	dataDir string
	cfg     WALConfig

	// Lock-free write path
	writeChan chan *writeRequest // Buffered channel for lock-free writes
	ackChan   chan *ackRequest   // Buffered channel for ACKs (queue+offset)

	// ACK tracking
	ackBitmap      *roaring64.Bitmap
	bitmapMutex    sync.RWMutex
	lastCheckpoint uint64 // Last offset checkpointed to segments

	// Current active file
	currentFile             *os.File
	currentReadFile         *os.File // Read-only handle for ReadAt (currentFile is O_WRONLY)
	fileNum                 atomic.Uint64
	fileOffset              atomic.Int64
	fileMutex               sync.Mutex
	currentFileOffsets      *roaring64.Bitmap
	currentFileOffsetsMutex sync.Mutex

	// currentFileHasSharedBody (ITER5 §2) records that the active file carries at
	// least one BodyBlock record. It is set under fileMutex by flushBatch when a
	// fused shared unit is serialized and captured into walFileInfo.hasSharedBody
	// (then reset) by rollFile. performCheckpoint skips a rolled file with this set
	// so a shared body survives as ONE copy across arbitrarily many checkpoints
	// (until all refs ack -> whole-file reclaim, or the SharedBodyMaxPinAge backstop
	// fires). Guarded by fileMutex (always held in flushBatch and rollFile).
	currentFileHasSharedBody bool

	// Old inactive files
	oldFiles      map[uint64]*walFileInfo
	oldFilesMutex sync.RWMutex

	// File handle cache for old WAL files (avoids open/close per read)
	oldFileCache      map[uint64]*os.File
	oldFileCacheMutex sync.RWMutex

	// Offset index for O(1) lookups (Phase 6D: Fix consumer bottleneck)
	// Maps offset → (fileNum, filePosition) for fast reads
	offsetIndex      map[uint64]*offsetLocation
	offsetIndexMutex sync.RWMutex

	// Segment manager for checkpointing
	segmentMgr      *SegmentManager
	segmentMgrMutex sync.RWMutex

	// Metrics collector
	metrics WALMetrics

	// Background goroutines
	stopChan chan struct{}
	wg       sync.WaitGroup

	// txEpoch issues a monotonically increasing id per atomic transaction write
	// (SQ-8). It pairs a Begin marker with its Commit marker so recovery can
	// detect a stray/torn commit whose begin is missing.
	txEpoch atomic.Uint64
}

// offsetLocation tracks where an offset lives in the WAL
type offsetLocation struct {
	fileNum      uint64
	filePosition int64
}

// sharedIndexEntry records where a fused shared unit's reference record landed,
// so flushBatch can populate the offset index for it post-fsync (the BodyBlock
// record itself is never indexed). See flushBatch.
type sharedIndexEntry struct {
	offset       uint64
	filePosition int64
}

type writeRequest struct {
	queueName string
	message   *protocol.Message
	offset    uint64
	// Exactly one of done / onDone is set:
	//   - done  : the synchronous caller (Write, tx staging, transient spill,
	//     tests) blocks on this cap-1 pooled channel until its batch is fsynced.
	//   - onDone : the ASYNC caller (WriteAsync) supplies a completion callback
	//     that flushBatch invokes, in enqueue order, AFTER the batch is durable
	//     (fdatasync returned) AND the offset index is populated. The callback
	//     runs on the shared batch-writer goroutine and MUST be non-blocking
	//     (atomics + non-blocking pokes only) — it must never do socket I/O,
	//     fsync, or hold a lock across I/O (invariant A4), or it re-creates the
	//     per-connection fsync stall this design removes.
	done   chan error
	onDone func(error)

	// unit, when non-nil, makes this ONE writeChan item a fused shared-body unit
	// (ITER5, §3): flushBatch serializes it as one BodyBlock record followed by N
	// reference records, all within this single request so they land in ONE file /
	// ONE fsync (co-location invariant). unit is mutually exclusive with
	// queueName/message/offset/done/onDone above (those describe an ordinary
	// single-record write; a unit carries its own per-sub fields). A nil unit is
	// the byte-for-byte-unchanged ordinary write path.
	unit *sharedUnit
}

// sharedSub is one fan-out target of a fused shared-body write: a per-queue
// message record that will carry a body REFERENCE to the unit's shared block.
// Exactly one of onDone/done is set (same contract as writeRequest).
type sharedSub struct {
	queueName string
	offset    uint64            // delivery tag for this target
	message   *protocol.Message // shares Body with its siblings; Headers already per-target
	onDone    func(error)       // per-copy completion (async path)
	done      chan error        // per-copy completion (sync path)
}

// sharedUnit is the indivisible write unit for a durable fan-out with a large,
// shared body: ONE BodyBlock (body written once) + one reference record per sub.
type sharedUnit struct {
	body       []byte
	fanoutHint uint32
	subs       []sharedSub
}

type ackRequest struct {
	queueName string
	offset    uint64
}

type walFileInfo struct {
	path      string
	offsets   *roaring64.Bitmap // actual message offsets in this file
	createdAt time.Time         // when this file was rolled (for retention)
	// hasSharedBody (ITER5 §2) is true iff this rolled file carries at least one
	// BodyBlock record. performCheckpoint skips such files (keeping the body as
	// ONE copy) unless the SharedBodyMaxPinAge backstop forces re-inline.
	hasSharedBody bool
}

// NewWALManager creates a new WAL manager with shared WAL using default config
func NewWALManager(dataDir string) (*WALManager, error) {
	return NewWALManagerWithConfig(dataDir, DefaultWALConfig())
}

// NewWALManagerWithConfig creates a new WAL manager with custom config
func NewWALManagerWithConfig(dataDir string, cfg WALConfig) (*WALManager, error) {
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wm := &WALManager{
		dataDir: walDir,
		cfg:     cfg,
	}

	// Create the single shared WAL for all queues
	sharedWAL, err := wm.createSharedWAL()
	if err != nil {
		return nil, fmt.Errorf("failed to create shared WAL: %w", err)
	}
	wm.sharedWAL = sharedWAL

	return wm, nil
}

// SetSegmentManager sets the segment manager for checkpointing
func (wm *WALManager) SetSegmentManager(segmentMgr *SegmentManager) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.sharedWAL != nil {
		wm.sharedWAL.segmentMgrMutex.Lock()
		wm.sharedWAL.segmentMgr = segmentMgr
		wm.sharedWAL.segmentMgrMutex.Unlock()
	}
}

// getSegmentMgr returns the segment manager (thread-safe)
func (qw *QueueWAL) getSegmentMgr() *SegmentManager {
	qw.segmentMgrMutex.RLock()
	defer qw.segmentMgrMutex.RUnlock()
	return qw.segmentMgr
}

// SetMetrics sets the metrics collector for the WAL manager
func (wm *WALManager) SetMetrics(metrics WALMetrics) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.metrics = metrics
	if wm.sharedWAL != nil {
		wm.sharedWAL.metrics = metrics
	}
}

// doneChannelPool pools buffered error channels (cap 1) used by Write() for
// synchronous batch durability. Each durable message previously allocated a
// new channel via make(chan error, 1); the pool eliminates that allocation on
// the hot path after warmup.
var doneChannelPool = sync.Pool{
	New: func() interface{} {
		return make(chan error, 1)
	},
}

// getDoneChannelPool returns the package-level done channel pool (for testing).
func getDoneChannelPool() *sync.Pool {
	return &doneChannelPool
}

// getDoneChannel gets a buffered error channel (cap 1) from the pool.
func getDoneChannel(pool *sync.Pool) chan error {
	return pool.Get().(chan error)
}

// putDoneChannel returns a channel to the pool. Defensively drains any stale
// value to prevent deadlock if a future code path returns the channel without
// reading the result (a pending value in a cap-1 channel would cause the next
// flushBatch send to block forever).
func putDoneChannel(pool *sync.Pool, ch chan error) {
	select {
	case <-ch: // drain any stale value
	default:
	}
	pool.Put(ch)
}

// Write writes a message to the shared WAL (synchronous batch write)
// Blocks until the batch containing this message is fsynced to disk.
// This ensures proper AMQP durability semantics (Option 2: Batch Synchronous).
func (wm *WALManager) Write(queueName string, message *protocol.Message, offset uint64) error {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return fmt.Errorf("shared WAL not initialized")
	}

	// Get a response channel from the pool (avoids make(chan error, 1) per write)
	doneChan := getDoneChannel(&doneChannelPool)
	defer putDoneChannel(&doneChannelPool, doneChan)

	// Guard against send-on-closed-channel panic during concurrent Close:
	// select on stopChan so a shutting-down WAL returns an error instead of
	// blocking forever or panicking (mirrors Acknowledge's pattern).
	select {
	case sw.writeChan <- &writeRequest{
		queueName: queueName,
		message:   message,
		offset:    offset,
		done:      doneChan,
	}:
	case <-sw.stopChan:
		return fmt.Errorf("WAL shutting down")
	}

	// Block waiting for batch to be fsynced
	// This provides proper durability guarantee before returning
	return <-doneChan
}

// WriteAsync enqueues a message for durable group-commit exactly like Write,
// but returns as soon as the request is queued instead of blocking the caller
// until the fsync completes. The supplied onDone is invoked by flushBatch after
// the batch containing this message is fsynced to disk AND the offset index is
// populated (success => onDone(nil)), or with the write/fsync error on failure
// (index NOT updated). Completions for a batch fire in enqueue order.
//
// This decouples the durable publish path from the per-connection frame
// processor: pipelined publishes fill writeChan so one F_FULLFSYNC amortises
// over the whole drained batch (see the design in the async-WAL proposal).
// onDone runs on the shared batch-writer goroutine and MUST be non-blocking
// (see writeRequest.onDone). The blocking writeChan send is preserved, so
// writeChan's capacity remains the durable in-flight memory bound.
func (wm *WALManager) WriteAsync(queueName string, message *protocol.Message, offset uint64, onDone func(error)) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		if onDone != nil {
			onDone(fmt.Errorf("shared WAL not initialized"))
		}
		return
	}

	select {
	case sw.writeChan <- &writeRequest{
		queueName: queueName,
		message:   message,
		offset:    offset,
		onDone:    onDone,
	}:
	case <-sw.stopChan:
		if onDone != nil {
			onDone(fmt.Errorf("WAL shutting down"))
		}
	}
}

// WriteSharedAsync enqueues a fused shared-body durable write (ITER5, §3): ONE
// BodyBlock record carrying body a single time, plus one reference record per
// sub, as a SINGLE writeChan item. Because a single request is never split across
// batches (batchWriterLoop) and a batch never rolls mid-flush (flushBatch rolls
// only AFTER the whole-batch Write+fsync), the block and all N refs are provably
// in ONE file and made durable by ONE fdatasync (co-location invariant + A1).
// Each sub's onDone/done fires — in sub order — only AFTER the unit is durable
// AND indexed, exactly like WriteAsync. All-or-nothing: a serialize error rolls
// the whole unit back and nacks every sub. body must be the shared body every
// sub references (the caller keeps each sub.message.Body pointing at it for
// in-memory delivery; the on-disk record for each sub is a reference).
func (wm *WALManager) WriteSharedAsync(subs []sharedSub, body []byte, fanoutHint uint32) error {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return fmt.Errorf("shared WAL not initialized")
	}
	if len(subs) == 0 {
		return nil
	}
	select {
	case sw.writeChan <- &writeRequest{
		unit: &sharedUnit{
			body:       body,
			fanoutHint: fanoutHint,
			subs:       subs,
		},
	}:
		return nil
	case <-sw.stopChan:
		return fmt.Errorf("WAL shutting down")
	}
}

// walGroupCommitFsync is the fsync applied by the group-commit batch writer
// (flushBatch). It is swappable via an atomic pointer so tests can gate the
// durability barrier or inject a write/fsync error deterministically; the
// production path always uses fdatasyncFile. Only the group-commit path is
// affected — WriteTxAtomic keeps calling fdatasyncFile directly, so the
// transaction slow path is byte-for-byte unchanged.
type walFsyncFn func(*os.File) error

var walGroupCommitFsyncHook atomic.Pointer[walFsyncFn]

func walGroupCommitFsync(f *os.File) error {
	if p := walGroupCommitFsyncHook.Load(); p != nil {
		return (*p)(f)
	}
	return fdatasyncFile(f)
}

// SetWALGroupCommitFsyncForTest installs fn as the fsync used by the shared-WAL
// group-commit writer and returns a restore function. TEST-ONLY: it lets tests
// hold the durability barrier open (to prove no confirm/visibility precedes the
// fsync) or force an fsync error (to prove the nack path). Never called by
// production code. Not safe to call concurrently with an active writer that is
// mid-fsync; tests set it before publishing and restore it after.
func SetWALGroupCommitFsyncForTest(fn func(*os.File) error) (restore func()) {
	prev := walGroupCommitFsyncHook.Load()
	f := walFsyncFn(fn)
	walGroupCommitFsyncHook.Store(&f)
	return func() { walGroupCommitFsyncHook.Store(prev) }
}

// WALRecordCounts tallies the record types found in the shared WAL. See
// WALRecordCountsForTest.
type WALRecordCounts struct {
	Messages     int // total message records (inline + reference)
	InlineBodies int // message records with an inline body (bodyKind 0x00)
	RefBodies    int // message records with a body reference (bodyKind 0x01)
	TxBoundary   int // transaction-boundary markers
	BodyBlocks   int // ITER5 shared BodyBlock records
}

// WALRecordCountsForTest scans every .wal file under dataDir/wal/shared and
// tallies record types. TEST-ONLY (used by cross-package tests to assert the
// shared-body fan-out writes exactly one BodyBlock plus one reference record per
// fan-out target, and that the N==1 hot path stays inline). Never used by
// production code.
func WALRecordCountsForTest(dataDir string) (WALRecordCounts, error) {
	var total WALRecordCounts
	sharedDir := filepath.Join(dataDir, "wal", "shared")
	entries, err := os.ReadDir(sharedDir)
	if err != nil {
		return total, err
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != WALFileExtension {
			continue
		}
		c, cerr := scanWALFileRecordCounts(filepath.Join(sharedDir, e.Name()))
		if cerr != nil {
			return total, cerr
		}
		total.Messages += c.Messages
		total.InlineBodies += c.InlineBodies
		total.RefBodies += c.RefBodies
		total.TxBoundary += c.TxBoundary
		total.BodyBlocks += c.BodyBlocks
	}
	return total, nil
}

// scanWALFileRecordCounts tallies record types in one WAL file (see
// WALRecordCountsForTest).
func scanWALFileRecordCounts(filePath string) (WALRecordCounts, error) {
	var c WALRecordCounts
	f, err := os.Open(filePath)
	if err != nil {
		return c, err
	}
	defer f.Close()

	dataStart, err := walFileDataStart(f)
	if err != nil {
		return c, err
	}
	if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
		return c, err
	}
	for {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(hdr[4:8])
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(f, data); err != nil {
			break
		}
		storedCRC := binary.BigEndian.Uint32(hdr[0:4])
		if storedCRC != 0 {
			h := crc32.NewIEEE()
			h.Write(hdr[4:8])
			h.Write(data)
			if storedCRC != h.Sum32() {
				continue
			}
		}
		recType, payload := recordTypeAndPayload(data)
		switch recType {
		case WALRecordTypeBodyBlock:
			c.BodyBlocks++
		case WALRecordTypeTxBoundary:
			c.TxBoundary++
		default:
			c.Messages++
			if rm, ok := deserializeMessagePayload(payload); ok {
				if len(rm.Message.BodyRef) > 0 {
					c.RefBodies++
				} else {
					c.InlineBodies++
				}
			}
		}
	}
	return c, nil
}

// WriteTxAtomic durably writes a set of message records as one all-or-nothing
// transaction unit (SQ-8): a Begin marker, the records, then a Commit marker,
// appended contiguously and fsynced exactly once. Recovery only applies the
// records if the trailing Commit marker is present, so a crash before the fsync
// completes (or a torn tail) leaves the whole transaction uncommitted.
//
// This is deliberately a SLOW PATH that is independent of the group-commit
// batch writer (batchWriterLoop/flushBatch) and its CAS ring — transaction
// commit favors correctness over throughput and must not be interleaved with
// unrelated messages, so the records are written directly under fileMutex
// rather than through writeChan.
func (wm *WALManager) WriteTxAtomic(records []*RecoveryMessage) error {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return fmt.Errorf("shared WAL not initialized")
	}
	return sw.writeTxAtomic(records)
}

func (qw *QueueWAL) writeTxAtomic(records []*RecoveryMessage) error {
	if len(records) == 0 {
		return nil
	}

	qw.fileMutex.Lock()
	defer qw.fileMutex.Unlock()

	currentFileNum := qw.fileNum.Load()
	startPos := qw.fileOffset.Load()
	epoch := qw.txEpoch.Add(1)

	// Serialize [Begin][record...][Commit] into a single contiguous buffer and
	// record each message record's start position for the offset index.
	buf := serializeTxMarker(WALTxMarkerBegin, epoch, qw.cfg.CRCDisabled)
	positions := make([]int64, len(records))
	for i, rec := range records {
		positions[i] = startPos + int64(len(buf))
		recBytes, serErr := qw.serializeMessageVersioned(rec.QueueName, rec.Message, rec.Offset)
		if serErr != nil {
			// A record that cannot be encoded (e.g. a shortstr property >255
			// bytes) fails the WHOLE transaction before anything is written, so
			// the all-or-nothing guarantee holds and no corrupt record is emitted.
			return fmt.Errorf("WAL transaction serialize failed: %w", serErr)
		}
		buf = append(buf, recBytes...)
	}
	buf = append(buf, serializeTxMarker(WALTxMarkerCommit, epoch, qw.cfg.CRCDisabled)...)

	n, err := qw.currentFile.Write(buf)
	if err != nil {
		if qw.metrics != nil {
			qw.metrics.RecordWALWriteError()
		}
		return fmt.Errorf("WAL transaction write failed: %w", err)
	}

	// Single fsync makes the whole bracketed unit durable. The Commit marker is
	// the last thing written, so if this fsync does not complete the tail
	// (Commit) is lost and recovery discards the transaction.
	fsyncStart := time.Now()
	fsyncErr := fdatasyncFile(qw.currentFile)
	if qw.metrics != nil {
		qw.metrics.RecordWALFsync(time.Since(fsyncStart).Seconds())
		if fsyncErr != nil {
			qw.metrics.RecordWALWriteError()
		}
	}
	if fsyncErr != nil {
		return fmt.Errorf("WAL transaction fsync failed: %w", fsyncErr)
	}
	if qw.metrics != nil {
		for range records {
			qw.metrics.RecordWALWrite()
		}
	}

	// Durable: publish the records to the offset index and current-file offset
	// set so they are readable/ackable exactly like batch-written messages.
	qw.offsetIndexMutex.Lock()
	for i, rec := range records {
		qw.offsetIndex[rec.Offset] = &offsetLocation{
			fileNum:      currentFileNum,
			filePosition: positions[i],
		}
	}
	qw.offsetIndexMutex.Unlock()

	qw.currentFileOffsetsMutex.Lock()
	for _, rec := range records {
		qw.currentFileOffsets.Add(rec.Offset)
	}
	qw.currentFileOffsetsMutex.Unlock()

	newOffset := qw.fileOffset.Add(int64(n))
	if newOffset >= qw.cfg.FileSize {
		qw.rollFile()
	}
	return nil
}

// Acknowledge marks a message as ACKed
func (wm *WALManager) Acknowledge(queueName string, offset uint64) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return
	}

	// Send to ACK channel (blocking with stopChan to prevent silent drops)
	select {
	case sw.ackChan <- &ackRequest{
		queueName: queueName,
		offset:    offset,
	}:
	case <-sw.stopChan:
		// WAL is shutting down, discard ACK
	}
}

// Read reads a message from shared WAL by queue+offset
func (wm *WALManager) Read(queueName string, offset uint64) (*protocol.Message, error) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return nil, fmt.Errorf("shared WAL not initialized")
	}

	return sw.readMessage(queueName, offset)
}

// Close closes the shared WAL
func (wm *WALManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.sharedWAL != nil {
		wm.sharedWAL.close()
		wm.sharedWAL = nil // Set to nil so writes fail after close
	}
	return nil
}

// RecoveryMessage represents a message recovered from WAL for crash recovery
type RecoveryMessage struct {
	QueueName string
	Message   *protocol.Message
	Offset    uint64
}

// RecoverFromWAL scans all WAL files and returns unacknowledged messages
// This is called during broker startup to recover messages after a crash
// Phase 3: Crash recovery implementation
func (wm *WALManager) RecoverFromWAL() ([]*RecoveryMessage, error) {
	if wm.sharedWAL == nil {
		return nil, fmt.Errorf("shared WAL not initialized")
	}

	var recoveredMessages []*RecoveryMessage

	// Get list of all WAL files
	sharedDir := filepath.Join(wm.dataDir, "shared")
	files, err := os.ReadDir(sharedDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// Scan each WAL file
	for _, file := range files {
		if filepath.Ext(file.Name()) != WALFileExtension {
			continue
		}

		filePath := filepath.Join(sharedDir, file.Name())
		messages, err := wm.sharedWAL.scanWALFile(filePath)
		if err != nil {
			// An unknown/unsupported file format version is a hard error: we
			// must not silently skip a file that may hold durable messages.
			if errors.Is(err, ErrUnsupportedWALVersion) {
				return nil, fmt.Errorf("WAL recovery aborted for %s: %w", file.Name(), err)
			}
			// Other errors (e.g. transient open/read failures, torn tail) are
			// best-effort — log and continue with other files.
			continue
		}

		// Filter out acknowledged messages
		for _, msg := range messages {
			wm.sharedWAL.bitmapMutex.RLock()
			isAcked := wm.sharedWAL.ackBitmap.Contains(msg.Offset)
			wm.sharedWAL.bitmapMutex.RUnlock()

			if !isAcked {
				// Message not acknowledged - needs redelivery
				recoveredMessages = append(recoveredMessages, msg)
			}
		}
	}

	return recoveredMessages, nil
}

// scanWALFile scans a single WAL file and returns all messages
func (qw *QueueWAL) scanWALFile(filePath string) ([]*RecoveryMessage, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Detect the file-level header and start parsing records after it. ITER4 is a
	// clean break: only v4 files are accepted; a non-v4 or headerless (pre-v4)
	// file returns ErrUnsupportedWALVersion rather than being silently mis-parsed.
	dataStart, err := walFileDataStart(file)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(dataStart, io.SeekStart); err != nil {
		return nil, err
	}

	var messages []*RecoveryMessage

	// Transaction-boundary state (SQ-8). Message records that appear between a
	// Begin marker and its matching Commit marker are buffered and only emitted
	// once the Commit is read. A Begin with no Commit (crash mid-transaction or
	// a torn tail) leaves txBuf pending at EOF and is discarded — uncommitted
	// transaction operations are never applied.
	inTx := false
	var txEpoch uint64
	var txBuf []*RecoveryMessage

	// ITER5 shared-body recovery (§4). bodyByOffset maps a BodyBlock record's
	// file-relative start offset to its body bytes. Because the co-location
	// invariant writes the block FIRST in its unit, a reference record is always
	// scanned AFTER its block, so the map is populated by the time a ref resolves —
	// no forward references, no second pass. recStart tracks the file-relative
	// start of the current record, which is exactly the value a reference locator
	// carries.
	bodyByOffset := map[uint64][]byte{}
	recStart := dataStart

	for {
		// Read CRC (4) + length (4) + data (dataLen). io.ReadFull makes the byte
		// accounting exact so recStart stays synchronized with the record grammar
		// (a large BodyBlock body cannot be under-read into a false torn tail); any
		// short read at the tail (a crash mid-write) breaks out, dropping the
		// partial trailing record — CRC would reject it anyway.
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(file, hdr); err != nil {
			break
		}
		crc := binary.BigEndian.Uint32(hdr[0:4])
		dataLen := binary.BigEndian.Uint32(hdr[4:8])

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(file, data); err != nil {
			break
		}

		curRecStart := recStart
		recStart += 8 + int64(dataLen)

		// Verify CRC over [length][data]. A zero stored CRC means CRC was
		// disabled at write time — skip verification (mixed-file safe).
		if crc != 0 {
			h := crc32.NewIEEE()
			h.Write(hdr[4:8])
			h.Write(data)
			if crc != h.Sum32() {
				continue
			}
		}

		recType, payload := recordTypeAndPayload(data)

		switch recType {
		case WALRecordTypeTxBoundary:
			if len(payload) < walTxMarkerPayloadLen {
				continue
			}
			kind := payload[0]
			epoch := binary.BigEndian.Uint64(payload[1:9])
			switch kind {
			case WALTxMarkerBegin:
				inTx = true
				txEpoch = epoch
				txBuf = txBuf[:0]
			case WALTxMarkerCommit:
				// Only commit a transaction whose Begin we actually saw with a
				// matching epoch; a stray/torn commit is ignored.
				if inTx && epoch == txEpoch {
					messages = append(messages, txBuf...)
				}
				inTx = false
				txBuf = txBuf[:0]
			}

		case WALRecordTypeBodyBlock:
			// [fanoutHint u32][bodyLen u32][body]. Store the body keyed by this
			// record's start offset so a later reference record resolves it. The
			// block is NOT a deliverable message and is never emitted.
			if len(payload) < 8 {
				continue
			}
			bodyLen := binary.BigEndian.Uint32(payload[4:8])
			if 8+int(bodyLen) > len(payload) {
				continue
			}
			body := make([]byte, bodyLen) // copy out of the reused scan buffer
			copy(body, payload[8:8+int(bodyLen)])
			bodyByOffset[uint64(curRecStart)] = body

		default: // WALRecordTypeMessage
			recoveryMsg, ok := deserializeMessagePayload(payload)
			if !ok {
				continue
			}
			// Resolve a body reference against the per-file block map. On a hit,
			// materialize Body and clear BodyRef so the recovered message is a
			// self-contained inline message (checkpoint re-inlines it; segments
			// never carry a reference). On a miss (a torn unit whose block was lost)
			// skip the record defensively — at-least-once still holds because a
			// torn-tail unit's fsync never completed, so the publisher never got a
			// confirm and re-publish recreates it.
			if len(recoveryMsg.Message.BodyRef) == 8 {
				loc := binary.BigEndian.Uint64(recoveryMsg.Message.BodyRef)
				body, found := bodyByOffset[loc]
				if !found {
					continue
				}
				recoveryMsg.Message.Body = body
				recoveryMsg.Message.BodyRef = nil
			}
			if inTx {
				txBuf = append(txBuf, recoveryMsg)
			} else {
				messages = append(messages, recoveryMsg)
			}
		}
	}

	// EOF with a transaction still open: the Commit marker was never durable,
	// so these operations are uncommitted and must not be applied.
	return messages, nil
}

// createSharedWAL creates the single shared WAL for all queues
func (wm *WALManager) createSharedWAL() (*QueueWAL, error) {
	// Create shared WAL directory
	sharedDir := filepath.Join(wm.dataDir, "shared")
	if err := os.MkdirAll(sharedDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shared WAL directory: %w", err)
	}

	wal := &QueueWAL{
		name:               "shared",
		dataDir:            sharedDir,
		cfg:                wm.cfg,
		writeChan:          make(chan *writeRequest, wm.cfg.ChannelBuffer),
		ackChan:            make(chan *ackRequest, wm.cfg.ChannelBuffer),
		ackBitmap:          roaring64.New(),
		currentFileOffsets: roaring64.New(),
		oldFiles:           make(map[uint64]*walFileInfo),
		oldFileCache:       make(map[uint64]*os.File),
		offsetIndex:        make(map[uint64]*offsetLocation), // Phase 6D: Offset index for O(1) reads
		stopChan:           make(chan struct{}),
	}

	// Open first WAL file
	if err := wal.openNextFile(); err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Start background goroutines (single set for all queues!)
	wal.wg.Add(3)
	go wal.batchWriterLoop()
	go wal.cleanupLoop()
	go wal.checkpointLoop()

	return wal, nil
}

// batchWriterLoop implements group commit for synchronous WAL writes:
// it greedily coalesces every request already queued in writeChan (writers
// that arrived while the previous batch was fsyncing) into one batch, then
// flushes immediately.
//
// Flushing as soon as writeChan is drained — instead of holding a
// sub-BatchSize batch until a BatchTimeout ticker fired, as this loop
// previously did — is what keeps a lone synchronous writer fast: with the
// old ticker design every solo Write() stalled for up to BatchTimeout
// (10ms by default, ~100 writes/s), which presented as a publisher wedge
// once a queue ring crossed the spill threshold and every transient publish
// became a synchronous WAL write. Concurrent writers still amortize one
// fsync across the whole drained batch, so group-commit throughput is
// preserved; the batch size now self-tunes to arrival-rate × fsync-latency
// (capped at BatchSize) rather than arrival-rate × BatchTimeout.
func (qw *QueueWAL) batchWriterLoop() {
	defer qw.wg.Done()

	batch := make([]*writeRequest, 0, qw.cfg.BatchSize)

	// Persistent, single-owner scratch owned exclusively by this goroutine and
	// passed to flushBatch by pointer. buf is the serialization accumulator
	// (reset to buf[:0] and re-filled in place each flush — no fresh nil
	// accumulator, no geometric regrowth); positions is the per-record file
	// position scratch. Neither ever escapes this goroutine or is aliased to a
	// message body, which is what makes the reuse race-free (invariant A1 §4).
	var buf []byte
	var positions []int64

	for {
		select {
		case req := <-qw.writeChan:
			batch = append(batch, req)
			// Coalesce everything already queued, up to BatchSize.
		drain:
			for len(batch) < qw.cfg.BatchSize {
				select {
				case r := <-qw.writeChan:
					batch = append(batch, r)
				default:
					break drain
				}
			}
			qw.flushBatch(batch, &buf, &positions)
			batch = batch[:0]

		case <-qw.stopChan:
			// Final flush: drain anything still queued so no Write() caller
			// is left blocked forever on its done channel during shutdown.
			for {
				select {
				case r := <-qw.writeChan:
					batch = append(batch, r)
				default:
					if len(batch) > 0 {
						qw.flushBatch(batch, &buf, &positions)
					}
					return
				}
			}
		}
	}
}

// flushBatch writes a batch of messages to the current WAL file.
//
// bufp and positionsp point to the batch writer's persistent, single-owner
// scratch (see batchWriterLoop). The serialization accumulator is reset with
// (*bufp)[:0] and re-filled IN PLACE — each record is serialized directly onto
// the shared buffer's tail via appendMessageRecord — instead of allocating a
// fresh nil accumulator per batch and a per-message serialize buffer that is then
// re-copied in. This removes the two biggest allocators on the durable path (the
// former flushBatch re-append + serializeMessageVersioned per-message buffer) and
// the geometric regrowth of the accumulator.
//
// A1 SAFETY: the reused buffer's lifetime ends at qw.currentFile.Write()'s
// RETURN — write(2) copies the bytes into the kernel page cache, and the
// subsequent fsync flushes KERNEL state, never this Go slice. So resetting/reusing
// *bufp on the NEXT flush cannot race, corrupt, or invalidate the previous batch's
// fsync, CRC (computed during serialization, before Write), any confirm, or any
// delivery. Everything after Write — metrics, fsync, the fsync-error early return
// that leaves the index un-updated, offset-index population, offset/roll, unlock,
// and completions — is byte-for-byte unchanged, so the write→fsync→index→completion
// ordering and durability barrier are preserved.
func (qw *QueueWAL) flushBatch(batch []*writeRequest, bufp *[]byte, positionsp *[]int64) {
	if len(batch) == 0 {
		return
	}

	qw.fileMutex.Lock()

	// Get current file number and position BEFORE write
	currentFileNum := qw.fileNum.Load()
	currentFilePos := qw.fileOffset.Load()

	// Reuse the persistent serialization buffer (grow-once) and position scratch.
	buf := (*bufp)[:0]
	if cap(*positionsp) < len(batch) {
		*positionsp = make([]int64, len(batch))
	}
	positions := (*positionsp)[:len(batch)]

	// Serialize all messages in batch IN PLACE and track their positions.
	//
	// A record that fails to encode (e.g. a shortstr property >255 bytes) is
	// rolled back to its start so no partial/corrupt bytes are written, its
	// position is marked -1 (skip index + offset-set below), and its own error is
	// delivered to that caller — the rest of the batch is unaffected. serErr stays
	// nil (no allocation) on the common all-success path, preserving Stage A's
	// zero-alloc batch serialization; it is lazily allocated only when a record
	// actually fails, which the wire decoder's 255-byte bound makes near-unreachable.
	var serErr []error
	// sharedIdx collects (offset, filePosition) for the reference records of any
	// fused shared unit(s) in this batch, to be indexed post-fsync alongside the
	// ordinary records. It stays nil on the common no-unit path (the N==1 durable
	// hot path), so that path adds ZERO allocations vs today.
	var sharedIdx []sharedIndexEntry
	for i, req := range batch {
		if req.unit == nil {
			recStart := len(buf)
			positions[i] = currentFilePos + int64(recStart) // Position in file
			var e error
			buf, e = appendMessageRecord(buf, req.queueName, req.message, req.offset, qw.cfg.CRCDisabled)
			if e != nil {
				buf = buf[:recStart] // discard the failed record's partial bytes
				positions[i] = -1
				if serErr == nil {
					serErr = make([]error, len(batch))
				}
				serErr[i] = e
			}
			continue
		}

		// FUSED SHARED UNIT (ITER5, §3.3): emit the BodyBlock first (capturing its
		// file-relative start = the reference locator), then each sub as a message
		// record carrying that locator via the existing bodyKindReference arm. The
		// block's offset is NOT indexed (it is not a deliverable message); each
		// sub's (offset, position) is recorded for post-fsync index population.
		// This req occupies ONE batch index, so positions[i] is marked -1 (skipped
		// by the ordinary index loop below); its subs are indexed via sharedIdx.
		positions[i] = -1
		u := req.unit
		unitStart := len(buf)
		blockStart := currentFilePos + int64(unitStart)
		buf = appendBodyBlockRecord(buf, u.fanoutHint, u.body, qw.cfg.CRCDisabled)

		var locator [8]byte
		binary.BigEndian.PutUint64(locator[:], uint64(blockStart))

		savedSharedLen := len(sharedIdx)
		var unitErr error
		for si := range u.subs {
			sub := &u.subs[si]
			subPos := currentFilePos + int64(len(buf))
			// Drive the existing bodyKindReference writer arm by setting BodyRef to
			// the 8-byte locator, then CLEAR it immediately so the caller's in-memory
			// message stays a clean inline message (a later dead-letter/requeue
			// re-store must NOT emit a dangling reference into another file). Safe:
			// the sub message is not consumer-visible until its completion fires
			// (after this fsync), so nothing reads BodyRef concurrently here.
			sub.message.BodyRef = locator[:]
			var e error
			buf, e = appendMessageRecord(buf, sub.queueName, sub.message, sub.offset, qw.cfg.CRCDisabled)
			sub.message.BodyRef = nil
			if e != nil {
				unitErr = e
				break
			}
			sharedIdx = append(sharedIdx, sharedIndexEntry{offset: sub.offset, filePosition: subPos})
		}
		if unitErr != nil {
			// All-or-nothing per unit: discard the block + every already-written
			// sub record, drop the partial index entries, and nack every sub.
			buf = buf[:unitStart]
			sharedIdx = sharedIdx[:savedSharedLen]
			if serErr == nil {
				serErr = make([]error, len(batch))
			}
			serErr[i] = unitErr
			continue
		}
		// A shared body is now on the active file: mark it so rollFile can pin the
		// file out of checkpoint (§2). fileMutex is held for the whole flushBatch.
		qw.currentFileHasSharedBody = true
	}
	// Publish the (possibly reallocated) buffer back so the next flush reuses
	// its grown capacity. A rare oversized batch is released rather than pinned.
	if cap(buf) > maxRetainedWALBatchBytes {
		*bufp = nil
	} else {
		*bufp = buf
	}

	// Single write (O_APPEND makes this atomic across processes)
	n, err := qw.currentFile.Write(buf)
	if err != nil {
		// Record WAL write error
		if qw.metrics != nil {
			qw.metrics.RecordWALWriteError()
		}
		// Write failed - not durable, index NOT updated. Release the file lock
		// before delivering completions (async callbacks must not run under
		// fileMutex; see deliverCompletions).
		qw.fileMutex.Unlock()
		qw.deliverCompletions(batch, serErr, fmt.Errorf("WAL write failed: %w", err))
		return
	}

	// Record successful WAL writes (only records that were actually serialized).
	if qw.metrics != nil {
		for i := range batch {
			if positions[i] >= 0 {
				qw.metrics.RecordWALWrite()
			}
		}
	}

	// Fdatasync for durability (one sync per batch!)
	// Uses fdatasync on Linux (skips metadata sync for ~2-5x speedup vs fsync).
	// This is the critical durability guarantee - messages are only durable after this completes.
	//
	// When fsync is explicitly disabled (Storage.Fsync=false =>
	// WALConfig.SyncDisabled), the barrier is skipped: the write(2) above is
	// still page-cache-visible for same-process reads, so the index update and
	// completions below proceed exactly as on the durable path, but the batch is
	// not crash-durable. The fsync is NOT recorded when skipped, so
	// wal_fsync_total stays a truthful count of real syncs. WriteTxAtomic is
	// unaffected — it always fdatasyncs directly.
	var fsyncErr error
	if !qw.cfg.SyncDisabled {
		fsyncStart := time.Now()
		fsyncErr = walGroupCommitFsync(qw.currentFile)
		fsyncDuration := time.Since(fsyncStart).Seconds()

		// Record fsync metrics
		if qw.metrics != nil {
			qw.metrics.RecordWALFsync(fsyncDuration)
			if fsyncErr != nil {
				qw.metrics.RecordWALWriteError()
			}
		}
	}

	// If fsync failed, messages are NOT durable: do NOT update the index (so a
	// reader can never observe a non-durable record and no async caller ever
	// advances visibility), and deliver the error to every caller.
	if fsyncErr != nil {
		qw.fileMutex.Unlock()
		qw.deliverCompletions(batch, serErr, fsyncErr)
		return
	}

	// Durable. Update the offset index / current-file offset set / file offset
	// and roll BEFORE releasing the lock and BEFORE firing completions, so that
	// when a completion advances a message's visibility (async path), a woken
	// consumer's wal.Read finds the record. Records that failed to serialize
	// (positions[i] < 0) were never written, so they are not indexed.
	qw.offsetIndexMutex.Lock()
	for i, req := range batch {
		if positions[i] < 0 {
			continue
		}
		qw.offsetIndex[req.offset] = &offsetLocation{
			fileNum:      currentFileNum,
			filePosition: positions[i],
		}
	}
	// Fused shared-unit reference records (never the BodyBlock itself). Empty on
	// the no-unit hot path, so this loop is a zero-cost no-op there.
	for _, e := range sharedIdx {
		qw.offsetIndex[e.offset] = &offsetLocation{
			fileNum:      currentFileNum,
			filePosition: e.filePosition,
		}
	}
	qw.offsetIndexMutex.Unlock()

	// Track offsets written to current file
	qw.currentFileOffsetsMutex.Lock()
	for i, req := range batch {
		if positions[i] < 0 {
			continue
		}
		qw.currentFileOffsets.Add(req.offset)
	}
	for _, e := range sharedIdx {
		qw.currentFileOffsets.Add(e.offset)
	}
	qw.currentFileOffsetsMutex.Unlock()

	// Update offset
	newOffset := qw.fileOffset.Add(int64(n))

	// Check if we need to roll to new file
	if newOffset >= qw.cfg.FileSize {
		qw.rollFile()
	}

	// Release the file lock, THEN deliver completions. Completions fire LAST,
	// after the record is durable and indexed, so both the sync done-channel
	// send and the async onDone callback observe the true durability barrier.
	// Doing it outside fileMutex keeps the shared-writer critical section short
	// (consumers reading via readMessageAtPosition contend on fileMutex) and
	// honors the "no lock held across the callback" spirit of invariant A4.
	qw.fileMutex.Unlock()
	qw.deliverCompletions(batch, serErr, nil)
}

// deliverCompletions signals every request in the batch, iterating in enqueue
// order. batchErr is the shared write/fsync outcome for the batch (nil on
// success). serErr, when non-nil, carries a per-record serialization error
// (index i) that OVERRIDES batchErr for that record — a record that failed to
// encode is nacked with its own error even when the rest of the batch was
// durable. serErr is nil on the common all-success path (no allocation).
//
// A sync request's done is a cap-1 pooled channel (the send never blocks). An
// async request's onDone runs inline on the batch-writer goroutine; its contract
// is non-blocking (atomics + non-blocking pokes only), so it never stalls the
// shared writer. Exactly one of done/onDone is set per request. Enqueue-order
// iteration gives, for any single channel, non-decreasing confirm-tag completion
// order for single-target publishes — the property the per-channel confirm fold
// relies on.
func (qw *QueueWAL) deliverCompletions(batch []*writeRequest, serErr []error, batchErr error) {
	for i, req := range batch {
		err := batchErr
		if serErr != nil && serErr[i] != nil {
			err = serErr[i]
		}
		// A fused shared unit (ITER5): fire every sub's completion in sub order.
		// The unit is all-or-nothing, so every sub gets the same outcome (the
		// unit's serErr override if it failed to serialize, else the batch error).
		if req.unit != nil {
			for si := range req.unit.subs {
				sub := &req.unit.subs[si]
				if sub.onDone != nil {
					sub.onDone(err)
				} else if sub.done != nil {
					sub.done <- err
				}
			}
			continue
		}
		if req.onDone != nil {
			req.onDone(err)
		} else if req.done != nil {
			req.done <- err
		}
	}
}

// serializeMessageVersioned serializes ONE v4 message record into a fresh
// single-allocation buffer: [CRC][length][recordType=Message][<message payload
// v4>]. Used by the transaction slow path (WriteTxAtomic); the group-commit batch
// writer instead calls appendMessageRecord directly onto its persistent buffer.
// Returns an error (and a partial buffer the caller must discard) if the message
// contains a shortstr property longer than 255 bytes.
func (qw *QueueWAL) serializeMessageVersioned(queueName string, message *protocol.Message, offset uint64) ([]byte, error) {
	buf := make([]byte, 0, len(message.Body)+len(message.Exchange)+len(message.RoutingKey)+len(queueName)+256)
	return appendMessageRecord(buf, queueName, message, offset, qw.cfg.CRCDisabled)
}

// appendMessageRecord serializes ONE v4 message record onto the TAIL of buf (grow
// in place) and returns the grown buffer. It is the shared serializer for both
// the single-allocation serializeMessageVersioned wrapper (fresh buf, recStart
// == 0) and the group-commit batch writer (one persistent buf, many records
// packed back-to-back).
//
// STAGE A: the record is written single-pass with only the record's own CRC+len
// backpatched — never a second write pass over field bodies. Correctness of
// packing many records into one buffer rests on RECORD-RELATIVE backpatch: the
// CRC and length placeholders are patched at buf[recStart:] and the CRC is
// computed over buf[recStart+4:] (the record's own [length][data] span, which is
// at the tail because this record was just appended). recStart is an index, not a
// pointer, so it survives any growslice reallocation during the appends. The body
// is copied exactly once (the single inherent store-path copy, inside
// appendMessagePayload).
//
// On a shortstr overflow (a property >255 bytes) it returns the error together
// with the partially-grown buffer; the caller MUST roll back to recStart (the
// batch writer does) so no partial/corrupt record is ever written.
func appendMessageRecord(buf []byte, queueName string, message *protocol.Message, offset uint64, crcDisabled bool) ([]byte, error) {
	recStart := len(buf)

	// Reserve space for CRC and length (patched below, record-relative).
	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder

	// Per-record type tag immediately after the framing (v4 always writes it).
	buf = append(buf, WALRecordTypeMessage)

	buf, err := appendMessagePayload(buf, queueName, message, offset)
	if err != nil {
		return buf, err // caller discards buf[recStart:]
	}

	// Calculate actual length (excluding CRC and length fields), record-relative.
	dataLen := len(buf) - recStart - 8
	binary.BigEndian.PutUint32(buf[recStart+4:recStart+8], uint32(dataLen))

	if !crcDisabled {
		crc := crc32.ChecksumIEEE(buf[recStart+4:])
		binary.BigEndian.PutUint32(buf[recStart:recStart+4], crc)
	}

	return buf, nil
}

// appendMessagePayload writes the v4 message payload onto the tail of buf and
// returns the grown buffer. It is the SINGLE shared codec used by BOTH the WAL
// (via appendMessageRecord) and the per-queue segments (via
// serializeSegmentMessage), so the two on-disk payloads are byte-identical by
// construction. Layout (big-endian throughout):
//
//	[flags u16]                      presence bitmap (§ msgFlag* constants)
//	[queue u8-len + bytes]           WAL: queue name; segments write ""
//	[offset u64]
//	[exchange u8-len + bytes]
//	[routingKey u8-len + bytes]
//	[deliveryMode u8]
//	[bodyKind u8]  0x00 inline: [bodyLen u32][body]  | 0x01 ref: [refLen u16][locator]
//	--- optional fields, present iff their flag bit is set, in this canonical order:
//	ContentType, ContentEncoding, Headers(field-table), Priority(u8),
//	CorrelationID, ReplyTo, Expiration, MessageID, Timestamp(u64), Type, UserID,
//	AppID, ClusterID, EnqueueUnixMilli(i64) ---
//
// STAGE A: every write is an append onto the caller's buffer; messagePresenceFlags
// allocates nothing; the no-properties/no-headers hot path writes only the
// structural prefix + inline body and takes NO allocation (the Headers bit is
// clear, so EncodeFieldTable is never invoked). The single body copy is preserved.
// Returns ErrShortStringOverflow (wrapped) if any shortstr property exceeds 255
// bytes — the record is then nacked rather than silently truncated.
func appendMessagePayload(buf []byte, queueName string, message *protocol.Message, offset uint64) ([]byte, error) {
	flags := messagePresenceFlags(message)
	buf = binary.BigEndian.AppendUint16(buf, flags)

	var err error
	if buf, err = appendShort(buf, queueName); err != nil {
		return buf, err
	}
	buf = binary.BigEndian.AppendUint64(buf, offset)
	if buf, err = appendShort(buf, message.Exchange); err != nil {
		return buf, err
	}
	if buf, err = appendShort(buf, message.RoutingKey); err != nil {
		return buf, err
	}
	buf = append(buf, message.DeliveryMode)

	// Body union. ITER4 always emits the inline arm (bodyKind 0x00); the reference
	// arm (0x01) is the ITER5 seam and is only produced when BodyRef is set.
	if len(message.BodyRef) > 0 {
		if len(message.BodyRef) > 0xFFFF {
			return buf, fmt.Errorf("%w: body reference locator %d bytes exceeds 65535", ErrShortStringOverflow, len(message.BodyRef))
		}
		buf = append(buf, bodyKindReference)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(message.BodyRef)))
		buf = append(buf, message.BodyRef...)
	} else {
		buf = append(buf, bodyKindInline)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Body)))
		buf = append(buf, message.Body...) // the ONE store-path body copy
	}

	// Optional fields in canonical (descending flag-value) order, each gated by
	// its presence bit so an absent field costs nothing beyond its bit.
	if flags&msgFlagContentType != 0 {
		if buf, err = appendShort(buf, message.ContentType); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagContentEncoding != 0 {
		if buf, err = appendShort(buf, message.ContentEncoding); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagHeaders != 0 {
		buf = appendHeaders(buf, message.Headers)
	}
	if flags&msgFlagPriority != 0 {
		buf = append(buf, message.Priority)
	}
	if flags&msgFlagCorrelationID != 0 {
		if buf, err = appendShort(buf, message.CorrelationID); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagReplyTo != 0 {
		if buf, err = appendShort(buf, message.ReplyTo); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagExpiration != 0 {
		if buf, err = appendShort(buf, message.Expiration); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagMessageID != 0 {
		if buf, err = appendShort(buf, message.MessageID); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagTimestamp != 0 {
		buf = binary.BigEndian.AppendUint64(buf, message.Timestamp)
	}
	if flags&msgFlagType != 0 {
		if buf, err = appendShort(buf, message.Type); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagUserID != 0 {
		if buf, err = appendShort(buf, message.UserID); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagAppID != 0 {
		if buf, err = appendShort(buf, message.AppID); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagClusterID != 0 {
		if buf, err = appendShort(buf, message.ClusterID); err != nil {
			return buf, err
		}
	}
	if flags&msgFlagEnqueue != 0 {
		buf = binary.BigEndian.AppendUint64(buf, uint64(message.EnqueueUnixMilli))
	}
	return buf, nil
}

// messagePresenceFlags computes the v4 presence bitmap for message. A bit is set
// iff its field differs from its Go zero value ("" for strings, 0 for numbers,
// empty for Headers) — which exactly matches the wire re-encode gates
// (server/frame_helpers.go), so an unset property and a present-empty property
// are indistinguishable AND equal, the correct AMQP unset-property semantic.
// Allocation-free (a dozen comparisons), so the durable hot path stays zero-alloc.
func messagePresenceFlags(m *protocol.Message) uint16 {
	var flags uint16
	if m.ContentType != "" {
		flags |= msgFlagContentType
	}
	if m.ContentEncoding != "" {
		flags |= msgFlagContentEncoding
	}
	if len(m.Headers) > 0 {
		flags |= msgFlagHeaders
	}
	if m.Priority != 0 {
		flags |= msgFlagPriority
	}
	if m.CorrelationID != "" {
		flags |= msgFlagCorrelationID
	}
	if m.ReplyTo != "" {
		flags |= msgFlagReplyTo
	}
	if m.Expiration != "" {
		flags |= msgFlagExpiration
	}
	if m.MessageID != "" {
		flags |= msgFlagMessageID
	}
	if m.Timestamp != 0 {
		flags |= msgFlagTimestamp
	}
	if m.Type != "" {
		flags |= msgFlagType
	}
	if m.UserID != "" {
		flags |= msgFlagUserID
	}
	if m.AppID != "" {
		flags |= msgFlagAppID
	}
	if m.ClusterID != "" {
		flags |= msgFlagClusterID
	}
	if m.EnqueueUnixMilli != 0 {
		flags |= msgFlagEnqueue
	}
	return flags
}

// appendShort appends a shortstr (u8 length prefix + bytes) onto buf. It returns
// ErrShortStringOverflow (wrapped) if s exceeds 255 bytes rather than truncating
// the length into the u8 (which would silently corrupt the record). Zero-alloc.
func appendShort(buf []byte, s string) ([]byte, error) {
	if len(s) > 255 {
		return buf, fmt.Errorf("%w: length %d", ErrShortStringOverflow, len(s))
	}
	buf = append(buf, byte(len(s)))
	buf = append(buf, s...)
	return buf, nil
}

// appendHeaders appends a self-length'd AMQP field table (its own u32 length
// prefix, as produced by protocol.EncodeFieldTable) onto buf. Only called when
// the Headers presence bit is set (len(headers) > 0), so the no-headers hot path
// never reaches EncodeFieldTable and stays allocation-free. A Headers map holding
// a value the field-table encoder cannot emit (e.g. a Decimal, which the wire
// codec decodes but does not re-encode) is persisted as an empty table rather
// than corrupting the record or losing the message — a bounded, pre-existing
// encoder limitation (not introduced here).
func appendHeaders(buf []byte, headers map[string]interface{}) []byte {
	headerBytes, err := protocol.EncodeFieldTable(headers)
	if err != nil {
		return binary.BigEndian.AppendUint32(buf, 0) // empty field table
	}
	return append(buf, headerBytes...)
}

// serializeTxMarker serializes a transaction-boundary marker record (SQ-8, v2
// only): [CRC][length][type=TxBoundary][kind][epoch]. kind is Begin or Commit.
func serializeTxMarker(kind uint8, epoch uint64, crcDisabled bool) []byte {
	buf := make([]byte, 0, 8+1+walTxMarkerPayloadLen)
	buf = append(buf, 0, 0, 0, 0) // CRC placeholder
	buf = append(buf, 0, 0, 0, 0) // length placeholder
	buf = append(buf, WALRecordTypeTxBoundary)
	buf = append(buf, kind)
	buf = binary.BigEndian.AppendUint64(buf, epoch)

	dataLen := len(buf) - 8
	binary.BigEndian.PutUint32(buf[4:8], uint32(dataLen))
	if !crcDisabled {
		crc := crc32.ChecksumIEEE(buf[4:])
		binary.BigEndian.PutUint32(buf[0:4], crc)
	}
	return buf
}

// appendBodyBlockRecord serializes ONE ITER5 BodyBlock record onto the TAIL of
// buf (grow in place) and returns the grown buffer:
//
//	[CRC32][length][recordType=BodyBlock][fanoutHint u32][bodyLen u32][body]
//
// The CRC and length are backpatched record-relative (recStart is an index, so it
// survives any growslice reallocation), exactly like appendMessageRecord, so many
// records — a block and its refs — pack contiguously into one batch buffer. The
// body is copied exactly once. This helper never fails (a body's length is a u32;
// callers cap body size well below 4 GiB).
func appendBodyBlockRecord(buf []byte, fanoutHint uint32, body []byte, crcDisabled bool) []byte {
	recStart := len(buf)

	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder
	buf = append(buf, WALRecordTypeBodyBlock)
	buf = binary.BigEndian.AppendUint32(buf, fanoutHint)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(body)))
	buf = append(buf, body...)

	dataLen := len(buf) - recStart - 8
	binary.BigEndian.PutUint32(buf[recStart+4:recStart+8], uint32(dataLen))
	if !crcDisabled {
		crc := crc32.ChecksumIEEE(buf[recStart+4:])
		binary.BigEndian.PutUint32(buf[recStart:recStart+4], crc)
	}
	return buf
}

// recordTypeAndPayload splits a CRC-verified v4 record's data region into its
// 1-byte record type and the type-specific payload. Every v4 record carries the
// type tag as data[0]; a zero-length data region defaults to a message record.
func recordTypeAndPayload(data []byte) (recType uint8, payload []byte) {
	if len(data) < 1 {
		return WALRecordTypeMessage, data
	}
	return data[0], data[1:]
}

// deserializeMessagePayload parses a v4 message record's payload (everything
// after the record-type tag) into a RecoveryMessage for the crash-recovery path.
// Returns ok=false on a truncated/malformed payload so callers can skip it. An
// ITER5 body-reference record (bodyKind 0x01) returns ok=true carrying BodyRef
// (Body==nil); the caller resolves it to the co-located BodyBlock's bytes (the
// scanWALFile recovery map / a second positional ReadAt). The recovered message is
// marked Redelivered (a redelivery after a restart); the live positional read path
// clears that flag.
func deserializeMessagePayload(payload []byte) (*RecoveryMessage, bool) {
	queueName, offset, msg, ok := parseMessagePayload(payload)
	if !ok {
		return nil, false
	}
	msg.Redelivered = true
	return &RecoveryMessage{
		QueueName: queueName,
		Offset:    offset,
		Message:   msg,
	}, true
}

// parseMessagePayload is the SINGLE shared decoder for the v4 message payload,
// used by both the WAL (via deserializeMessagePayload / the positional live-read
// path) and the per-queue segments (via readSegmentMessageAt). It mirrors
// appendMessagePayload exactly. Every read is bounds-checked against len(payload);
// on ANY short read it returns ok=false rather than panicking or slicing out of
// bounds, so a torn tail (the record already passed CRC, so this is defensive) is
// skipped cleanly. The body is aliased zero-copy into the CRC-checked payload, as
// before. It does NOT set Redelivered (the caller decides).
func parseMessagePayload(payload []byte) (queueName string, offset uint64, m *protocol.Message, ok bool) {
	pos := 0
	if pos+2 > len(payload) {
		return "", 0, nil, false
	}
	flags := binary.BigEndian.Uint16(payload[pos : pos+2])
	pos += 2
	// A set continuation bit means a future format wrote a second flags word this
	// build does not understand — refuse rather than misparse.
	if flags&msgFlagContinuation != 0 {
		return "", 0, nil, false
	}

	qn, pos, ok := readShort(payload, pos)
	if !ok {
		return "", 0, nil, false
	}
	if pos+8 > len(payload) {
		return "", 0, nil, false
	}
	off := binary.BigEndian.Uint64(payload[pos : pos+8])
	pos += 8
	exchange, pos, ok := readShort(payload, pos)
	if !ok {
		return "", 0, nil, false
	}
	routingKey, pos, ok := readShort(payload, pos)
	if !ok {
		return "", 0, nil, false
	}
	if pos+1 > len(payload) {
		return "", 0, nil, false
	}
	deliveryMode := payload[pos]
	pos++
	if pos+1 > len(payload) {
		return "", 0, nil, false
	}
	bodyKind := payload[pos]
	pos++

	msg := &protocol.Message{
		Exchange:     exchange,
		RoutingKey:   routingKey,
		DeliveryMode: deliveryMode,
		DeliveryTag:  off,
	}

	switch bodyKind {
	case bodyKindInline:
		if pos+4 > len(payload) {
			return "", 0, nil, false
		}
		bodyLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		if int(bodyLen) < 0 || pos+int(bodyLen) > len(payload) {
			return "", 0, nil, false
		}
		msg.Body = payload[pos : pos+int(bodyLen)] // zero-copy alias into CRC-checked payload
		pos += int(bodyLen)
	case bodyKindReference:
		// ITER5 seam. Read the opaque locator into msg.BodyRef and FALL THROUGH to
		// parse the optional fields exactly like the inline arm, returning ok=true
		// (msg carries BodyRef, Body==nil). Resolving the reference to actual body
		// bytes is the caller's job: crash recovery (scanWALFile) resolves it from
		// the per-file bodyByOffset map; the live positional read (readMessageAt*)
		// resolves it with a second ReadAt against the co-located BodyBlock. This is
		// forward-only and safe — no shipped build emits a 0x01 arm, so the ok=true
		// behavior is observable only for ITER5-written files.
		if pos+2 > len(payload) {
			return "", 0, nil, false
		}
		refLen := binary.BigEndian.Uint16(payload[pos : pos+2])
		pos += 2
		if pos+int(refLen) > len(payload) {
			return "", 0, nil, false
		}
		msg.BodyRef = payload[pos : pos+int(refLen)]
		pos += int(refLen)
	default:
		return "", 0, nil, false // unknown body kind
	}

	// Optional fields in the same canonical order appendMessagePayload writes them.
	if flags&msgFlagContentType != 0 {
		if msg.ContentType, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagContentEncoding != 0 {
		if msg.ContentEncoding, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagHeaders != 0 {
		if pos+4 > len(payload) {
			return "", 0, nil, false
		}
		tblLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		end := pos + 4 + tblLen
		if tblLen < 0 || end > len(payload) {
			return "", 0, nil, false
		}
		// An empty decoded table normalizes to nil Headers so a headers-less
		// message reconstructs identically regardless of how it was written.
		if headers, err := protocol.DecodeFieldTable(payload[pos:end]); err == nil && len(headers) > 0 {
			msg.Headers = headers
		}
		pos = end
	}
	if flags&msgFlagPriority != 0 {
		if pos+1 > len(payload) {
			return "", 0, nil, false
		}
		msg.Priority = payload[pos]
		pos++
	}
	if flags&msgFlagCorrelationID != 0 {
		if msg.CorrelationID, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagReplyTo != 0 {
		if msg.ReplyTo, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagExpiration != 0 {
		if msg.Expiration, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagMessageID != 0 {
		if msg.MessageID, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagTimestamp != 0 {
		if pos+8 > len(payload) {
			return "", 0, nil, false
		}
		msg.Timestamp = binary.BigEndian.Uint64(payload[pos : pos+8])
		pos += 8
	}
	if flags&msgFlagType != 0 {
		if msg.Type, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagUserID != 0 {
		if msg.UserID, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagAppID != 0 {
		if msg.AppID, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagClusterID != 0 {
		if msg.ClusterID, pos, ok = readShort(payload, pos); !ok {
			return "", 0, nil, false
		}
	}
	if flags&msgFlagEnqueue != 0 {
		if pos+8 > len(payload) {
			return "", 0, nil, false
		}
		msg.EnqueueUnixMilli = int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
		pos += 8
	}

	return qn, off, msg, true
}

// readShort reads a shortstr (u8 length prefix + bytes) from payload at pos,
// returning the value, the new position, and ok=false on a short read (never
// slices out of bounds).
func readShort(payload []byte, pos int) (s string, newPos int, ok bool) {
	if pos+1 > len(payload) {
		return "", pos, false
	}
	n := int(payload[pos])
	pos++
	if pos+n > len(payload) {
		return "", pos, false
	}
	return string(payload[pos : pos+n]), pos + n, true
}

// rollFile rolls to a new WAL file when size limit reached
func (qw *QueueWAL) rollFile() {
	currentFileNum := qw.fileNum.Load()
	currentPath := qw.currentFile.Name()

	qw.currentFileOffsetsMutex.Lock()
	fileOffsets := qw.currentFileOffsets
	qw.currentFileOffsets = roaring64.New()
	qw.currentFileOffsetsMutex.Unlock()

	// ITER5 §2: capture whether the file being rolled carries a shared BodyBlock,
	// then reset for the fresh file. rollFile is only ever invoked from flushBatch
	// (and WriteTxAtomic) while fileMutex is held, so this read+reset is race-free
	// against the flushBatch writer that sets the flag.
	sharedBody := qw.currentFileHasSharedBody
	qw.currentFileHasSharedBody = false

	// Close current write file
	_ = qw.currentFile.Close()

	// Move current read handle to old file cache (it's already open, no need to reopen)
	if qw.currentReadFile != nil {
		qw.oldFileCacheMutex.Lock()
		if qw.oldFileCache == nil {
			qw.oldFileCache = make(map[uint64]*os.File)
		}
		qw.oldFileCache[currentFileNum] = qw.currentReadFile
		qw.currentReadFile = nil
		qw.oldFileCacheMutex.Unlock()
	}

	// Move to old files map
	qw.oldFilesMutex.Lock()
	qw.oldFiles[currentFileNum] = &walFileInfo{
		path:          currentPath,
		offsets:       fileOffsets,
		createdAt:     time.Now(),
		hasSharedBody: sharedBody,
	}
	qw.oldFilesMutex.Unlock()

	// Open next file
	_ = qw.openNextFile()
}

// openNextFile opens the next WAL file
func (qw *QueueWAL) openNextFile() error {
	fileNum := qw.fileNum.Add(1)
	filename := filepath.Join(qw.dataDir, fmt.Sprintf("%020d%s", fileNum, WALFileExtension))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Determine the current size so we (a) only write the file-level header on a
	// genuinely new/empty file and (b) seed fileOffset to the true end of the
	// file. Seeding from the real size keeps the offset index correct when an
	// existing file is reopened and appended to (e.g. a restart that reuses the
	// same file number, or a legacy headerless file that is being appended to).
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}
	size := stat.Size()

	// Write the file-level header (magic + version) only on a brand-new file
	// (SQ-4). The header is flushed to disk together with the first record
	// batch's fdatasync — one small write per file, never per record. Records
	// therefore begin at WALHeaderSize. A pre-existing file keeps whatever
	// header (or none, for legacy v0 files) it already has; we never write a
	// second header, and records continue at the real end of file.
	if size == 0 {
		header := make([]byte, 0, WALHeaderSize)
		header = append(header, WALMagic...)
		header = append(header, WALFormatVersion)
		n, werr := file.Write(header)
		if werr != nil {
			_ = file.Close()
			return fmt.Errorf("failed to write WAL file header: %w", werr)
		}
		size = int64(n)
	}

	// Open a read-only handle for ReadAt (currentFile is O_WRONLY, can't ReadAt)
	readFile, err := os.Open(filename)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to open WAL read handle: %w", err)
	}

	// Close previous read handle if any
	if qw.currentReadFile != nil {
		_ = qw.currentReadFile.Close()
	}

	qw.currentFile = file
	qw.currentReadFile = readFile
	qw.fileOffset.Store(size)
	return nil
}

// walFileDataStart inspects the beginning of an open WAL file and reports the
// byte offset at which record data begins. ITER4 is a CLEAN BREAK — this build
// reads ONLY v4 files:
//   - A file that starts with WALMagic and whose version byte is WALVersion4:
//     records begin at WALHeaderSize.
//   - A file that starts with WALMagic but carries any other version (v1/v2/v3 or
//     a future version): ErrUnsupportedWALVersion (loud, never a silent skip —
//     silently ignoring it could drop durable messages).
//   - A file with no WALMagic (a pre-v4 headerless v0 file, or foreign bytes):
//     ErrUnsupportedWALVersion. Pre-v4 records are NOT structurally back-parsed.
//   - A file too short to hold a header (0 bytes, or a crash between file
//     creation and the header write): treated as empty (start 0, no records).
func walFileDataStart(f *os.File) (dataStart int64, err error) {
	hdr := make([]byte, WALHeaderSize)
	n, rerr := f.ReadAt(hdr, 0)
	if n < WALHeaderSize {
		// Too short to contain a v4 header. A genuinely empty file has no records
		// to recover; parse it as empty. Non-EOF read errors are surfaced.
		if rerr != nil && rerr != io.EOF {
			return 0, rerr
		}
		return 0, nil
	}
	if string(hdr[:len(WALMagic)]) != WALMagic {
		return 0, fmt.Errorf("%w: missing SQWAL magic (pre-v4 headerless file)", ErrUnsupportedWALVersion)
	}
	if v := hdr[len(WALMagic)]; v != WALVersion4 {
		return 0, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedWALVersion, v, WALFormatVersion)
	}
	return int64(WALHeaderSize), nil
}

// cleanupLoop processes ACKs and deletes old WAL files
func (qw *QueueWAL) cleanupLoop() {
	defer qw.wg.Done()

	ticker := time.NewTicker(qw.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case ack := <-qw.ackChan:
			qw.bitmapMutex.Lock()
			qw.ackBitmap.Add(ack.offset)
			qw.bitmapMutex.Unlock()

			// Clean up offset index (Phase 6D)
			// NOTE: In shared WAL, we still use offset alone as key
			// because offsets are globally unique across queues
			qw.offsetIndexMutex.Lock()
			delete(qw.offsetIndex, ack.offset)
			qw.offsetIndexMutex.Unlock()

		case <-ticker.C:
			qw.tryDeleteOldFiles()

		case <-qw.stopChan:
			return
		}
	}
}

// tryDeleteOldFiles deletes WAL files where all messages are ACKed,
// or where the retention period has expired (with best-effort checkpoint).
func (qw *QueueWAL) tryDeleteOldFiles() {
	qw.oldFilesMutex.Lock()
	defer qw.oldFilesMutex.Unlock()

	qw.bitmapMutex.RLock()
	defer qw.bitmapMutex.RUnlock()

	for fileNum, info := range qw.oldFiles {
		// Check if all actual message offsets in this file are ACKed
		// Uses bitmap intersection: file offsets ⊆ ackBitmap iff |file ∩ ack| == |file|
		allAcked := info.offsets != nil &&
			info.offsets.GetCardinality() > 0 &&
			roaring64.And(info.offsets, qw.ackBitmap).GetCardinality() == info.offsets.GetCardinality()

		// Check time-based retention
		expired := qw.cfg.RetentionPeriod > 0 &&
			!info.createdAt.IsZero() &&
			time.Since(info.createdAt) > qw.cfg.RetentionPeriod

		if allAcked {
			_ = os.Remove(info.path)
			qw.closeOldFileHandle(fileNum)
			delete(qw.oldFiles, fileNum)
		} else if expired {
			// Force-checkpoint unACKed messages to segments before deletion
			qw.forceCheckpointFile(info)
			_ = os.Remove(info.path)
			qw.closeOldFileHandle(fileNum)
			delete(qw.oldFiles, fileNum)
		}
	}
}

// forceCheckpointFile is a best-effort checkpoint of unACKed messages from an
// expired WAL file to segments before force-deletion. Errors are logged but
// do not prevent file deletion (the retention contract takes priority).
// NOTE: Caller must hold bitmapMutex (RLock) and oldFilesMutex (Lock).
func (qw *QueueWAL) forceCheckpointFile(info *walFileInfo) {
	segMgr := qw.getSegmentMgr()
	if segMgr == nil {
		return
	}

	messages, err := qw.scanWALFile(info.path)
	if err != nil {
		return
	}

	// Group unACKed messages by queue
	byQueue := make(map[string][]*RecoveryMessage)
	for _, msg := range messages {
		if !qw.ackBitmap.Contains(msg.Offset) {
			byQueue[msg.QueueName] = append(byQueue[msg.QueueName], msg)
		}
	}

	// Write to segments (best-effort — errors are ignored)
	for queueName, queueMsgs := range byQueue {
		_ = segMgr.CheckpointBatch(queueName, queueMsgs)
	}
}

// readMessage reads a message from shared WAL by queue+offset
// Phase 6D: Uses O(1) offset index instead of sequential scan
// queueName is used for validation (entries include queue name in shared WAL)
func (qw *QueueWAL) readMessage(queueName string, offset uint64) (*protocol.Message, error) {
	// Check offset index first (Phase 6D: O(1) lookup!)
	qw.offsetIndexMutex.RLock()
	location, ok := qw.offsetIndex[offset]
	qw.offsetIndexMutex.RUnlock()

	if ok {
		// Fast path: Use index to jump directly to message location
		return qw.readMessageAtPosition(queueName, location.fileNum, location.filePosition, offset)
	}

	// Fallback: Sequential scan for messages not yet indexed (rare)
	// This handles the case where index isn't populated yet
	return qw.readMessageSequential(queueName, offset)
}

// getOldFileHandle returns an open file handle for an old WAL file, using a cache
// to avoid repeated open/close syscalls. Callers must call the returned unlock
// function after done with the file (ReadAt calls) to prevent concurrent close.
func (qw *QueueWAL) getOldFileHandle(fileNum uint64) (*os.File, func(), error) {
	// Fast path: cache hit (RLock allows concurrent readers)
	qw.oldFileCacheMutex.RLock()
	if file, ok := qw.oldFileCache[fileNum]; ok {
		return file, qw.oldFileCacheMutex.RUnlock, nil
	}
	qw.oldFileCacheMutex.RUnlock()

	// Cache miss: get file path first (separate lock to avoid lock order inversion)
	qw.oldFilesMutex.RLock()
	info, ok := qw.oldFiles[fileNum]
	filePath := ""
	if ok {
		filePath = info.path
	}
	qw.oldFilesMutex.RUnlock()
	if !ok {
		return nil, func() {}, fmt.Errorf("WAL file not found for fileNum %d", fileNum)
	}

	// Open file outside any locks
	file, err := os.Open(filePath)
	if err != nil {
		return nil, func() {}, err
	}

	// Add to cache (write lock)
	qw.oldFileCacheMutex.Lock()
	defer qw.oldFileCacheMutex.Unlock()

	// Double-check: another goroutine may have opened and cached it
	if existing, ok := qw.oldFileCache[fileNum]; ok {
		_ = file.Close() // close our redundant handle
		return existing, func() {}, nil
	}

	if qw.oldFileCache == nil {
		qw.oldFileCache = make(map[uint64]*os.File)
	}
	qw.oldFileCache[fileNum] = file
	return file, func() {}, nil
}

// closeOldFileHandle closes and removes a cached file handle (called when old file is deleted)
func (qw *QueueWAL) closeOldFileHandle(fileNum uint64) {
	qw.oldFileCacheMutex.Lock()
	defer qw.oldFileCacheMutex.Unlock()
	if file, ok := qw.oldFileCache[fileNum]; ok {
		_ = file.Close()
		delete(qw.oldFileCache, fileNum)
	}
}

// readMessageAtPosition reads a message at a specific file position (O(1))
// Uses ReadAt (pread) for positional I/O — no Seek needed, no file open/close per read.
// Current file: uses the already-open handle. Old files: uses a file handle cache.
func (qw *QueueWAL) readMessageAtPosition(queueName string, fileNum uint64, filePosition int64, expectedOffset uint64) (*protocol.Message, error) {
	var file *os.File

	// Load currentFileNum inside fileMutex to prevent TOCTOU race with rollFile
	qw.fileMutex.Lock()
	currentFileNum := qw.fileNum.Load()
	if fileNum == currentFileNum {
		file = qw.currentReadFile
		qw.fileMutex.Unlock()
		if file == nil {
			return nil, fmt.Errorf("WAL current read file not open for fileNum %d", fileNum)
		}
	} else {
		qw.fileMutex.Unlock()
		// Reading from old file — use file handle cache
		var unlock func()
		var cacheErr error
		file, unlock, cacheErr = qw.getOldFileHandle(fileNum)
		if cacheErr != nil {
			return nil, cacheErr
		}
		defer unlock()
	}

	// Read CRC + length in a single ReadAt (8 bytes at filePosition)
	header := make([]byte, 8)
	_, err := file.ReadAt(header, filePosition)
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	// Read data portion in a single ReadAt (dataLen bytes at filePosition+8)
	data := make([]byte, dataLen)
	_, err = file.ReadAt(data, filePosition+8)
	if err != nil {
		return nil, err
	}

	// Verify CRC (calculated over [length_bytes][data]). A zero stored CRC
	// means CRC was disabled at write time — skip verification.
	if crc != 0 {
		h := crc32.NewIEEE()
		h.Write(header[4:8])
		h.Write(data)
		if crc != h.Sum32() {
			return nil, fmt.Errorf("CRC mismatch for offset %d (expected %d, got %d)", expectedOffset, crc, h.Sum32())
		}
	}

	// The offset index is populated only by writes made in the current process,
	// which always use the current v4 format. Strip the per-record type tag,
	// confirm it is a message record, and reconstruct via the shared payload
	// parser so the live read path recovers the full basic.properties set exactly
	// like crash recovery does.
	recType, payload := recordTypeAndPayload(data)
	if recType != WALRecordTypeMessage {
		return nil, fmt.Errorf("unexpected WAL record type %d for offset %d", recType, expectedOffset)
	}

	rm, ok := deserializeMessagePayload(payload)
	if !ok {
		return nil, fmt.Errorf("invalid WAL data: malformed message record for offset %d", expectedOffset)
	}

	// Validate queue name matches (for multi-queue isolation) and the offset.
	if rm.QueueName != queueName {
		return nil, fmt.Errorf("queue name mismatch: expected %s, got %s", queueName, rm.QueueName)
	}
	if rm.Offset != expectedOffset {
		return nil, fmt.Errorf("offset mismatch: expected %d, got %d", expectedOffset, rm.Offset)
	}

	// ITER5: a shared-body record carries a body REFERENCE. Resolve it with a
	// SECOND ReadAt for the co-located BodyBlock on the SAME open handle, still
	// under the handle-lifetime protection this function holds (current file: the
	// held currentReadFile; old file: the cache RLock / kept handle), so a
	// concurrent close cannot race the block read (§5.3). The block is in the same
	// file as the reference by the co-location invariant, so `file` is correct.
	if len(rm.Message.BodyRef) == 8 {
		blockOffset := int64(binary.BigEndian.Uint64(rm.Message.BodyRef))
		body, berr := readBodyBlockAt(file, blockOffset)
		if berr != nil {
			return nil, fmt.Errorf("failed to resolve shared body for offset %d: %w", expectedOffset, berr)
		}
		rm.Message.Body = body
		rm.Message.BodyRef = nil
	}

	// This is a positional re-read, not a post-restart redelivery, so the
	// message is not marked redelivered (deserializeMessagePayload defaults it
	// to true for the crash-recovery path).
	rm.Message.Redelivered = false
	return rm.Message, nil
}

// readBodyBlockAt reads the ITER5 BodyBlock record at file offset `position` and
// returns a COPY of its body bytes. It verifies the record framing (CRC) and the
// record type, so a corrupt or misdirected locator surfaces as an error rather
// than delivering wrong bytes. Used to resolve a body reference on the live
// positional read path (readMessageAtPosition) and the sequential fallback.
func readBodyBlockAt(file *os.File, position int64) ([]byte, error) {
	header := make([]byte, 8)
	if _, err := file.ReadAt(header, position); err != nil {
		return nil, err
	}
	crc := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	data := make([]byte, dataLen)
	if _, err := file.ReadAt(data, position+8); err != nil {
		return nil, err
	}

	if crc != 0 {
		h := crc32.NewIEEE()
		h.Write(header[4:8])
		h.Write(data)
		if crc != h.Sum32() {
			return nil, fmt.Errorf("CRC mismatch for body block at %d", position)
		}
	}

	recType, payload := recordTypeAndPayload(data)
	if recType != WALRecordTypeBodyBlock {
		return nil, fmt.Errorf("record at %d is type %d, not a body block", position, recType)
	}
	if len(payload) < 8 {
		return nil, fmt.Errorf("truncated body block at %d", position)
	}
	bodyLen := binary.BigEndian.Uint32(payload[4:8])
	if 8+int(bodyLen) > len(payload) {
		return nil, fmt.Errorf("body block at %d claims %d body bytes, has %d", position, bodyLen, len(payload)-8)
	}
	body := make([]byte, bodyLen)
	copy(body, payload[8:8+int(bodyLen)])
	return body, nil
}

// readMessageSequential does sequential scan fallback (slow, only for unindexed messages)
func (qw *QueueWAL) readMessageSequential(queueName string, offset uint64) (*protocol.Message, error) {
	// Try to read from old files first
	qw.oldFilesMutex.RLock()
	defer qw.oldFilesMutex.RUnlock()

	for _, fileInfo := range qw.oldFiles {
		if fileInfo.offsets != nil && fileInfo.offsets.Contains(offset) {
			// Found the file containing this offset
			return qw.readMessageFromFile(queueName, fileInfo.path, offset)
		}
	}

	// Not in old files, try the current file — but ONLY for an offset that has
	// been durably written (recorded in currentFileOffsets, which flushBatch
	// populates only AFTER a successful fsync). This gates out a record whose
	// bytes reached the file but whose fsync FAILED: its offset is not in
	// currentFileOffsets and it is not indexed, so it must not be returned. That
	// closes the async durable path's fsync-error exposure — the frontier
	// advances head past such a tag, so its cursor is claimed, and without this
	// gate a raw scan would find the un-fsynced bytes and deliver a message that
	// was nacked (never durable). A successfully-written offset is always in the
	// offset index by the time it is claimable, so it takes the O(1) fast path
	// and never reaches this scan; the gate therefore only excludes the
	// non-durable case.
	qw.currentFileOffsetsMutex.Lock()
	durableInCurrent := qw.currentFileOffsets.Contains(offset)
	qw.currentFileOffsetsMutex.Unlock()
	if !durableInCurrent {
		return nil, fmt.Errorf("message not in WAL")
	}

	qw.fileMutex.Lock()
	currentPath := ""
	if qw.currentFile != nil {
		currentPath = qw.currentFile.Name()
	}
	qw.fileMutex.Unlock()

	if currentPath != "" {
		return qw.readMessageFromFile(queueName, currentPath, offset)
	}

	return nil, fmt.Errorf("message not in WAL")
}

// readMessageFromFile reads a message from a specific WAL file (sequential scan for shared WAL format)
func (qw *QueueWAL) readMessageFromFile(queueName string, filePath string, offset uint64) (*protocol.Message, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Skip the file-level header before scanning records. Only v4 files are
	// accepted; a non-v4/headerless file returns ErrUnsupportedWALVersion.
	dataStart, err := walFileDataStart(file)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(dataStart, io.SeekStart); err != nil {
		return nil, err
	}

	// Sequential scan through shared WAL entries. ITER5: accumulate BodyBlock
	// bodies keyed by their record start offset (a block always precedes any
	// record that references it, per the co-location invariant), so a matched
	// reference record resolves its body from the map in this same pass.
	bodyByOffset := map[uint64][]byte{}
	recStart := dataStart
	for {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(file, hdr); err != nil {
			return nil, err
		}
		crc := binary.BigEndian.Uint32(hdr[0:4])
		dataLen := binary.BigEndian.Uint32(hdr[4:8])

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}

		curRecStart := recStart
		recStart += 8 + int64(dataLen)

		// Verify CRC (calculated over [length_bytes][data]). A zero stored CRC
		// means CRC was disabled at write time — skip verification.
		if crc != 0 {
			h := crc32.NewIEEE()
			h.Write(hdr[4:8])
			h.Write(data)
			if crc != h.Sum32() {
				return nil, fmt.Errorf("CRC mismatch in sequential scan")
			}
		}

		recType, payload := recordTypeAndPayload(data)
		if recType == WALRecordTypeBodyBlock {
			if len(payload) >= 8 {
				bodyLen := binary.BigEndian.Uint32(payload[4:8])
				if 8+int(bodyLen) <= len(payload) {
					body := make([]byte, bodyLen)
					copy(body, payload[8:8+int(bodyLen)])
					bodyByOffset[uint64(curRecStart)] = body
				}
			}
			continue
		}
		if recType != WALRecordTypeMessage {
			// Transaction-boundary marker (or any non-message record): skip.
			continue
		}

		rm, ok := deserializeMessagePayload(payload)
		if !ok {
			return nil, fmt.Errorf("malformed message record in sequential scan")
		}

		// Check if this is the message we're looking for
		if rm.Offset == offset && rm.QueueName == queueName {
			if len(rm.Message.BodyRef) == 8 {
				loc := binary.BigEndian.Uint64(rm.Message.BodyRef)
				body, found := bodyByOffset[loc]
				if !found {
					return nil, fmt.Errorf("shared body block not found for offset %d", offset)
				}
				rm.Message.Body = body
				rm.Message.BodyRef = nil
			}
			rm.Message.Redelivered = false
			return rm.Message, nil
		}

		// Not the right message, continue scanning
	}
}

// checkpointLoop periodically checkpoints old WAL files to segments
func (qw *QueueWAL) checkpointLoop() {
	defer qw.wg.Done()

	// Use CheckpointInterval from WALConfig directly — avoids a race where
	// segmentMgr is nil at goroutine start (it's set later via SetSegmentManager).
	ticker := time.NewTicker(qw.cfg.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qw.performCheckpoint()

		case <-qw.stopChan:
			// Final checkpoint before exit
			qw.performCheckpoint()
			return
		}
	}
}

// sharedBodyBackstopFired reports whether a skipped shared-body file has aged
// past SharedBodyMaxPinAge and must now be re-inlined + deleted (§2). When the
// knob is 0 the backstop is disabled and reclaim relies on all-ack (whole-file)
// or RetentionPeriod (tryDeleteOldFiles). A permanently-unacked reference then
// pins its file — holding exactly ONE body copy, never the N-copy blowup.
func (qw *QueueWAL) sharedBodyBackstopFired(info *walFileInfo) bool {
	return qw.cfg.SharedBodyMaxPinAge > 0 &&
		!info.createdAt.IsZero() &&
		time.Since(info.createdAt) > qw.cfg.SharedBodyMaxPinAge
}

// performCheckpoint moves messages from old WAL files to segments.
// Only operates on old/rolled files — never touches the current active file.
// For each old file: scan messages, filter out ACKed ones, write unACKed
// messages to segments, then delete the WAL file and clean up the offset index.
func (qw *QueueWAL) performCheckpoint() {
	segMgr := qw.getSegmentMgr()
	if segMgr == nil {
		return
	}

	// Snapshot old files to avoid holding the lock during I/O
	qw.oldFilesMutex.RLock()
	filesToCheckpoint := make(map[uint64]*walFileInfo, len(qw.oldFiles))
	for num, info := range qw.oldFiles {
		filesToCheckpoint[num] = info
	}
	qw.oldFilesMutex.RUnlock()

	if len(filesToCheckpoint) == 0 {
		return
	}

	for fileNum, info := range filesToCheckpoint {
		// ITER5 cold-tail hardening (§2): a rolled file that carries a shared
		// BodyBlock is SKIPPED (left in oldFiles), so the shared body survives as
		// ONE copy across arbitrarily many checkpoints — reclaimed whole by
		// tryDeleteOldFiles once all its refs ack, instead of re-inlining into N
		// segment copies now. The SharedBodyMaxPinAge backstop overrides the skip
		// once the file is old enough, re-inlining via the path below (severing all
		// external refs before delete — no premature free). tryDeleteOldFiles is
		// unaffected: whole-file reclaim on all-ack still applies.
		if info.hasSharedBody && !qw.sharedBodyBackstopFired(info) {
			continue
		}

		// Scan the WAL file
		messages, err := qw.scanWALFile(info.path)
		if err != nil {
			continue
		}

		// Group unACKed messages by queue
		byQueue := make(map[string][]*RecoveryMessage)
		qw.bitmapMutex.RLock()
		for _, msg := range messages {
			if !qw.ackBitmap.Contains(msg.Offset) {
				byQueue[msg.QueueName] = append(byQueue[msg.QueueName], msg)
			}
		}
		qw.bitmapMutex.RUnlock()

		// Write unACKed messages to segments
		checkpointOK := true
		for queueName, queueMsgs := range byQueue {
			if err := segMgr.CheckpointBatch(queueName, queueMsgs); err != nil {
				checkpointOK = false
				break
			}
		}

		if !checkpointOK {
			continue
		}

		// Checkpoint succeeded — delete WAL file and clean up
		_ = os.Remove(info.path)
		qw.closeOldFileHandle(fileNum)

		// Remove from oldFiles map
		qw.oldFilesMutex.Lock()
		delete(qw.oldFiles, fileNum)
		qw.oldFilesMutex.Unlock()

		// Clean up offset index entries for this file's offsets
		if info.offsets != nil {
			qw.offsetIndexMutex.Lock()
			it := info.offsets.Iterator()
			for it.HasNext() {
				offset := it.Next()
				delete(qw.offsetIndex, offset)
			}
			qw.offsetIndexMutex.Unlock()
		}
	}
}

// close stops background goroutines and closes files
func (qw *QueueWAL) close() {
	close(qw.stopChan)
	qw.wg.Wait()

	// Close cached old file handles
	qw.oldFileCacheMutex.Lock()
	for _, file := range qw.oldFileCache {
		_ = file.Close()
	}
	qw.oldFileCache = nil
	qw.oldFileCacheMutex.Unlock()

	qw.fileMutex.Lock()
	defer qw.fileMutex.Unlock()

	if qw.currentFile != nil {
		_ = qw.currentFile.Sync()
		_ = qw.currentFile.Close()
	}
	if qw.currentReadFile != nil {
		_ = qw.currentReadFile.Close()
	}
}
