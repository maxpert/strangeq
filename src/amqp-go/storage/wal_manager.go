package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	// WAL settings
	DefaultWALBatchSize    = 1000                  // Messages per batch
	DefaultWALBatchTimeout = 10 * time.Millisecond // Legacy batch flush timeout (no longer gates flushing; see WALConfig.BatchTimeout)
	DefaultWALFileSize     = 64 * 1024 * 1024      // 64 MB per WAL file (segstore v1)
	WALFileExtension       = ".wal"

	// maxRetainedWALBatchBytes bounds how large EACH of the pipelined group
	// commit writer's two persistent, reused stagedBatch serialization
	// buffers may grow before it is released back to the GC (WS-F §1.4:
	// stagedBatch.reset drops the buffer when its capacity exceeds this).
	// The buffers are normally retained across batches (grow-once) to avoid
	// re-allocating the batch accumulator every group commit; this cap
	// guards against a rare, unusually large batch permanently pinning a
	// very large buffer. Since WS-F phase 1 the stager also seals every
	// batch at walBatchSealBytes (64 MiB) + one record, so steady-state
	// buffers stay well below this cap and it only catches outliers; the
	// retained worst case is two buffers, ~2x this value (§1.4, accepted).
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

	// SyncDisabled, when true, skips the group-commit fdatasync in
	// commitBatch (the async publish durability barrier). Zero value (false)
	// = fsync ON, the durable default, so DefaultWALConfig() and
	// zero-value WALConfig{} stay durable. Set only via WALConfigFromEngine
	// from the user's Storage.Fsync flag. It does NOT relax WriteTxAtomic,
	// which always fdatasyncs directly — transactions remain durable even
	// when this is set.
	SyncDisabled bool

	// CRCDisabled, when true, skips CRC32 computation on the write path (writes
	// zero in the CRC field) and skips verification on the read path (when the
	// stored CRC is zero). Zero value (false) = CRC ON, the safe default, so
	// DefaultWALConfig() and zero-value WALConfig{} keep integrity checks. Set
	// via WALConfigFromEngine from the user's Storage.CRCCheck flag. The record
	// format is unchanged: the CRC field is always present. The read path
	// handles mixed files by verifying non-zero CRCs and skipping zero ones.
	CRCDisabled bool

	// UnsafeRecovery, when true (--unsafe-recovery), lets the shared WAL be
	// constructed on a directory holding a file this build cannot parse,
	// DISCARDING that file's contents, instead of refusing. Zero value (false)
	// = refuse, which is the safe default for DefaultWALConfig() and every
	// zero-value WALConfig{}. Set only via WALConfigFromEngine.
	UnsafeRecovery bool

	// CompactionThreshold is the fraction of a sealed WAL file's durable bytes
	// that must be ACKNOWLEDGED before the file is compacted (WI7). The
	// comparison is strict: exactly this fraction does not compact.
	//
	// A NON-POSITIVE value means "use DefaultWALCompactionThreshold", which
	// keeps the zero-value WALConfig as safe as its other zero-value fields
	// (no fsync skip, no CRC skip): 0 would mean "compact as soon as one
	// byte is dead", which is a maintenance storm, not a default.
	CompactionThreshold float64

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

		CompactionThreshold: DefaultWALCompactionThreshold,
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

	// ACK tracking is volatile and scoped by queue. The bitmaps are read gates
	// only; physical segmentRecord.acked state drives accounting/reclamation.
	ackBitmaps  map[string]*roaring64.Bitmap
	bitmapMutex sync.RWMutex

	// LAYOUT BOUNDARY — read this before adding or removing a field ABOVE it.
	//
	// This position used to hold `_walLayoutPad uint64`, an 8-byte placeholder
	// occupying the bytes vacated by the dead `lastCheckpoint` field. Deleting
	// that field shifted every field below by -8, which moved fileMutex out of
	// the start of cache line 3 and into the tail of line 2, sharing that line
	// with fileNum and fileOffset — all three touched on the committer's
	// group-commit path. That is a hot-path layout change smuggled in by a
	// cold-path deletion, and the zero-regression rule is measured against the
	// layout at 60b2d21, so the layout is restored rather than re-measured.
	//
	// The placeholder is GONE because those 8 bytes are now held by
	// WALConfig.CompactionThreshold, which grew WALConfig by exactly 8 bytes
	// (the WAL tier's byte-ratio compaction threshold, WI7). The growth and
	// the removal are both above `currentFile`, so the group-commit hot group's
	// offsets are byte-for-byte the ones measured at 60b2d21 (64-bit,
	// sync.Mutex == 8 bytes) and no A/B is owed.
	//
	// The invariant: the TOTAL number of bytes between `cfg` and `currentFile`
	// must not change. storage/wal_layout_test.go asserts the resulting
	// offsets at runtime; if a field above this point is ever added or removed,
	// that test fails and the change owes a single-hunk A/B rather than an
	// assertion that it "cannot matter".

	// Current active file
	currentFile             *os.File
	currentReadFile         *os.File // Read-only handle for ReadAt (currentFile is O_WRONLY)
	fileNum                 atomic.Uint64
	fileOffset              atomic.Int64
	fileMutex               sync.Mutex
	currentFileOffsets      *roaring64.Bitmap
	currentFileOffsetsMutex sync.Mutex

	// currentFileHasSharedBody (ITER5 §2) records that the active file carries at
	// least one BodyBlock record. It is set under fileMutex by commitBatch when a
	// batch carrying a fused shared unit is successfully written, restored on a
	// failed write/fsync, and captured into walFileInfo.hasSharedBody
	// (then reset) by rollFile. performCheckpoint skips a rolled file with this set
	// so a shared body survives as ONE copy across arbitrarily many checkpoints
	// (until all refs ack -> whole-file reclaim, or the SharedBodyMaxPinAge backstop
	// fires). Guarded by fileMutex (always held in commitBatch and rollFile).
	currentFileHasSharedBody bool

	// Old inactive files
	oldFiles      map[uint64]*walFileInfo
	oldFilesMutex sync.RWMutex

	// File handle cache for old WAL files (avoids open/close per read)
	oldFileCache      map[uint64]*os.File
	oldFileCacheMutex sync.RWMutex

	// Physical record index. offsetIndex publishes the newest copy for a
	// logical queue/tag; recordCopies retains every physical position.
	offsetIndex      map[recordKey]*segmentRecord
	recordCopies     map[recordKey]map[recordPos]*segmentRecord
	offsetIndexMutex sync.RWMutex

	// Metrics collector
	metrics WALMetrics

	// Background goroutines
	stopChan chan struct{}
	wg       sync.WaitGroup

	// txEpoch issues a monotonically increasing id per atomic transaction write
	// (SQ-8). It pairs a Begin marker with its Commit marker so recovery can
	// detect a stray/torn commit whose begin is missing.
	txEpoch atomic.Uint64

	// --- WS-C: write-error repair / roll state. Guarded by fileMutex. ---
	// Every field below is appended AFTER txEpoch: the layout test pins only
	// the fields above, and all of these are cold-path.

	// poisoned is non-nil when the current file's size != fileOffset and the
	// divergence could not be repaired. While poisoned, no byte is written to
	// the diverged file; ensureWritableLocked tries to roll it away first.
	poisoned error
	// consecutiveFailures counts failed batches/txs in a row; reset on success.
	consecutiveFailures int
	// degraded is set when consecutiveFailures >= walDegradedAfterFailures or
	// the file is poisoned; errors then carry ErrWALUnavailable. Cleared by a
	// successful roll (the WAL is writable again) or a successful batch.
	degraded bool
	// rollBackoff / nextRollAttempt bound roll retries: a failed roll is
	// retried on a later flush once the backoff window expires, inline — no
	// timer goroutine (acceptance rule 9).
	rollBackoff     time.Duration
	nextRollAttempt time.Time // zero = may attempt now
	// rollFailures counts consecutive failed roll attempts, for log throttling.
	rollFailures int
	// fileClosed is set by close() under fileMutex; commitBatch and
	// writeTxAtomic refuse with ErrWALClosed after it.
	fileClosed bool

	// --- WS-C: close race (C9) ---
	// enqueueMu/closing make "Close has begun" visible to enqueuers. Every
	// successful writeChan send happens-before closing = true (RWMutex), so
	// after close(stopChan) the writer's final drain observes every enqueued
	// request and no send can arrive after it.
	enqueueMu sync.RWMutex
	closing   bool // guarded by enqueueMu

	// logger reports the WAL's cold-path durability failures (poison, roll
	// backoff, degraded transitions). nil = silent. Wired by
	// WALManager.SetLogger.
	logger atomicLogger

	// currentInv is the active file's physical-record inventory. Appended after
	// the pinned hot-path layout; it becomes walFileInfo.inv on roll.
	currentInv *segmentInventory

	// --- WS-F phase 1: pipelined group commit. Appended AFTER logger (the
	// last field the pre-pipeline layout pinned), never above txEpoch, so
	// the hot-group offsets wal_layout_test.go pins are untouched. ---

	// commitCh hands a sealed stagedBatch from the stager
	// (batchWriterLoop) to the committer (walCommitLoop). UNBUFFERED by
	// design (§1.1): a hand-off completes only when the committer has fully
	// finished the previous batch — write, fsync, index, roll, completions,
	// and the free <- sb return — so write(i+1) is never issued before
	// fsync(i) returns (D3) and at most one unfsynced batch ever exists
	// beyond the durable end.
	commitCh chan *stagedBatch
	// free returns staged batches from the committer to the stager, one
	// buffer per outstanding hand-off. Capacity 2 because the stager owns
	// at most one and the committer at most one of the exactly
	// walPipelineBatchCount stagedBatch objects, so a free <- sb can never
	// block.
	free chan *stagedBatch

	// pipelineMu guards pipelineBatches (the registry of the ≤2 stagedBatch
	// objects, created by the stager) and each object's handoffSnapshot
	// metadata, for pipelineBufsForTest. TEST-ONLY observability; its only
	// production cost is one lock per batch hand-off.
	pipelineMu      sync.Mutex
	pipelineBatches []*stagedBatch
}

// WALBatchSealBytesDefault is the default byte-based WAL batch seal (64 MiB):
// the stager stops adding requests to a batch once its serialization buffer
// reaches this size. Exported for the WS-I Phase B publish-inflight lease,
// whose default budget G is 4 × WALBatchSealBytesDefault = 256 MiB (a batch in
// fsync, a staging batch, and slack).
const WALBatchSealBytesDefault = 64 << 20 // 64 MiB

// Package tunables for the WS-C roll/retry machinery. Unexported vars so
// tests can shrink them; every test that mutates one restores it with
// t.Cleanup and mutates it before the first write (the values are read under
// fileMutex, so a mutation concurrent with a flush would be a data race).
var (
	walRollBackoffMin        = 10 * time.Millisecond
	walRollBackoffMax        = 1 * time.Second
	walDegradedAfterFailures = 8

	// walBatchSealBytes (WS-F phase 1 §1.4) is the byte-based batch seal:
	// the stager stops adding requests to a batch once its serialization
	// buffer reaches this size, so each staged buffer is bounded at
	// walBatchSealBytes + one record and steady-state growth never crosses
	// maxRetainedWALBatchBytes (the b4000 free/regrow cliff). A single
	// record larger than the seal forms its own batch. Test-shrinkable like
	// the roll backoff vars; a test that shrinks it must do so before the
	// manager is created (the stager reads it outside every lock).
	// Initialised from the exported WALBatchSealBytesDefault (WS-I Phase B:
	// the default publish-inflight lease budget is 4× that constant).
	walBatchSealBytes = WALBatchSealBytesDefault
)

// walPipelineBatchCount is the exact number of stagedBatch objects per
// QueueWAL (WS-F §1.4: exactly two, allocated lazily by the stager, ownership
// moving only over commitCh/free). It keeps nextStagedBatch's lazy
// allocation bounded.
const walPipelineBatchCount = 2

// sharedIndexEntry records where a fused shared unit's reference record landed
// so the committer can populate the offset index for it post-fsync (the
// BodyBlock record itself is never indexed). See commitBatch.
type sharedIndexEntry struct {
	queueName string
	offset    uint64
	bytes     int64
	bodyRef   int64
	// filePosition is the position RELATIVE to the staged buffer's start
	// while the batch is owned by the stager (WS-F §1.1); commitBatch binds
	// it to an absolute file position (base + filePosition) under
	// fileMutex before indexing.
	filePosition int64
}

// handoffSnapshot is TEST-ONLY observability metadata that replaces the old
// []byte root-of-backing-array snapshot. It captures enough to compare buffer
// identity across hand-off rounds without retaining the backing array.
type handoffSnapshot struct {
	HandoffLen  int
	HandoffCap  int
	HandoffBase uintptr
	HandoffGen  int64
}

// stagedBatch is ONE reusable group-commit batch of the pipelined writer
// (WS-F phase 1 §1.1). Exactly walPipelineBatchCount objects exist per
// QueueWAL; ownership moves only over commitCh/free (channel happens-before
// is the whole synchronization story), so no buffer is ever touched by two
// goroutines at once. The stager serializes requests onto buf AS THEY ARRIVE
// and records every position buf-RELATIVE (relPos, sharedRel, refPatch);
// the committer binds the base under fileMutex and translates to absolute
// file positions at commit time (§1.2), which is what lets a roll, a
// truncate-repair, a poison-roll or an interleaved writeTxAtomic between
// staging and commit never invalidate a staged batch.
type stagedBatch struct {
	// reqs are the writeRequests of this batch, in enqueue order.
	reqs []*writeRequest
	// buf is the packed, back-to-back serialization of the batch's records.
	buf []byte
	// relPos[i] is reqs[i]'s record start RELATIVE to buf[0]; -1 = the
	// request was not written (a per-record serialize failure, or a fused
	// shared unit, which is indexed via sharedRel instead).
	relPos      []int64
	recordBytes []int64
	// serErr, when non-nil, carries a per-request serialization error
	// (index i, kept len-aligned with reqs) that overrides the batch error
	// for that request's completion. Lazily allocated on the first failure,
	// nil on the common all-success path (no allocation, as in flushBatch's
	// day).
	serErr []error
	// sharedRel collects the reference records of fused shared units
	// (filePosition RELATIVE, bound at commit).
	sharedRel []sharedIndexEntry
	blocks    []stagedBodyBlock
	// refPatch records every shared-unit BodyRef locator to bind at commit:
	// the stager wrote the block's buf-RELATIVE position into the locator
	// bytes; the committer patches base+blockRel in and recomputes the
	// record's CRC (§1.2).
	refPatch []refPatch
	// hasShared is true iff the batch serialized at least one fused shared
	// unit successfully; the committer sets currentFileHasSharedBody under
	// fileMutex iff this is set after a successful write.
	hasShared bool

	// handoffSnapshot captures the buf metadata at the stager's most recent
	// hand-off, published under QueueWAL.pipelineMu for pipelineBufsForTest.
	// Unlike a []byte snapshot this does NOT root the backing array, so the
	// oversized-buffer release path (reset's maxRetainedWALBatchBytes guard)
	// is observable via nonzero HandoffBase without retention. TEST-ONLY.
	// HandoffGen is incremented monotonically at every hand-off of this object.
	handoffSnapshot handoffSnapshot
}

// refPatch is one shared-unit reference locator awaiting its commit-time
// binding. All fields are buf-relative indices into the owning stagedBatch:
// the ref record spans [recStart, recEnd), its 8 locator bytes sit at
// locAt, and blockRel is the BodyBlock record's start relative to buf[0].
// locAt is derived from bodyRefLocatorOffset (pinned by
// TestSharedUnit_LocatorPatchEqualsDirectSerialize), not from byte counting
// at append time, so a layout change cannot silently drift it.
type refPatch struct {
	recStart, recEnd, locAt, blockRel int
}

type stagedBodyBlock struct {
	relPos int64
	bytes  int64
}

type writeRequest struct {
	queueName string
	message   *protocol.Message
	offset    uint64
	// Exactly one of done / onDone is set:
	//   - done  : the synchronous caller (Write, tx staging, transient spill,
	//     tests) blocks on this cap-1 pooled channel until its batch is fsynced.
	//   - onDone : the ASYNC caller (WriteAsync) supplies a completion callback
	//     that the committer (commitBatch) invokes, in enqueue order, AFTER the
	//     batch is durable (fdatasync returned) AND the offset index is
	//     populated. The callback runs on the shared committer goroutine and
	//     MUST be non-blocking
	//     (atomics + non-blocking pokes only) — it must never do socket I/O,
	//     fsync, or hold a lock across I/O (invariant A4), or it re-creates the
	//     per-connection fsync stall this design removes.
	done   chan error
	onDone func(error)

	// unit, when non-nil, makes this ONE writeChan item a fused shared-body unit
	// (ITER5, §3): the stager serializes it as one BodyBlock record followed by
	// N reference records, all within this single request so they land in ONE
	// file / ONE fsync (co-location invariant). unit is mutually exclusive with
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
	inv           *segmentInventory

	// --- WS-C (C1/C2/C5) durability bookkeeping, appended below the pinned
	// fields. durableSize and damagedTail are written once, at roll time,
	// before the entry is published under oldFilesMutex, so any read through
	// the published entry is ordered by construction. checkpointed and
	// retentionFailLogged are MUTATED after publication and are therefore
	// only ever accessed under oldFilesMutex: tryReclaimOldFiles reads and
	// writes them under Lock, and performCheckpoint copies checkpointed out
	// under the RLock of its map snapshot (walCheckpointTarget) instead of
	// reading it in its unlocked loop body. ---

	// compactionAttempted guards ONE compaction attempt per file (WI7). It is
	// claimed with a compare-and-swap, so two concurrent maintenance passes
	// cannot compact the same file twice, and it is RESET after a successful
	// compaction so a file that accumulates more dead bytes later can be
	// compacted again. A FAILED attempt leaves it set: compaction reads and
	// rewrites a whole file, so retrying a file that cannot be compacted would
	// repeat that cost on every tick forever.
	compactionAttempted atomic.Bool

	// durableSize is the number of bytes known durable and intact. -1 (the
	// inherited value set by rebuildBootState) means the whole file.
	durableSize int64
	// damagedTail is true when bytes beyond durableSize are a nacked partial
	// batch that could not be truncated. The file is still checkpointed (up
	// to durableSize) and unlinked once that succeeds.
	damagedTail bool
}

// NewWALManager creates a new WAL manager with shared WAL using default config
func NewWALManager(dataDir string) (*WALManager, error) {
	return NewWALManagerWithConfig(dataDir, DefaultWALConfig())
}

// NewWALManagerWithConfig creates a new WAL manager with custom config
func NewWALManagerWithConfig(dataDir string, cfg WALConfig) (*WALManager, error) {
	walDir := filepath.Join(dataDir, "wal")
	if err := mkdirAllDurable(walDir); err != nil {
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
// commitBatch send to block forever).
func putDoneChannel(pool *sync.Pool, ch chan error) {
	select {
	case <-ch: // drain any stale value
	default:
	}
	pool.Put(ch)
}

// enqueue hands req to the batch writer, refusing fast once Close has begun.
//
// C9 race fix: the entry points used to select between `writeChan <- req` and
// `<-stopChan`; after close(stopChan) both cases could be ready, so a send
// could win the select AFTER the writer's stop arm drained the channel and
// returned — that request then sat in the buffer forever, hanging Write on
// <-doneChan and losing an async onDone.
//
// closing is observed under enqueueMu: every successful send
// happens-before closing = true (RWMutex), and close() closes stopChan only
// after every in-flight sender has released the RLock. The writer's final
// drain therefore observes every enqueued request, and no send can arrive
// after it. A sender blocked on a full writeChan still makes progress: the
// writer is guaranteed alive (stopChan is not yet closed) and never takes
// enqueueMu, and completion callbacks are non-blocking by contract.
func (qw *QueueWAL) enqueue(req *writeRequest) error {
	qw.enqueueMu.RLock()
	defer qw.enqueueMu.RUnlock()
	if qw.closing {
		return ErrWALClosed
	}
	qw.writeChan <- req
	return nil
}

// Write writes a message to the shared WAL (synchronous batch write)
// Blocks until the batch containing this message is fsynced to disk.
// This ensures proper AMQP durability semantics (Option 2: Batch Synchronous).
func (wm *WALManager) Write(queueName string, message *protocol.Message, offset uint64) error {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return ErrWALClosed
	}

	// Get a response channel from the pool (avoids make(chan error, 1) per write)
	doneChan := getDoneChannel(&doneChannelPool)
	defer putDoneChannel(&doneChannelPool, doneChan)

	if err := sw.enqueue(&writeRequest{
		queueName: queueName,
		message:   message,
		offset:    offset,
		done:      doneChan,
	}); err != nil {
		return err
	}

	// Block waiting for batch to be fsynced
	// This provides proper durability guarantee before returning
	return <-doneChan
}

// WriteAsync enqueues a message for durable group-commit exactly like Write,
// but returns as soon as the request is queued instead of blocking the caller
// until the fsync completes. The supplied onDone is invoked by the committer
// (commitBatch) after the batch containing this message is fsynced to disk
// AND the offset index is populated (success => onDone(nil)), or with the
// write/fsync error on failure (index NOT updated). Completions for a batch
// fire in enqueue order.
//
// This decouples the durable publish path from the per-connection frame
// processor: pipelined publishes fill writeChan so one F_FULLFSYNC amortises
// over the whole drained batch (see the design in the async-WAL proposal).
// onDone runs on the shared committer goroutine and MUST be non-blocking
// (see writeRequest.onDone). The blocking writeChan send is preserved, so
// writeChan's capacity remains the durable in-flight memory bound.
func (wm *WALManager) WriteAsync(queueName string, message *protocol.Message, offset uint64, onDone func(error)) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		if onDone != nil {
			onDone(ErrWALClosed)
		}
		return
	}

	if err := sw.enqueue(&writeRequest{
		queueName: queueName,
		message:   message,
		offset:    offset,
		onDone:    onDone,
	}); err != nil && onDone != nil {
		onDone(err)
	}
}

// WriteSharedAsync enqueues a fused shared-body durable write (ITER5, §3): ONE
// BodyBlock record carrying body a single time, plus one reference record per
// sub, as a SINGLE writeChan item. Because a single request is never split
// across batches (the stager serializes one request into exactly one
// stagedBatch) and a batch never rolls mid-commit (the committer rolls only
// AFTER the whole-batch Write+fsync, inside commitBatch), the block and all
// N refs are provably in ONE file and made durable by ONE fdatasync
// (co-location invariant + A1). Each sub's onDone/done fires — in sub order
// — only AFTER the unit is durable AND indexed, exactly like WriteAsync.
// All-or-nothing: a serialize error rolls the whole unit back and nacks
// every sub. body must be the shared body every sub references (the caller
// keeps each sub.message.Body pointing at it for in-memory delivery; the
// on-disk record for each sub is a reference).
func (wm *WALManager) WriteSharedAsync(subs []sharedSub, body []byte, fanoutHint uint32) error {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return ErrWALClosed
	}
	if len(subs) == 0 {
		return nil
	}
	return sw.enqueue(&writeRequest{
		unit: &sharedUnit{
			body:       body,
			fanoutHint: fanoutHint,
			subs:       subs,
		},
	})
}

// walGroupCommitFsync is the fsync applied by the group-commit committer
// (commitBatch). It is swappable via an atomic pointer so tests can gate the
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
		if !isWALFileName(e) {
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
		if walBlankRecordHeader(binary.BigEndian.Uint32(hdr[0:4]), dataLen) {
			break // zero-filled tail, not a run of zero-length records
		}
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

// repairAfterFailedWriteLocked repairs the WAL file after a failed batch/tx
// write or fsync: truncate back to the pre-write offset (which is >= the
// header and equals the durable end, I4), make the truncation durable, and
// verify with Stat. Precondition: fileMutex is held and pre is the fileOffset
// captured before the write.
//
// WHY TRUNCATE ON FSYNC FAILURE, DELETING BYTES THAT MAY BE ON DISK: on
// Linux a failed fsync can leave dirty pages marked clean ("fsyncgate").
// The batch has been nacked; leaving the bytes creates (a) a resurrected
// nacked record at restart, (b) for a tx, a resurrected nacked commit, and
// (c) once more data is appended after possibly-lost pages, a zero/stale
// hole followed by data — exactly the interior pattern that refuses boot.
// Truncating removes the region from the page cache and the file; the
// publisher retries and at-least-once is preserved. The truncate happens
// even when n == 0: one syscall that guards a kernel/FUSE layer reporting an
// error after a partial append.
//
// If the repair itself fails, the file is POISONED (never written again; the
// roll is attempted immediately) and the caller reports Repaired=false.
func (qw *QueueWAL) repairAfterFailedWriteLocked(pre int64, op string, cause error) *WALWriteError {
	werr := &WALWriteError{Op: op, Cause: cause}
	err := walTruncate(qw.currentFile, pre, !qw.cfg.SyncDisabled)
	if err == nil {
		st, serr := qw.currentFile.Stat()
		switch {
		case serr != nil:
			err = fmt.Errorf("stat after truncate: %w", serr)
		case st.Size() != pre:
			err = fmt.Errorf("size %d after truncate to %d", st.Size(), pre)
		}
	}
	if err == nil {
		// Belt-and-braces: fileOffset was never advanced past pre, but
		// restoring it explicitly keeps I3 true by construction.
		qw.fileOffset.Store(pre)
		werr.Repaired = true
		return werr
	}
	qw.poisonLocked(fmt.Errorf("WAL file %s diverged after failed %s and could not be truncated: %w",
		qw.currentFile.Name(), op, err))
	// First attempt is immediate (ignoreBackoff): if the roll works, the very
	// next batch is served by a fresh file.
	_ = qw.tryRollLocked(true)
	return werr
}

// poisonLocked marks the current file as diverged. While poisoned, no byte is
// written to it (ensureWritableLocked refuses or rolls first). The diverged
// file STAYS OPEN FOR READS: every index entry for it points below the
// pre-write offset, which is intact, and read-ahead is bounded at the durable
// end (C7). On a successful roll it is retired with durableSize/damagedTail
// and the poison clears.
func (qw *QueueWAL) poisonLocked(cause error) {
	qw.poisoned = cause
	qw.degraded = true
	qw.logError("WAL file diverged after a failed write and could not be repaired; rolling it away (the file stays readable and is checkpointed only up to its durable end)",
		interfaces.LogField{Key: "error", Value: cause.Error()},
		interfaces.LogField{Key: "file", Value: qw.currentFile.Name()})
}

// ensureWritableLocked refuses to write while the current file is poisoned
// and tries to roll it away first. Precondition: fileMutex is held.
func (qw *QueueWAL) ensureWritableLocked() error {
	if qw.poisoned == nil {
		return nil
	}
	if err := qw.tryRollLocked(false); err != nil {
		// Fail fast, no disk I/O on the diverged file. The roll is retried on
		// the next batch once the backoff window expires.
		return fmt.Errorf("%w: %w", ErrWALUnavailable, qw.poisoned)
	}
	return nil // roll replaced the poisoned file; poisoned cleared inside
}

// failBatchLocked accounts one failed batch/tx, escalating to the typed
// ErrWALUnavailable signal once the WAL is degraded (poisoned, or the
// failure streak crossed walDegradedAfterFailures). While a writable file
// exists there is NO request short-circuit: a cleared fault heals on the very
// next batch. Short-circuiting happens only while poisoned (ensureWritableLocked).
func (qw *QueueWAL) failBatchLocked(e *WALWriteError) error {
	qw.consecutiveFailures++
	if qw.consecutiveFailures == 1 || qw.consecutiveFailures%100 == 0 {
		qw.logError("WAL batch failed; the batch was nacked and its partial bytes removed",
			interfaces.LogField{Key: "op", Value: e.Op},
			interfaces.LogField{Key: "repaired", Value: e.Repaired},
			interfaces.LogField{Key: "error", Value: e.Cause.Error()},
			interfaces.LogField{Key: "streak", Value: qw.consecutiveFailures})
	}
	if !qw.degraded && (qw.poisoned != nil || qw.consecutiveFailures >= walDegradedAfterFailures) {
		qw.degraded = true
		qw.logError("WAL DEGRADED: publishes are being nacked with ErrWALUnavailable-typed errors; the WAL self-heals on the next successful batch or roll",
			interfaces.LogField{Key: "streak", Value: qw.consecutiveFailures})
	}
	if qw.degraded {
		return fmt.Errorf("%w: %w", ErrWALUnavailable, e)
	}
	return e
}

// noteSuccessLocked records a durable success: the failure streak resets and
// a degraded WAL recovers (logged once per transition). Precondition:
// fileMutex is held.
func (qw *QueueWAL) noteSuccessLocked() {
	if qw.degraded {
		qw.degraded = false
		qw.logWarn("WAL recovered: a durable batch succeeded and publishes are confirmed again")
	}
	qw.consecutiveFailures = 0
}

// Health reports the WAL's write-path health: nil when healthy, or an error
// wrapping ErrWALUnavailable when the WAL is degraded or poisoned. Cold path
// (readiness wiring is a follow-up, spec §17). A closed manager reports nil:
// it is not sick, it is gone.
func (wm *WALManager) Health() error {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return nil
	}
	sw.fileMutex.Lock()
	defer sw.fileMutex.Unlock()
	if sw.degraded || sw.poisoned != nil {
		cause := sw.poisoned
		if cause == nil {
			cause = fmt.Errorf("%d consecutive write failures", sw.consecutiveFailures)
		}
		return fmt.Errorf("%w: %w", ErrWALUnavailable, cause)
	}
	return nil
}

// writeTxAtomic durably writes a set of message records as one all-or-nothing
// transaction unit (SQ-8): a Begin marker, the records, then a Commit marker,
// appended contiguously and fsynced exactly once. Recovery only applies the
// records if the trailing Commit marker is present, so a crash before the fsync
// completes (or a torn tail) leaves the whole transaction uncommitted.
//
// This is deliberately a SLOW PATH that is independent of the group-commit
// pipeline (batchWriterLoop/walCommitLoop) — transaction commit favors
// correctness over throughput and must not be interleaved with unrelated
// messages beyond the fileMutex serialization, so the records are written
// directly under fileMutex rather than through writeChan.
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

	// The transaction slow path bypasses writeChan, so the enqueue gate cannot
	// protect it: refuse under fileMutex once Close has shut the file down.
	if qw.fileClosed {
		return ErrWALClosed
	}
	// C1: never write to a diverged (poisoned) file; roll it away first.
	if err := qw.ensureWritableLocked(); err != nil {
		return err
	}

	currentFileNum := qw.fileNum.Load()
	startPos := qw.fileOffset.Load()
	epoch := qw.txEpoch.Add(1)

	// Serialize [Begin][record...][Commit] into a single contiguous buffer and
	// record each message record's start position for the offset index.
	buf := serializeTxMarker(WALTxMarkerBegin, epoch, qw.cfg.CRCDisabled)
	positions := make([]int64, len(records))
	recordBytes := make([]int64, len(records))
	for i, rec := range records {
		positions[i] = startPos + int64(len(buf))
		recBytes, serErr := qw.serializeMessageVersioned(rec.QueueName, rec.Message, rec.Offset)
		if serErr != nil {
			// A record that cannot be encoded (e.g. a shortstr property >255
			// bytes) fails the WHOLE transaction before anything is written, so
			// the all-or-nothing guarantee holds and no corrupt record is emitted.
			return fmt.Errorf("WAL transaction serialize failed: %w", serErr)
		}
		recordBytes[i] = int64(len(recBytes))
		buf = append(buf, recBytes...)
	}
	buf = append(buf, serializeTxMarker(WALTxMarkerCommit, epoch, qw.cfg.CRCDisabled)...)

	// Single write; through the walWrite hook seam (TEST-ONLY faults;
	// default is f.Write). A short write (n < len(buf) with no error) is
	// normalised to io.ErrShortWrite so the repair path is taken either way.
	n, err := walWrite(qw.currentFile, buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if qw.metrics != nil {
			qw.metrics.RecordWALWriteError()
		}
		// C1: the whole contiguous tx unit is [startPos, EOF) — one truncate
		// to startPos removes it and nothing else. Recovery never sees a
		// half tx from a HANDLED error (a crash's torn Begin is already
		// discarded by scanWALFile).
		e := qw.failBatchLocked(qw.repairAfterFailedWriteLocked(startPos, "tx-write", err))
		qw.assertOffsetInvariantLocked()
		return e
	}

	// Single fsync makes the whole bracketed unit durable. The Commit marker is
	// the last thing written, so if this fsync does not complete the tail
	// (Commit) is lost and recovery discards the transaction. walTxFsync is a
	// hook seam whose default is exactly the previous direct fdatasyncFile.
	//
	// C1: a FAILED fsync no longer leaves the unit on disk. The client was
	// told the commit failed; keeping the bytes would resurrect a nacked
	// commit after restart and seed interior corruption once more data
	// follows possibly-lost pages (fsyncgate). Truncate to startPos instead.
	fsyncStart := time.Now()
	fsyncErr := walTxFsync(qw.currentFile)
	if qw.metrics != nil {
		qw.metrics.RecordWALFsync(time.Since(fsyncStart).Seconds())
		if fsyncErr != nil {
			qw.metrics.RecordWALWriteError()
		}
	}
	if fsyncErr != nil {
		e := qw.failBatchLocked(qw.repairAfterFailedWriteLocked(startPos, "tx-fsync", fsyncErr))
		qw.assertOffsetInvariantLocked()
		return e
	}
	if qw.metrics != nil {
		for range records {
			qw.metrics.RecordWALWrite()
		}
	}

	// Durable: publish the records to the offset index and current-file offset
	// set so they are readable/ackable exactly like batch-written messages.
	inv := qw.currentInv
	if inv == nil {
		inv = newSegmentInventory(startPos)
		qw.currentInv = inv
	}
	inv.durableBytes += int64(n)
	qw.offsetIndexMutex.Lock()
	for i, rec := range records {
		r := &segmentRecord{
			pos:       recordPos{segmentID: currentFileNum, position: positions[i]},
			queueName: rec.QueueName, deliveryTag: rec.Offset,
			bytes: recordBytes[i], bodyRef: -1, committed: true,
		}
		inv.addRecord(r)
		qw.indexRecordLocked(r)
	}
	qw.offsetIndexMutex.Unlock()

	qw.currentFileOffsetsMutex.Lock()
	for _, rec := range records {
		qw.currentFileOffsets.Add(rec.Offset)
	}
	qw.currentFileOffsetsMutex.Unlock()

	newOffset := qw.fileOffset.Add(int64(n))
	qw.noteSuccessLocked()
	qw.assertOffsetInvariantLocked()
	if newOffset >= qw.cfg.FileSize {
		qw.maybeRollLocked()
	}
	return nil
}

// Acknowledge marks a message as ACKed. The read gate is updated before the
// physical copies are settled, and both are complete when this method returns.
func (wm *WALManager) Acknowledge(queueName string, offset uint64) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return
	}

	sw.bitmapMutex.Lock()
	if sw.ackBitmaps == nil {
		sw.ackBitmaps = make(map[string]*roaring64.Bitmap)
	}
	bitmap := sw.ackBitmaps[queueName]
	if bitmap == nil {
		bitmap = roaring64.New()
		sw.ackBitmaps[queueName] = bitmap
	}
	bitmap.Add(offset)
	sw.bitmapMutex.Unlock()

	key := recordKey{queueName: queueName, deliveryTag: offset}
	sw.offsetIndexMutex.RLock()
	for _, r := range sw.recordCopies[key] {
		r.markAck()
	}
	sw.offsetIndexMutex.RUnlock()
}

// PurgeQueue acknowledges every physical copy of each WAL record in the
// inclusive queue/tag band. It returns each logical tag for which this call won
// at least one previously-unacknowledged physical copy, at most once.
func (wm *WALManager) PurgeQueue(queueName string, minTag, maxTag uint64) []uint64 {
	if minTag > maxTag {
		return nil
	}
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return nil
	}

	var newlyAcked, gated []uint64
	sw.offsetIndexMutex.RLock()
	for key, copies := range sw.recordCopies {
		if key.queueName != queueName || key.deliveryTag < minTag || key.deliveryTag > maxTag {
			continue
		}
		gated = append(gated, key.deliveryTag)
		won := false
		for _, record := range copies {
			if record.markAck() {
				won = true
			}
		}
		if won {
			newlyAcked = append(newlyAcked, key.deliveryTag)
		}
	}
	sw.offsetIndexMutex.RUnlock()

	// bitmapMutex is leaf-only: update the volatile read gate after releasing
	// the physical-index lock, matching the lock contract of Acknowledge.
	sw.bitmapMutex.Lock()
	if sw.ackBitmaps == nil {
		sw.ackBitmaps = make(map[string]*roaring64.Bitmap)
	}
	bitmap := sw.ackBitmaps[queueName]
	if bitmap == nil {
		bitmap = roaring64.New()
		sw.ackBitmaps[queueName] = bitmap
	}
	for _, tag := range gated {
		bitmap.Add(tag)
	}
	sw.bitmapMutex.Unlock()

	sort.Slice(newlyAcked, func(i, j int) bool { return newlyAcked[i] < newlyAcked[j] })
	return newlyAcked
}

// isAcknowledged is the leaf-only, queue-scoped read gate. Acknowledgement is
// process-volatile: an empty map after boot deliberately permits redelivery.
func (qw *QueueWAL) isAcknowledged(queueName string, offset uint64) bool {
	qw.bitmapMutex.RLock()
	bitmap := qw.ackBitmaps[queueName]
	acked := bitmap != nil && bitmap.Contains(offset)
	qw.bitmapMutex.RUnlock()
	return acked
}

// IsAcknowledged reports whether THIS INCARNATION acknowledged queueName/tag.
func (wm *WALManager) IsAcknowledged(queueName string, offset uint64) bool {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	return sw != nil && sw.isAcknowledged(queueName, offset)
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
	pos       recordPos
}

// RecoverFromWAL scans all WAL files and returns unacknowledged messages
// This is called during broker startup to recover messages after a crash
// Phase 3: Crash recovery implementation
//
// IT SCANS EVERY FILE AND ENUMERATES EVERY FAULT BEFORE RETURNING. A fatal
// fault is confined to the file that produced it: that file is QUARANTINED —
// named, counted, its records abandoned — and every other file in the
// directory is still opened, scanned and recovered.
//
// This used to `return` from inside the file loop on the first fatal fault.
// Measured consequence (review-3 B-1, against the real binary): a six-file
// directory holding 271,080 confirmed durable messages recovered ZERO with
// --unsafe-recovery, from a single flipped bit, because every file sorting
// after the damaged one was never opened, never scanned and NEVER ENUMERATED
// AS A FAULT — so the artifact list the refusal message printed was not the
// set of artifacts that would be lost, and its cost statement understated the
// loss by 271,080 messages. Two properties follow from scanning to the end and
// they are the reason it is done this way:
//
//   - The cost statement is TRUE. What the message says was discarded is
//     exactly what was discarded.
//   - The refusal is ONE COMPLETE REPORT rather than a bisect-by-restart. Ra
//     stops at the first bad record because its WAL is one file; a shared WAL
//     directory can enumerate all of them, which ref-rabbitmq.md §1 names as
//     the one improvement available over Ra here.
func (wm *WALManager) RecoverFromWAL() ([]*RecoveryMessage, error) {
	if wm.sharedWAL == nil {
		return nil, interfaces.FatalFault("wal-scan", filepath.Join(wm.dataDir, "shared"),
			"the shared WAL was never initialized, so no durable record in this directory can be read",
			"every durable message in the WAL directory is abandoned", nil)
	}

	winners := make(map[recordKey]*RecoveryMessage)
	var faults []error

	// Get list of all WAL files
	sharedDir := filepath.Join(wm.dataDir, "shared")
	files, err := os.ReadDir(sharedDir)
	if err != nil {
		return nil, interfaces.FatalFault("wal-scan", sharedDir,
			"the shared WAL directory could not be enumerated, so recovery cannot know which files exist",
			"every durable message in the WAL directory is abandoned", err)
	}

	// Scan each WAL file. isWALFileName is the SINGLE enumeration predicate for
	// this directory (canon rule 12) — rebuildBootState used to accept only
	// <digits>.wal while this loop accepted any *.wal, and the looser one was
	// the fatal one, so a file the boot-state rebuild could not even see could
	// still refuse the boot from here.
	for _, file := range files {
		if !isWALFileName(file) {
			continue
		}

		filePath := filepath.Join(sharedDir, file.Name())
		messages, serr := wm.sharedWAL.scanWALFile(filePath)
		if serr != nil {
			fault := classifyWALScanError(filePath, serr)
			faults = append(faults, fault)
			if interfaces.OutcomeOf(fault) == interfaces.RecoveryFatal {
				// QUARANTINE, not abort. This file cannot be interpreted, so
				// nothing is taken from it — but every other file in the
				// directory is still scanned, still enumerated, and still
				// recovered. See the function doc.
				continue
			}
			if messages == nil {
				continue
			}
		}

		// Select the newest physical copy before considering acknowledgement.
		// Filtering each copy independently could revive an older duplicate after
		// the winning copy had been settled.
		for _, msg := range messages {
			key := recordKey{queueName: msg.QueueName, deliveryTag: msg.Offset}
			if current := winners[key]; current == nil || recordPositionNewer(msg.pos, current.pos) {
				winners[key] = msg
			}
		}
	}

	recoveredMessages := make([]*RecoveryMessage, 0, len(winners))
	qw := wm.sharedWAL
	qw.offsetIndexMutex.RLock()
	for key, msg := range winners {
		winner := qw.offsetIndex[key]
		if winner != nil && winner.pos != msg.pos {
			// The index knows a DIFFERENT copy of this key, so the scanned copy
			// is the older one: never revive it as a fallback.
			continue
		}
		if winner == nil {
			// The index has no entry for this key, but the record was just read
			// from a durable WAL file on disk (a file the boot index did not
			// cover, or a record written after the index was built). There is no
			// evidence it was ever acknowledged, and refusing it would lose a
			// CONFIRMED durable record silently. Serve it: a redelivery is
			// recoverable and AMQP models it.
			recoveredMessages = append(recoveredMessages, msg)
			continue
		}
		if !winner.acked.Load() {
			recoveredMessages = append(recoveredMessages, msg)
		}
	}
	qw.offsetIndexMutex.RUnlock()
	sort.Slice(recoveredMessages, func(i, j int) bool {
		if recoveredMessages[i].QueueName != recoveredMessages[j].QueueName {
			return recoveredMessages[i].QueueName < recoveredMessages[j].QueueName
		}
		return recoveredMessages[i].Offset < recoveredMessages[j].Offset
	})
	return recoveredMessages, joinFaults(faults)
}

// classifyWALScanError turns a raw scan error into a classified fault. Errors
// that scanWALFile already classified are passed through unchanged — the site
// that knows the semantics keeps the decision.
func classifyWALScanError(filePath string, err error) error {
	if len(interfaces.FaultsOf(err)) > 0 {
		return err
	}
	if errors.Is(err, ErrUnsupportedWALVersion) {
		return interfaces.FatalFault("wal-scan", filePath,
			"this WAL file's framing version is not one this build can parse, so not one byte of it can be interpreted",
			"every durable message in this WAL file is abandoned; the file is left on disk untouched, and every OTHER WAL file in the directory is still recovered",
			err)
	}
	// An open/read failure on a file that IS a WAL file. Skipping it silently
	// (the previous behaviour) drops every durable message it holds while
	// recovery still reports success.
	return interfaces.DegradedFault("wal-scan", filePath,
		"this WAL file could not be read and is quarantined for this boot",
		"every durable message in this WAL file is abandoned for as long as it stays unreadable; the file is left on disk untouched, and every OTHER WAL file in the directory is still recovered",
		err)
}

// joinFaults collapses accumulated faults into a single error, or nil when
// none of them gates the boot. errors.Join's multi-error Unwrap is what
// interfaces.FaultsOf walks, so every fault stays individually addressable by
// an operator.
//
// BENIGN FAULTS ARE NOT RETURNED. "Benign" means "proceeds silently", and this
// package's error returns are read by callers — performCheckpoint,

// SetSegmentManager is a no-op stub retained for test compatibility.
// Checkpoint copying has been removed (segstore v1); segments are drained
// independently. This will be deleted in WI8.
func (wm *WALManager) SetSegmentManager(sm *SegmentManager) {}

// performCheckpoint is a no-op stub retained for test compatibility.
// Checkpoint copying has been removed (segstore v1). This will be deleted in WI8.
func (qw *QueueWAL) performCheckpoint() {} // forceCheckpointFile, and every existing test of RecoverFromWAL — as "did
// something go wrong". Handing them a non-nil error for a torn tail, which is
// the ordinary state of a WAL after any unclean shutdown, would make the
// normal case look like a failure. The benign classification still exists and
// is still produced at the site that knows the semantics; it simply has no
// consumer inside storage, which has no logger. Plumbing benign faults to one
// is logged for Step 7.
func joinFaults(faults []error) error {
	gating := faults[:0:0]
	for _, f := range faults {
		if interfaces.OutcomeOf(f).GatesBoot() {
			gating = append(gating, f)
		}
	}
	if len(gating) == 0 {
		return nil
	}
	return errors.Join(gating...)
}

// walBlankRecordHeader reports whether an 8-byte record header is all zeros.
//
// NO RECORD THIS BUILD WRITES CAN LOOK LIKE THIS: every record's payload
// carries at least a one-byte type tag, so dataLen == 0 is unreachable from the
// write path (asserted at runtime by
// TestWALTail_TheWritePathNeverProducesAZeroLengthRecord). A zero header is
// therefore not data — it is the start of a zero region, which is what a
// kernel death leaves behind in blocks that never reached the platter.
//
// It exists so the FOUR walks over this grammar (scanWALFile, indexWALFile,
// readMessageFromFile, scanWALFileRecordCounts) cannot disagree about where a
// file's data ends (canon rule 12). Before it, a zero region that happened to
// start on a record boundary parsed as a run of zero-length zero-CRC records
// and was reported as NO FAULT AT ALL, while the same zero region starting one
// byte later was FATAL — review-3 named that asymmetry as the tell.
func walBlankRecordHeader(crc, dataLen uint32) bool { return crc == 0 && dataLen == 0 }

// walNothingIntelligibleFollows is Ra's is_last_record/3 (ref-rabbitmq.md
// §1.3), and it is a BYTE test, not an offset comparison.
//
// The predicate it replaces was `recStart < fileSize`, which is true whenever
// ANYTHING follows — including the zero-filled tail a power loss leaves, which
// is the shape it most needed to recognise. Measured (review-3 B-2): zeroing
// the last 2 KB of a 59 MB WAL in place produced "failed its CRC32 check and is
// NOT the last record in the file (1089 of 59041230 bytes follow it)" and
// refused the boot, for damage entirely inside the last 2 KB of the file.
//
// Ra inspects the remaining bytes precisely because zeros are what a crash
// leaves. So does this: everything from `from` to the end must be zero. A read
// failure reports false — we cannot PROVE nothing follows, and the safe
// direction for an unprovable tail is to treat it as interior corruption.
//
// limit bounds the "end": -1 means the file's real EOF. A bounded caller (a
// damaged-tail WAL file scanned only up to its durable end, WS-C C1 §3.11)
// treats bytes beyond the limit as ignorable, because it already knows they
// are a nacked partial batch that will go away with the unlink.
func walNothingIntelligibleFollows(f *os.File, from, limit int64) bool {
	buf := make([]byte, 64*1024)
	pos := from
	for {
		n := int64(len(buf))
		if limit >= 0 {
			remaining := limit - pos
			if remaining <= 0 {
				return true
			}
			if remaining < n {
				n = remaining
			}
		}
		nr, err := f.ReadAt(buf[:n], pos)
		for i := 0; i < nr; i++ {
			if buf[i] != 0 {
				return false
			}
		}
		if err == io.EOF {
			return true
		}
		if err != nil {
			return false
		}
		if nr == 0 {
			return true
		}
		pos += int64(nr)
	}
}

// scanWALFile scans a single WAL file and returns all messages. It is
// scanWALFileUpTo with no bound (the file's real EOF).
//
// CLASSIFICATION (Ra's three-way split, ref-rabbitmq.md §1.3 — RabbitMQ's own
// answer for its own modern engine):
//
//   - unknown framing version           -> FATAL, nothing in the file is parsed
//   - CRC failure with real data after it (walNothingIntelligibleFollows is
//     false) -> FATAL, everything after it is unintelligible and silently
//     dropping it loses a confirmed durable message
//   - CRC failure with nothing intelligible after it -> BENIGN torn tail
//   - a zero-filled region running to EOF                -> BENIGN torn tail
//   - short read at the tail                             -> BENIGN torn tail
//
// WHY THE TORN TAIL IS BENIGN RATHER THAN DEGRADED (changed in Step 3-fix; it
// was Degraded, which gated the boot). A partial trailing record was never
// fsynced, therefore never confirmed, therefore no durability promise is
// broken. No broker in the reference study asks for operator action on a torn
// tail: Ra drops the last record and continues, Kafka truncates and warns,
// NATS truncates and reports LostStreamData, Osiris/Artemis/CQv2 truncate
// (ref-rabbitmq.md §1.6). Requiring --unsafe-recovery after a power loss makes
// the safe path the annoying one, which is how operators end up running with
// the flag permanently set. Degraded is reserved for the abandonment of data
// that WAS confirmed — a quarantined unreadable file.
//
// It returns the messages it did parse ALONGSIDE any fault, so a degraded or
// benign outcome keeps its partial data; only a fatal outcome discards it.
func (qw *QueueWAL) scanWALFile(filePath string) ([]*RecoveryMessage, error) {
	return qw.scanWALFileUpTo(filePath, -1)
}

// scanWALFileUpTo scans a single WAL file up to limit bytes (-1 = whole
// file). WS-C C1 §3.11: a rolled file whose tail holds a nacked partial batch
// that could not be truncated (damagedTail) is checkpointed by scanning ONLY
// [0, durableSize) — the garbage is never read and goes away with the unlink.
// The bound is the scan's EOF: a record that would cross it is a torn tail
// (benign), and walNothingIntelligibleFollows treats bytes beyond it as
// ignorable.
func (qw *QueueWAL) scanWALFileUpTo(filePath string, limit int64) ([]*RecoveryMessage, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// fileSize is what makes "is this the last record" a fact rather than a
	// guess: a CRC failure is tolerable exactly when nothing follows it. When
	// a durable-end bound is given, bytes beyond it are known garbage and are
	// excluded from both the scan and the nothing-follows test.
	stat, serr := file.Stat()
	if serr != nil {
		return nil, serr
	}
	fileSize := stat.Size()
	if limit >= 0 && fileSize > limit {
		fileSize = limit
	}
	var scanFault error

	// Detect the file-level header and start parsing records after it. ITER4 is a
	// clean break: only v4 files are accepted; a non-v4 or headerless (pre-v4)
	// file returns ErrUnsupportedWALVersion rather than being silently mis-parsed.
	dataStart, err := walFileDataStart(file)
	if err != nil {
		return nil, err
	}

	// Read records through a section reader bounded at fileSize so a bounded
	// scan cannot run past the durable end.
	reader := io.NewSectionReader(file, 0, fileSize)
	if _, err := reader.Seek(dataStart, io.SeekStart); err != nil {
		return nil, err
	}

	var messages []*RecoveryMessage
	fileNum, _ := parseWALFileNum(filepath.Base(filePath))

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
		if _, err := io.ReadFull(reader, hdr); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				scanFault = interfaces.BenignFault("wal-scan", filePath,
					fmt.Sprintf("truncated record header at byte offset %d; the trailing partial record was never fsynced, so no publisher was confirmed for it", recStart))
			}
			break
		}
		crc := binary.BigEndian.Uint32(hdr[0:4])
		dataLen := binary.BigEndian.Uint32(hdr[4:8])

		if walBlankRecordHeader(crc, dataLen) {
			// A zero region starting exactly on a record boundary. See
			// walBlankRecordHeader: the write path cannot produce this.
			if walNothingIntelligibleFollows(file, recStart, fileSize) {
				scanFault = interfaces.BenignFault("wal-scan", filePath,
					fmt.Sprintf("zero-filled tail from byte offset %d to end of file (%d bytes); those blocks never reached the platter, so nothing in them was ever fsynced or confirmed", recStart, fileSize-recStart))
				break
			}
			return nil, interfaces.FatalFault("wal-scan", filePath,
				fmt.Sprintf("a zero-filled region begins at byte offset %d and is FOLLOWED BY MORE DATA before end of file (%d bytes); this file cannot be interpreted", recStart, fileSize),
				"every durable message in this WAL file is abandoned, including any that follow the zero-filled region and are themselves intact; every OTHER WAL file in the directory is still recovered",
				nil)
		}

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				scanFault = interfaces.BenignFault("wal-scan", filePath,
					fmt.Sprintf("truncated record body at byte offset %d; the trailing partial record was never fsynced, so no publisher was confirmed for it", recStart))
			}
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
				if !walNothingIntelligibleFollows(file, recStart, fileSize) {
					// INTERIOR. Silently skipping it (what this line used to do,
					// with no count and no log) drops a confirmed durable
					// message and reports the boot successful.
					return nil, interfaces.FatalFault("wal-scan", filePath,
						fmt.Sprintf("the record at byte offset %d failed its CRC32 check and is NOT the last record in the file — real data follows it (%d of %d bytes); this file cannot be interpreted",
							curRecStart, fileSize-recStart, fileSize),
						"every durable message in this WAL file is abandoned, including any that follow the damaged record and are themselves intact; every OTHER WAL file in the directory is still recovered",
						nil)
					// unreachable
				}
				// TAIL. Drop this record only and stop; nothing intelligible
				// follows it (Ra's is_last_record/3).
				scanFault = interfaces.BenignFault("wal-scan", filePath,
					fmt.Sprintf("the last intelligible record in this file (byte offset %d) failed its CRC32 check and nothing but zeros follows it — a torn write; it was never fsynced, so no publisher was confirmed for it", curRecStart))
				break
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
			recoveryMsg.pos = recordPos{segmentID: fileNum, position: curRecStart}
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
	return messages, scanFault
}

// createSharedWAL creates the single shared WAL for all queues
func (wm *WALManager) createSharedWAL() (*QueueWAL, error) {
	// Create shared WAL directory
	sharedDir := filepath.Join(wm.dataDir, "shared")
	if err := mkdirAllDurable(sharedDir); err != nil {
		return nil, fmt.Errorf("failed to create shared WAL directory: %w", err)
	}

	wal := &QueueWAL{
		name:               "shared",
		dataDir:            sharedDir,
		cfg:                wm.cfg,
		writeChan:          make(chan *writeRequest, wm.cfg.ChannelBuffer),
		ackChan:            make(chan *ackRequest, wm.cfg.ChannelBuffer),
		ackBitmaps:         make(map[string]*roaring64.Bitmap),
		currentFileOffsets: roaring64.New(),
		oldFiles:           make(map[uint64]*walFileInfo),
		oldFileCache:       make(map[uint64]*os.File),
		offsetIndex:        make(map[recordKey]*segmentRecord),
		recordCopies:       make(map[recordKey]map[recordPos]*segmentRecord),
		stopChan:           make(chan struct{}),
		// WS-F phase 1: the pipelined group-commit hand-off. commitCh is
		// unbuffered by design (see the QueueWAL field comment); free's
		// capacity is the exact staged-batch object count.
		commitCh: make(chan *stagedBatch),
		free:     make(chan *stagedBatch, walPipelineBatchCount),
	}

	// Rebuild everything the write path would otherwise be the only producer of:
	// fileNum, oldFiles (with each pre-existing file's COMPLETE offset
	// inventory) and offsetIndex. See
	// wal_boot_state.go for why this is the root cause of both C-1 and C-2.
	// It must run BEFORE openNextFile so the first Add(1) lands past every
	// pre-existing file instead of reopening one with O_APPEND.
	if err := wal.rebuildBootState(); err != nil {
		return nil, fmt.Errorf("failed to rebuild WAL boot state: %w", err)
	}

	// Open first WAL file
	if err := wal.openNextFile(); err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Start background goroutines (single set for all queues!)
	// WS-F phase 1: three goroutines — the stager (batchWriterLoop), the
	// committer (walCommitLoop), and cleanup. close() waits on all of them (D5).
	wal.wg.Add(3)
	go wal.batchWriterLoop()
	go wal.walCommitLoop()
	go wal.cleanupLoop()

	return wal, nil
}

// batchWriterLoop is the STAGER half of the pipelined group-commit writer
// (WS-F phase 1 §1.1). It owns one stagedBatch at a time, drains writeChan
// and serializes each request onto that batch's buffer AS IT ARRIVES, so the
// serialization of batch i+1 overlaps the committer's write+fsync of batch
// i. It takes NO lock (it reads only the immutable qw.cfg) and never touches
// the file, fileNum/fileOffset, the index, metrics or completions — those
// belong to walCommitLoop under fileMutex.
//
// Batches are sealed at cfg.BatchSize records or walBatchSealBytes bytes
// (§1.4); a sealed batch is handed to the committer over the UNBUFFERED
// commitCh, and because a hand-off completes only when the committer has
// fully finished the previous batch, write(i+1) is never issued before
// fsync(i) returns (D3).
//
// Flushing as soon as writeChan is drained — instead of holding a
// sub-BatchSize batch until a BatchTimeout ticker fired, as this loop
// did in an earlier design — is what keeps a lone synchronous writer fast:
// an idle committer is parked in `range commitCh`, so once writeChan is
// momentarily empty the very next select hands the partial batch off
// immediately. That is exactly the drain-then-flush behaviour
// TestWALWrite_LoneWriterNotBatchTimeoutBound pins, and it is why a lone
// writer still pays no BatchTimeout. Concurrent writers still amortize one
// fsync across the whole drained batch, so group-commit throughput is
// preserved; the batch size still self-tunes to arrival-rate ×
// fsync-latency (capped at BatchSize / walBatchSealBytes) rather than
// arrival-rate × BatchTimeout.
func (qw *QueueWAL) batchWriterLoop() {
	defer qw.wg.Done()

	var allocated int
	cur := qw.nextStagedBatch(&allocated)

	staged := 0 // cumulative requests staged, for walStageHookFire (TEST-ONLY)
	for {
		if len(cur.reqs) == 0 {
			select {
			case r := <-qw.writeChan:
				cur.add(r, qw.cfg.CRCDisabled)
				staged++
				walStageHookFire(staged)
			case <-qw.stopChan:
				qw.stageShutdownDrain(cur, &allocated, &staged)
				return
			}
		}
		// Coalesce everything already queued, until the batch seals.
	coalesce:
		for !cur.sealed(qw.cfg.BatchSize) {
			select {
			case r := <-qw.writeChan:
				cur.add(r, qw.cfg.CRCDisabled)
				staged++
				walStageHookFire(staged)
			default:
				break coalesce
			}
		}
		if cur.sealed(qw.cfg.BatchSize) {
			// Backpressure: the blocking hand-off parks the stager until the
			// committer has finished batch i (see commitCh).
			qw.handoff(cur)
			cur = qw.nextStagedBatch(&allocated)
			continue
		}
		// Partial batch and writeChan momentarily empty: hand it off if the
		// committer is idle (the lone-writer fast path — same
		// drain-then-flush as before), otherwise keep coalescing.
		//
		// Capture the snapshot data BEFORE offering the batch (under pipelineMu)
		// so the stager still owns the batch and the buf read does not race with
		// the committer's reset. Publish only inside the successful-send branch,
		// so generation counts only actual hand-offs, not attempted sends.
		qw.pipelineMu.Lock()
		ph := cur.captureHandoffSnapshot()
		qw.pipelineMu.Unlock()
		select {
		case qw.commitCh <- cur:
			qw.pipelineMu.Lock()
			cur.publishHandoffSnapshot(ph)
			qw.pipelineMu.Unlock()
			cur = qw.nextStagedBatch(&allocated)
		case r := <-qw.writeChan:
			cur.add(r, qw.cfg.CRCDisabled)
			staged++
			walStageHookFire(staged)
		case <-qw.stopChan:
			qw.stageShutdownDrain(cur, &allocated, &staged)
			return
		}
	}
}

// stageShutdownDrain is the stager's final drain (D5). After stopChan closes
// it keeps receiving from writeChan non-blockingly — every request enqueued
// before `closing` must be committed and completed exactly once — sealing
// and handing batches off as they fill, then hands the final non-empty
// batch to the committer and closes commitCh so walCommitLoop exits too.
// The C9 enqueue gate (enqueueMu) guarantees no send can arrive after this
// drain sees writeChan empty, so nothing is lost.
func (qw *QueueWAL) stageShutdownDrain(cur *stagedBatch, allocated *int, staged *int) {
	for {
		select {
		case r := <-qw.writeChan:
			cur.add(r, qw.cfg.CRCDisabled)
			*staged++
			walStageHookFire(*staged)
			if cur.sealed(qw.cfg.BatchSize) {
				qw.handoff(cur)
				cur = qw.nextStagedBatch(allocated)
			}
		default:
			if len(cur.reqs) > 0 {
				qw.handoff(cur)
			}
			close(qw.commitCh)
			return
		}
	}
}

// nextStagedBatch returns the stager's next working batch: a recycled one
// from free when available, otherwise — lazily, bounded at
// walPipelineBatchCount — a freshly allocated object. With exactly two
// objects and free's capacity of two, the blocking receive on the last line
// is reachable only while the committer still holds the other object, which
// it always returns, so it cannot deadlock.
func (qw *QueueWAL) nextStagedBatch(allocated *int) *stagedBatch {
	select {
	case sb := <-qw.free:
		return sb
	default:
	}
	if *allocated < walPipelineBatchCount {
		*allocated++
		c := qw.cfg.BatchSize
		if c < 1 {
			c = 1
		}
		sb := &stagedBatch{reqs: make([]*writeRequest, 0, c)}
		qw.pipelineMu.Lock()
		qw.pipelineBatches = append(qw.pipelineBatches, sb)
		qw.pipelineMu.Unlock()
		return sb
	}
	return <-qw.free
}

// handoff passes a sealed (or final) batch to the committer. The blocking
// send is the pipeline's backpressure point; the buffer snapshot metadata
// published under pipelineMu is TEST-ONLY observability (pipelineBufsForTest).
// The snapshot is captured before the send (under pipelineMu) and published
// with an incremented generation only on the actual hand-off path, so the
// generation counts only successful hand-offs.
func (qw *QueueWAL) handoff(sb *stagedBatch) {
	qw.pipelineMu.Lock()
	ph := sb.captureHandoffSnapshot()
	qw.pipelineMu.Unlock()
	qw.commitCh <- sb
	qw.pipelineMu.Lock()
	sb.publishHandoffSnapshot(ph)
	qw.pipelineMu.Unlock()
}

// walCommitLoop is the COMMITTER half of the pipelined group-commit writer
// (WS-F phase 1 §1.1). It receives sealed staged batches over commitCh and
// runs, under fileMutex, the exact bind → write → fsync → index → advance →
// roll sequence the single-threaded batchWriterLoop previously ran inline
// (flushBatch), then delivers completions outside every lock and returns
// the batch to the stager via free. One committer means fsyncs are strictly
// ordered and never concurrent (D2), and every completion of batch i fires
// before any of batch i+1's.
func (qw *QueueWAL) walCommitLoop() {
	defer qw.wg.Done()
	for sb := range qw.commitCh {
		qw.commitBatch(sb)
		sb.reset()
		qw.free <- sb
	}
}

// commitBatch writes one sealed staged batch to the current WAL file. It is
// flushBatch's body with the serialization removed (that now lives in
// (*stagedBatch).add on the stager goroutine) and the position bookkeeping
// inverted: the stager recorded every position buf-RELATIVE, so commitBatch
// binds the base under fileMutex — after the fileClosed / poison checks,
// atomically with the walWrite that consumes it (§1.2) — and translates
// base+relPos to absolute file positions. A roll, a truncate-repair, a
// poison-roll or an interleaved writeTxAtomic between staging and commit
// can therefore never invalidate a staged batch: ordinary records are
// position-independent (the CRC covers only the record's own [len][data]).
//
// A1 SAFETY: the reused buffer's lifetime ends at qw.currentFile.Write()'s
// RETURN — write(2) copies the bytes into the kernel page cache, and the
// subsequent fsync flushes KERNEL state, never this Go slice. So the stager
// reusing the OTHER staged buffer, and this buffer itself once the batch
// returns via free, cannot race, corrupt, or invalidate the previous
// batch's fsync, CRC (computed during staging, before Write), any confirm,
// or any delivery. Everything after Write — metrics, fsync, the
// fsync-error early return that leaves the index un-updated, offset-index
// population, offset/roll, unlock, and completions — is byte-for-byte what
// flushBatch ran, so the write→fsync→index→completion ordering and the
// durability barrier are preserved.
func (qw *QueueWAL) commitBatch(sb *stagedBatch) {
	if len(sb.reqs) == 0 {
		return
	}

	qw.fileMutex.Lock()

	// C9 belt-and-braces: a batch cannot normally arrive after close()
	// finished waiting (enqueue closed the window), but refuse it cleanly if
	// one ever does. (serErr is passed as nil here exactly as flushBatch did:
	// the batch error is the outcome that matters on this arm.)
	if qw.fileClosed {
		qw.fileMutex.Unlock()
		qw.deliverCompletions(sb.reqs, nil, ErrWALClosed)
		return
	}
	// C1: never write to a file whose size != fileOffset (poisoned); roll it
	// away first or fail fast.
	if err := qw.ensureWritableLocked(); err != nil {
		qw.fileMutex.Unlock()
		qw.deliverCompletions(sb.reqs, nil, err)
		return
	}

	// WS-F §1.2: bind the batch's file positions NOW, under fileMutex and
	// after the fileClosed/poison checks, atomically with the walWrite below
	// that consumes them. This is the whole "reservation": a roll, repair or
	// interleaved tx between staging and here changes base, never the
	// staged relPos values.
	base := qw.fileOffset.Load()
	currentFileNum := qw.fileNum.Load()
	// C1: currentFileHasSharedBody is set below after a successful write iff
	// this batch carried a shared unit; a failed write/fsync restores it so
	// a nacked shared unit cannot pin the file out of checkpoint forever.
	prevShared := qw.currentFileHasSharedBody

	// Bind shared-unit BodyRef locators (ITER5 + WS-F §1.2). The stager wrote
	// each ref record with its block's buf-RELATIVE position in the 8 locator
	// bytes and recorded a refPatch; the block's true file-relative position
	// is base+blockRel. Patch the locator and recompute that record's CRC
	// over its own [length][data] span (the locator is covered by it). Ref
	// records carry no body, so this is O(len(refPatch) × ~100 B); it stays
	// empty on the ordinary no-unit hot path. With CRCDisabled the staged
	// CRC is zero and stays zero (D6).
	for i := range sb.refPatch {
		p := &sb.refPatch[i]
		binary.BigEndian.PutUint64(sb.buf[p.locAt:], uint64(base+int64(p.blockRel)))
		if !qw.cfg.CRCDisabled {
			crc := crc32.ChecksumIEEE(sb.buf[p.recStart+4 : p.recEnd])
			binary.BigEndian.PutUint32(sb.buf[p.recStart:p.recStart+4], crc)
		}
	}

	// Single write (O_APPEND makes this atomic across processes). The write
	// goes through the walWrite hook seam (TEST-ONLY faults; default is
	// f.Write). A short write (n < len(buf), no error) is normalised to
	// io.ErrShortWrite: Go's os.File.Write sets it itself, but a hook may not.
	n, err := walWrite(qw.currentFile, sb.buf)
	if err == nil && n != len(sb.buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// Record WAL write error
		if qw.metrics != nil {
			qw.metrics.RecordWALWriteError()
		}
		// C1: the kernel may have appended n bytes. Truncate back to the
		// pre-write offset (repaired, or poisoned+rolled if that fails), so
		// the next batch's positions match the file and no interior
		// corruption is produced. Not durable, index NOT updated. Release the
		// file lock before delivering completions (async callbacks must not
		// run under fileMutex; see deliverCompletions).
		qw.currentFileHasSharedBody = prevShared
		e := qw.failBatchLocked(qw.repairAfterFailedWriteLocked(base, "write", err))
		qw.assertOffsetInvariantLocked()
		qw.fileMutex.Unlock()
		qw.deliverCompletions(sb.reqs, sb.serErr, e)
		return
	}

	// A shared body is now on the active file: mark it so rollFile can pin
	// the file out of checkpoint (§2). Set here (not during staging) so the
	// flag reflects only batches whose write succeeded; fileMutex is held
	// for the whole commitBatch.
	if sb.hasShared {
		qw.currentFileHasSharedBody = true
	}

	// Record successful WAL writes (only records that were actually serialized).
	if qw.metrics != nil {
		for i := range sb.reqs {
			if sb.relPos[i] >= 0 {
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
	//
	// C1: the bytes are also REMOVED. A failed fsync can leave dirty pages
	// marked clean (fsyncgate); nobody knows whether they reached the media,
	// and a later fsync can succeed without them. Keeping them would
	// resurrect nacked records at restart and, once more data is appended
	// after possibly-lost pages, create the zero-hole-then-data interior
	// pattern that refuses boot. Truncate back to the pre-write offset.
	if fsyncErr != nil {
		qw.currentFileHasSharedBody = prevShared
		e := qw.failBatchLocked(qw.repairAfterFailedWriteLocked(base, "fsync", fsyncErr))
		qw.assertOffsetInvariantLocked()
		qw.fileMutex.Unlock()
		qw.deliverCompletions(sb.reqs, sb.serErr, e)
		return
	}

	// Durable. Update the offset index / current-file offset set / file offset
	// and roll BEFORE releasing the lock and BEFORE firing completions, so that
	// when a completion advances a message's visibility (async path), a woken
	// consumer's wal.Read finds the record. Records that failed to serialize
	// (relPos[i] < 0) were never written, so they are not indexed.
	inv := qw.currentInv
	if inv == nil {
		inv = newSegmentInventory(base)
		qw.currentInv = inv
	}
	inv.durableBytes += int64(n)
	for _, block := range sb.blocks {
		inv.addBlock(base+block.relPos, block.bytes)
	}
	qw.offsetIndexMutex.Lock()
	for i, req := range sb.reqs {
		if sb.relPos[i] < 0 {
			continue
		}
		r := &segmentRecord{
			pos:       recordPos{segmentID: currentFileNum, position: base + sb.relPos[i]},
			queueName: req.queueName, deliveryTag: req.offset,
			bytes: sb.recordBytes[i], bodyRef: -1, committed: true,
		}
		inv.addRecord(r)
		qw.indexRecordLocked(r)
	}
	// Fused shared-unit reference records (never the BodyBlock itself). Empty on
	// the no-unit hot path, so this loop is a zero-cost no-op there.
	for _, e := range sb.sharedRel {
		r := &segmentRecord{
			pos:       recordPos{segmentID: currentFileNum, position: base + e.filePosition},
			queueName: e.queueName, deliveryTag: e.offset,
			bytes: e.bytes, bodyRef: base + e.bodyRef, committed: true,
		}
		inv.addRecord(r)
		qw.indexRecordLocked(r)
	}
	qw.offsetIndexMutex.Unlock()

	// Track offsets written to current file
	qw.currentFileOffsetsMutex.Lock()
	for i, req := range sb.reqs {
		if sb.relPos[i] < 0 {
			continue
		}
		qw.currentFileOffsets.Add(req.offset)
	}
	for _, e := range sb.sharedRel {
		qw.currentFileOffsets.Add(e.offset)
	}
	qw.currentFileOffsetsMutex.Unlock()

	// Update offset
	newOffset := qw.fileOffset.Add(int64(n))

	// Durable success: reset the failure streak (and recover from degraded).
	qw.noteSuccessLocked()
	qw.assertOffsetInvariantLocked()

	// Check if we need to roll to new file. A failed roll never fails this
	// durable batch (C3); the next commit retries it with backoff.
	if newOffset >= qw.cfg.FileSize {
		qw.maybeRollLocked()
	}

	// Release the file lock, THEN deliver completions. Completions fire LAST,
	// after the record is durable and indexed, so both the sync done-channel
	// send and the async onDone callback observe the true durability barrier.
	// Doing it outside fileMutex keeps the shared-writer critical section short
	// (consumers reading via readMessageAtPosition contend on fileMutex) and
	// honors the "no lock held across the callback" spirit of invariant A4.
	qw.fileMutex.Unlock()
	qw.deliverCompletions(sb.reqs, sb.serErr, nil)
}

// add serializes ONE write request onto the batch's buffer, recording its
// position buf-RELATIVE (commitBatch binds it to an absolute file position
// under fileMutex — §1.2). It is the body of the serialization loop
// flushBatch ran inline, unchanged except for the relative positions and
// the shared-unit locator patch bookkeeping.
//
// A record that fails to encode (e.g. a shortstr property >255 bytes) is
// rolled back to its start so no partial/corrupt bytes are written, its
// position is marked -1 (skipped by commitBatch's index loops), and its own
// error is delivered to that caller — the rest of the batch is unaffected.
// serErr stays nil (no allocation) on the common all-success path,
// preserving Stage A's zero-alloc batch serialization; it is lazily
// allocated only when a record actually fails, which the wire decoder's
// 255-byte bound makes near-unreachable. Once allocated it is kept
// len-aligned with reqs (commitBatch indexes it by batch position).
func (sb *stagedBatch) add(req *writeRequest, crcDisabled bool) {
	i := len(sb.reqs)
	sb.reqs = append(sb.reqs, req)
	if sb.serErr != nil {
		sb.serErr = append(sb.serErr, nil)
	}
	sb.relPos = append(sb.relPos, 0)
	sb.recordBytes = append(sb.recordBytes, 0)

	if req.unit == nil {
		recStart := len(sb.buf)
		sb.relPos[i] = int64(recStart) // position relative to buf[0]
		var e error
		sb.buf, e = appendMessageRecord(sb.buf, req.queueName, req.message, req.offset, crcDisabled)
		sb.recordBytes[i] = int64(len(sb.buf) - recStart)
		if e != nil {
			sb.buf = sb.buf[:recStart] // discard the failed record's partial bytes
			sb.relPos[i] = -1
			sb.recordBytes[i] = 0
			if sb.serErr == nil {
				sb.serErr = make([]error, len(sb.reqs))
			}
			sb.serErr[i] = e
		}
		return
	}

	// FUSED SHARED UNIT (ITER5, §3.3): emit the BodyBlock first (capturing
	// its buf-RELATIVE start as the locator placeholder), then each sub as a
	// message record carrying that placeholder via the existing
	// bodyKindReference arm. The block's offset is NOT indexed (it is not a
	// deliverable message); each sub's (offset, relative position) is
	// recorded for post-fsync index population, plus a refPatch for the
	// commit-time locator binding. This req occupies ONE batch index, so
	// relPos[i] is marked -1 (skipped by the ordinary index loops); its subs
	// are indexed via sharedRel.
	sb.relPos[i] = -1
	u := req.unit
	unitStart := len(sb.buf)
	blockRel := unitStart // the block's position relative to buf[0]
	sb.buf = appendBodyBlockRecord(sb.buf, u.fanoutHint, u.body, crcDisabled)
	blockBytes := int64(len(sb.buf) - unitStart)

	// The locator is staged as the RELATIVE block position and patched to
	// base+blockRel by commitBatch (§1.2), so a roll, a repair or an
	// interleaved tx between staging and commit cannot invalidate it.
	var locator [8]byte
	binary.BigEndian.PutUint64(locator[:], uint64(blockRel))

	savedSharedLen := len(sb.sharedRel)
	savedPatchLen := len(sb.refPatch)
	var unitErr error
	for si := range u.subs {
		sub := &u.subs[si]
		recStart := len(sb.buf)
		subRel := int64(recStart)
		// Drive the existing bodyKindReference writer arm by setting BodyRef
		// to the 8-byte locator, then CLEAR it immediately so the caller's
		// in-memory message stays a clean inline message (a later
		// dead-letter/requeue re-store must NOT emit a dangling reference
		// into another file). Safe: the sub message is not consumer-visible
		// until its completion fires (after the fsync), so nothing reads
		// BodyRef concurrently here.
		sub.message.BodyRef = locator[:]
		var e error
		sb.buf, e = appendMessageRecord(sb.buf, sub.queueName, sub.message, sub.offset, crcDisabled)
		sub.message.BodyRef = nil
		if e != nil {
			unitErr = e
			break
		}
		sb.sharedRel = append(sb.sharedRel, sharedIndexEntry{
			queueName: sub.queueName, offset: sub.offset, filePosition: subRel,
			bytes: int64(len(sb.buf) - recStart), bodyRef: int64(blockRel),
		})
		sb.refPatch = append(sb.refPatch, refPatch{
			recStart: recStart,
			recEnd:   len(sb.buf),
			locAt:    recStart + bodyRefLocatorOffset(sub.queueName, sub.message),
			blockRel: blockRel,
		})
	}
	if unitErr != nil {
		// All-or-nothing per unit: discard the block + every already-written
		// sub record, drop the partial index entries and patches, and nack
		// every sub.
		sb.buf = sb.buf[:unitStart]
		sb.sharedRel = sb.sharedRel[:savedSharedLen]
		sb.refPatch = sb.refPatch[:savedPatchLen]
		if sb.serErr == nil {
			sb.serErr = make([]error, len(sb.reqs))
		}
		sb.serErr[i] = unitErr
		return
	}
	sb.blocks = append(sb.blocks, stagedBodyBlock{relPos: int64(blockRel), bytes: blockBytes})
	sb.hasShared = true
}

// sealed reports whether the batch must be handed off now: cfg.BatchSize
// requests reached, or the serialization buffer crossed walBatchSealBytes
// (§1.4; a single record larger than the seal forms its own batch, which
// the post-add check produces naturally). A non-positive BatchSize
// degenerates to one record per batch, matching the pre-pipeline drain
// bound (len(batch) < BatchSize never coalesced anything).
func (sb *stagedBatch) sealed(batchSize int) bool {
	if len(sb.buf) >= walBatchSealBytes {
		return true
	}
	if batchSize > 0 {
		return len(sb.reqs) >= batchSize
	}
	return len(sb.reqs) >= 1
}

// reset recycles the batch for the stager's next use. It runs on the
// committer goroutine BEFORE free <- sb hands ownership back (§1.4), so no
// synchronization is needed. Buffer retention follows the old flushBatch
// rule: keep buf iff its capacity is within maxRetainedWALBatchBytes, else
// drop it so a rare oversized batch is not pinned (the seal already bounds
// steady-state growth). reqs and serErr are cleared before reslicing so the
// backing array's old message bodies, callbacks, subs, and errors are
// unreachable across bursts. relPos/relPos keep their capacity (≤ BatchSize
// in normal configs); handoffSnapshot is left as-is — it is a TEST-ONLY
// metadata record that stays valid until the stager's next hand-off of this
// object.
var handoffGen atomic.Int64

// pendingHandoff stores the snapshot data captured BEFORE a hand-off attempt.
// Publishing only happens on a successful hand-off, so the generation counter
// counts only actual hand-offs, not attempted sends. TEST-ONLY.
type pendingHandoff struct {
	base uintptr
	len  int
	cap  int
}

// captureHandoffSnapshot reads buf's current len, cap, and base address into
// the pendingHandoff, without incrementing the generation. The caller must
// later call publishHandoffSnapshot (after the successful send) to commit it.
// Called under pipelineMu so the read does not race with the committer's reset
// of a different batch. TEST-ONLY observability.
func (sb *stagedBatch) captureHandoffSnapshot() pendingHandoff {
	if len(sb.buf) > 0 {
		return pendingHandoff{
			base: uintptr(unsafe.Pointer(&sb.buf[0])),
			len:  len(sb.buf),
			cap:  cap(sb.buf),
		}
	}
	return pendingHandoff{len: len(sb.buf), cap: cap(sb.buf)}
}

// publishHandoffSnapshot commits a pre-captured pending snapshot into
// sb.handoffSnapshot and increments the handoff generation. Must be called
// under pipelineMu after a successful hand-off send. TEST-ONLY observability.
func (sb *stagedBatch) publishHandoffSnapshot(ph pendingHandoff) {
	sn := &sb.handoffSnapshot
	sn.HandoffBase = ph.base
	sn.HandoffLen = ph.len
	sn.HandoffCap = ph.cap
	sn.HandoffGen = handoffGen.Add(1)
}

func (sb *stagedBatch) reset() {
	if cap(sb.buf) > maxRetainedWALBatchBytes {
		sb.buf = nil
	} else {
		sb.buf = sb.buf[:0]
	}
	// Clear pointer and interface elements before reslicing so the backing
	// array's old message bodies, callbacks, subs, and errors are unreachable
	// across bursts (memory retention fix, §1.4).
	clear(sb.reqs)
	sb.reqs = sb.reqs[:0]
	if sb.serErr != nil {
		clear(sb.serErr)
	}
	sb.serErr = nil
	sb.relPos = sb.relPos[:0]
	sb.recordBytes = sb.recordBytes[:0]
	sb.sharedRel = sb.sharedRel[:0]
	sb.blocks = sb.blocks[:0]
	sb.refPatch = sb.refPatch[:0]
	sb.hasShared = false
}

// bodyRefLocatorOffset returns the byte offset, counted from the START of a
// message record, at which the 8-byte BodyRef locator is stored when the
// message serializes through the bodyKindReference arm of the v4 layout:
//
//	[CRC 4][len 4][type 1][flags 2][queue 1+n][offset 8][exch 1+n][rk 1+n]
//	[deliveryMode 1][bodyKind 1][refLen u16] <locator HERE>
//
// Optional fields follow the body arm and never shift this offset. It is
// derived from the same layout appendMessagePayload writes (rather than
// counted at append time) so the two cannot drift apart silently; P1-10
// (TestSharedUnit_LocatorPatchEqualsDirectSerialize) pins the equivalence.
func bodyRefLocatorOffset(queueName string, message *protocol.Message) int {
	return 4 + // CRC
		4 + // length
		1 + // record type
		2 + // payload flags
		(1 + len(queueName)) + // queue shortstr
		8 + // offset
		(1 + len(message.Exchange)) +
		(1 + len(message.RoutingKey)) +
		1 + // deliveryMode
		1 + // bodyKind
		2 // refLen (u16); the 8 locator bytes follow
}

// pipelineBufsForTest returns each staged-batch object's handoff snapshot
// metadata (len, cap, base µptr, generation). Unlike a []byte snapshot this
// does NOT root the backing array, so the oversized-buffer release path is
// observable via nonzero HandoffBase without retention. TEST-ONLY
// observability for the phase 1 pipeline tests (P1-11). Snapshots are
// published under pipelineMu, so reading them never races the stager or the
// committer.
func (qw *QueueWAL) pipelineBufsForTest() []handoffSnapshot {
	qw.pipelineMu.Lock()
	defer qw.pipelineMu.Unlock()
	out := make([]handoffSnapshot, len(qw.pipelineBatches))
	for i, sb := range qw.pipelineBatches {
		out[i] = sb.handoffSnapshot
	}
	return out
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

// rollFile rolls to a new WAL file when the size limit is reached.
//
// C3: OPEN BEFORE CLOSE. The next file is opened first; only then is the
// current file retired and the handles switched over, all in one critical
// section under fileMutex. If the open fails, NOTHING changes — the current
// file stays active and writable (oversized; FileSize is a soft limit), and
// the roll is retried by tryRollLocked/maybeRollLocked with backoff. The
// previous shape (close, then open, error discarded) wedged the WAL until
// restart on any open failure.
//
// Precondition: caller holds fileMutex.
func (qw *QueueWAL) rollFile() error {
	curNum := qw.fileNum.Load()
	num := curNum + 1

	// 1. Open the next file FIRST; the current file is untouched.
	w, r, size, err := qw.openWALFile(num)
	if err != nil {
		return fmt.Errorf("WAL roll: %w", err)
	}

	// 2. Retire the current file. oldFiles/oldFileCache are published BEFORE
	// the fileNum switch, so a reader that sees the new fileNum and holds an
	// index entry for the old file takes the old-file arm and finds the cached
	// handle.
	currentPath := qw.currentFile.Name()

	qw.currentFileOffsetsMutex.Lock()
	fileOffsets := qw.currentFileOffsets
	qw.currentFileOffsets = roaring64.New()
	qw.currentFileOffsetsMutex.Unlock()

	// ITER5 §2: capture whether the file being rolled carries a shared BodyBlock,
	// then reset for the fresh file. rollFile is only ever invoked from commitBatch
	// (and writeTxAtomic) while fileMutex is held, so this read+reset is race-free
	// against the committer that sets the flag.
	sharedBody := qw.currentFileHasSharedBody
	qw.currentFileHasSharedBody = false
	sealedInv := qw.currentInv
	if sealedInv == nil {
		sealedInv = newSegmentInventory(qw.fileOffset.Load())
	}

	// Move current read handle to old file cache (it's already open, no need to reopen)
	if qw.currentReadFile != nil {
		qw.oldFileCacheMutex.Lock()
		if qw.oldFileCache == nil {
			qw.oldFileCache = make(map[uint64]*os.File)
		}
		qw.oldFileCache[curNum] = qw.currentReadFile
		qw.oldFileCacheMutex.Unlock()
	}

	// Move to old files map. durableSize is the durable end (I4: fileOffset is
	// advanced only after a successful fsync); damagedTail records that bytes
	// beyond it are a nacked partial batch that could not be truncated (C1).
	qw.oldFilesMutex.Lock()
	qw.oldFiles[curNum] = &walFileInfo{
		path:          currentPath,
		offsets:       fileOffsets,
		createdAt:     time.Now(),
		hasSharedBody: sharedBody,
		durableSize:   qw.fileOffset.Load(),
		damagedTail:   qw.poisoned != nil,
		inv:           sealedInv,
	}
	qw.oldFilesMutex.Unlock()

	// Close current write file. The close error is ignored as before: the
	// file's records were fdatasynced by their batches, and the read handle
	// stays open in the cache.
	_ = qw.currentFile.Close()

	// 3. Switch over in one step (still under fileMutex). A successful roll
	// clears poison and degradation: the diverged file was just retired above
	// and the WAL is writable again, so Health() reports nil.
	qw.fileNum.Store(num)
	qw.currentFile = w
	qw.currentReadFile = r
	qw.fileOffset.Store(size)
	qw.currentInv = newSegmentInventory(size)
	qw.poisoned = nil
	qw.degraded = false
	qw.rollBackoff = 0
	qw.nextRollAttempt = time.Time{}
	qw.rollFailures = 0
	qw.assertOffsetInvariantLocked()
	return nil
}

// errRollBackoff is returned by tryRollLocked when the next roll attempt is
// still inside the backoff window. It is an internal pacing signal, never a
// write outcome.
var errRollBackoff = errors.New("WAL roll backoff")

// clampDuration clamps d into [min, max].
func clampDuration(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

// tryRollLocked attempts a roll, honouring the backoff window.
// Precondition: caller holds fileMutex. On failure the backoff grows
// (doubled, clamped to [walRollBackoffMin, walRollBackoffMax]) and the error
// is returned; nothing else changes — the current file stays active and
// writes continue past it (FileSize is a soft limit). ignoreBackoff skips the
// window so the first attempt after a poison is immediate.
func (qw *QueueWAL) tryRollLocked(ignoreBackoff bool) error {
	now := time.Now()
	if !ignoreBackoff && now.Before(qw.nextRollAttempt) {
		return errRollBackoff
	}
	err := qw.rollFile()
	if err == nil {
		return nil
	}
	qw.rollBackoff = clampDuration(2*qw.rollBackoff, walRollBackoffMin, walRollBackoffMax)
	qw.nextRollAttempt = now.Add(qw.rollBackoff)
	qw.rollFailures++
	if qw.rollFailures == 1 || qw.rollFailures%10 == 0 {
		qw.logError("WAL roll failed; the current file stays active and writes continue past the size limit; retrying with backoff",
			interfaces.LogField{Key: "error", Value: err.Error()},
			interfaces.LogField{Key: "backoff", Value: qw.rollBackoff.String()},
			interfaces.LogField{Key: "attempt", Value: qw.rollFailures})
	}
	return err
}

// maybeRollLocked is called after a durable batch/tx pushed the file past
// FileSize. A failed roll never fails the durable batch: the bytes are
// already fdatasynced and indexed, and nacking them would misreport durable
// data and mint duplicates on retry. fileOffset stays >= FileSize until a
// roll succeeds, so every later flush past the backoff window retries
// automatically — no timer goroutine (acceptance rule 9).
func (qw *QueueWAL) maybeRollLocked() {
	_ = qw.tryRollLocked(false)
}

// logError / logWarn report a cold-path WAL failure when a logger is wired
// (C1 §3.8); a nil logger (the default for a bare WALManager) is silent.
func (qw *QueueWAL) logError(msg string, fields ...interfaces.LogField) {
	if l := qw.logger.Load(); l != nil {
		l.Error(msg, fields...)
	}
}

func (qw *QueueWAL) logWarn(msg string, fields ...interfaces.LogField) {
	if l := qw.logger.Load(); l != nil {
		l.Warn(msg, fields...)
	}
}

// SetLogger wires a structured logger into the shared WAL's cold failure
// paths (poison, roll backoff, degraded transitions). Also publishes it as
// the package-level sink for the one-time "directory fsync unsupported" WARN
// (syncdir.go). Safe to call after the goroutines have started: the logger is
// read atomically and only on failure paths.
func (wm *WALManager) SetLogger(l interfaces.Logger) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	storageWarnLogger.Store(l)
	if wm.sharedWAL != nil {
		wm.sharedWAL.logger.Store(l)
	}
}

// openWALFile opens (creating if needed) WAL file number num and returns its
// write handle, read handle and data size. It does NOT touch QueueWAL state,
// which is what lets rollFile open the next file before retiring the current
// one. On any failure AFTER the file was created, the handles are closed and
// the file is removed, so a failed roll leaves at most nothing behind — never
// a 1–5 byte headerless stub that every later boot refuses to append to.
func (qw *QueueWAL) openWALFile(num uint64) (w, r *os.File, size int64, err error) {
	filename := filepath.Join(qw.dataDir, fmt.Sprintf("%020d%s", num, WALFileExtension))

	file, err := walOpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Determine the current size so we (a) only write the file-level header on a
	// genuinely new/empty file and (b) seed fileOffset to the true end of the
	// file. Seeding from the real size keeps the offset index correct when an
	// existing file is reopened and appended to (e.g. a restart that reuses the
	// same file number, or a legacy headerless file that is being appended to).
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, 0, fmt.Errorf("failed to stat WAL file: %w", err)
	}
	size = stat.Size()
	created := size == 0

	// Write the file-level header (magic + version) only on a brand-new file
	// (SQ-4). The header is flushed to disk together with the first record
	// batch's fdatasync — one small write per file, never per record. Records
	// therefore begin at WALHeaderSize.
	//
	// A PRE-EXISTING FILE MUST PROVE IT CARRIES A HEADER THIS BUILD CAN PARSE
	// BEFORE WE APPEND A SINGLE BYTE TO IT. This branch used to say "a
	// pre-existing file keeps whatever header (or none) it already has", which
	// made the broker append v4 records after a foreign header FOREVER: the
	// stale file is not empty, so no header was written, and every subsequent
	// boot re-failed to parse it while adding more unreadable records. There is
	// no self-healing and the only operator signal was one Error line. Two such
	// contaminated directories sat unnoticed in this repository for three weeks.
	//
	// COLD PATH: once per WAL file, i.e. once per FileSize bytes written
	// (512 MB by default). The probe handle is opened only when the file is
	// non-empty, which for every file this process creates is never.
	if created {
		header := make([]byte, 0, WALHeaderSize)
		header = append(header, WALMagic...)
		header = append(header, WALFormatVersion)
		n, werr := file.Write(header)
		if werr != nil {
			// Best-effort truncate first (a 1–5 byte partial header is refused
			// by every later boot), then close and REMOVE the file: a retry
			// reuses the same number and starts clean.
			_ = file.Truncate(0)
			_ = file.Close()
			_ = os.Remove(filename)
			return nil, nil, 0, fmt.Errorf("failed to write WAL file header: %w", werr)
		}
		size = int64(n)
		// C4: make the file's directory entry durable NOW, at creation. The
		// reliance point is the first confirm of a record in this file, which
		// is always later; syncing here costs one directory fsync per WAL file
		// (cold path: once per FileSize bytes) and keeps the per-batch cost
		// flat. On failure the file is removed and the open fails — a file
		// whose entry may not survive a crash must not be handed to the
		// writer.
		if serr := syncDir(qw.dataDir); serr != nil {
			_ = file.Close()
			_ = os.Remove(filename)
			return nil, nil, 0, fmt.Errorf("WAL file directory fsync failed: %w", serr)
		}
	} else {
		if size < int64(WALHeaderSize) {
			_ = file.Close()
			return nil, nil, 0, fmt.Errorf("refusing to append to WAL file %s: %w: file is %d bytes, too short to carry a %d-byte header",
				filename, ErrUnsupportedWALVersion, size, WALHeaderSize)
		}
		// currentFile is O_WRONLY and cannot ReadAt; probe with its own handle.
		probe, perr := os.Open(filename)
		if perr != nil {
			_ = file.Close()
			return nil, nil, 0, fmt.Errorf("failed to open WAL file %s for header validation: %w", filename, perr)
		}
		_, verr := walFileDataStart(probe)
		_ = probe.Close()
		if verr != nil {
			_ = file.Close()
			return nil, nil, 0, fmt.Errorf("refusing to append to WAL file %s: %w", filename, verr)
		}
	}

	// Open a read-only handle for ReadAt (currentFile is O_WRONLY, can't ReadAt)
	readFile, err := os.Open(filename)
	if err != nil {
		file.Close()
		if created {
			// The file is ours and brand new; do not leave it behind.
			_ = os.Remove(filename)
		}
		return nil, nil, 0, fmt.Errorf("failed to open WAL read handle: %w", err)
	}

	return file, readFile, size, nil
}

// openNextFile opens the next WAL file and installs it. BOOT ONLY: it is
// called from createSharedWAL before any background goroutine starts, which
// is why it may install the new triple directly. Runtime rolls go through
// rollFile, which retires the current file first.
func (qw *QueueWAL) openNextFile() error {
	num := qw.fileNum.Load() + 1
	w, r, size, err := qw.openWALFile(num)
	if err != nil {
		return err
	}
	qw.fileNum.Store(num)
	qw.currentFile = w
	qw.currentReadFile = r
	qw.fileOffset.Store(size)
	qw.currentInv = newSegmentInventory(size)
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

// cleanupLoop periodically reclaims old WAL files. Acknowledgement is wholly
// synchronous and never deletes read-index entries.
func (qw *QueueWAL) cleanupLoop() {
	defer qw.wg.Done()

	ticker := time.NewTicker(qw.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qw.tryReclaimOldFiles()
			// Compaction maintenance runs DIRECTLY on the cleanup tick: it is
			// the same maintenance pass, and a second ticker would only let
			// the two disagree about which files they are working on.
			qw.tryCompactOldFiles()
		case <-qw.stopChan:
			return
		}
	}
}

// tryReclaimOldFiles reclaims sealed WAL files whose valid physical inventory
// has no unacknowledged records. Retention time never authorizes deletion:
// unacknowledged data remains until it is acknowledged, regardless of age.
func (qw *QueueWAL) tryReclaimOldFiles() {
	// The index lock excludes sealed readers through their ReadAt and prevents a
	// late acknowledgement from acquiring a copy while its file is reclaimed.
	qw.offsetIndexMutex.Lock()
	qw.oldFilesMutex.Lock()

	unlinkedAny := false
	for fileNum, info := range qw.oldFiles {
		// oldFiles normally contains sealed files only. Keep the explicit active
		// guard so a malformed registration can never make the writer's file a
		// reclaim candidate.
		if fileNum == qw.fileNum.Load() || info.inv == nil || !info.inv.valid ||
			info.inv.unacked.Load() != 0 {
			continue
		}

		// Readers of sealed files hold offsetIndexMutex.RLock through ReadAt, so
		// no cached descriptor is in use here. If unlink fails the registration
		// and every index entry remain intact; the handle is lazily reopened for
		// the next read and the next sweep retries the unlink.
		qw.closeOldFileHandle(fileNum)
		if err := walUnlink(info.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			qw.logError("fully-acknowledged WAL file could not be unlinked; it stays registered and indexed for retry",
				interfaces.LogField{Key: "path", Value: info.path},
				interfaces.LogField{Key: "error", Value: err.Error()})
			continue
		}

		for _, record := range info.inv.records {
			qw.unindexRecordLocked(record)
		}
		delete(qw.oldFiles, fileNum)
		unlinkedAny = true
	}

	qw.oldFilesMutex.Unlock()
	qw.offsetIndexMutex.Unlock()

	// Make this sweep's directory mutations durable exactly once. The reclaim
	// is already complete in memory even if this barrier fails; only fully acked
	// bytes could reappear after a crash, so the failure cannot invalidate it.
	if unlinkedAny {
		if err := syncDir(qw.dataDir); err != nil {
			qw.logWarn("WAL directory fsync after reclaim failed; completed reclaim remains valid, but its directory mutations are not durable yet",
				interfaces.LogField{Key: "dir", Value: qw.dataDir},
				interfaces.LogField{Key: "error", Value: err.Error()})
		}
	}
}

// readMessage resolves the newest physical copy. A sealed-file lookup keeps
// offsetIndexMutex.RLock through every ReadAt so index/layout replacement cannot
// race it; the active-file arm releases the index before acquiring fileMutex.
func (qw *QueueWAL) readMessage(queueName string, offset uint64) (*protocol.Message, error) {
	if qw.isAcknowledged(queueName, offset) {
		return nil, fmt.Errorf("message not in WAL")
	}

	key := recordKey{queueName: queueName, deliveryTag: offset}
	for {
		qw.offsetIndexMutex.RLock()
		location, ok := qw.offsetIndex[key]
		if !ok {
			qw.offsetIndexMutex.RUnlock()
			return nil, fmt.Errorf("message not in WAL")
		}
		if location.pos.segmentID == qw.fileNum.Load() {
			qw.offsetIndexMutex.RUnlock()
			qw.fileMutex.Lock()
			if location.pos.segmentID != qw.fileNum.Load() {
				// It rolled between the atomic check and fileMutex. Retry through
				// the sealed arm with the index lock held through ReadAt.
				qw.fileMutex.Unlock()
				continue
			}
			file := qw.currentReadFile
			qw.fileMutex.Unlock()
			if file == nil {
				return nil, fmt.Errorf("WAL current read file not open for fileNum %d", location.pos.segmentID)
			}
			return qw.parseMessageAt(file, queueName, location.pos.position, offset)
		}

		var msg *protocol.Message
		err := qw.withOldFileHandle(location.pos.segmentID, func(file *os.File) error {
			var perr error
			msg, perr = qw.parseMessageAt(file, queueName, location.pos.position, offset)
			return perr
		})
		qw.offsetIndexMutex.RUnlock()
		if err != nil {
			return nil, err
		}
		return msg, nil
	}
}

// oldFileReadAttempts bounds the retry in withOldFileHandle. Two: one to
// discover the cached handle was closed under us, and one against a freshly
// opened replacement. There is no third state to recover from — a file that is
// genuinely gone fails the re-open with a filesystem error, which is the honest
// answer to give the caller.
const oldFileReadAttempts = 2

// getOldFileHandle returns an open file handle for an old WAL file, using a
// cache to avoid repeated open/close syscalls.
//
// IT RETURNS NO UNLOCK FUNCTION, AND THAT IS A TYPE-LEVEL INVARIANT RATHER THAN
// A CONVENTION. oldFileCacheMutex is a LEAF: it is released before this function
// returns on every path, and with no return slot for a lock no future edit can
// hand one to a caller. That is what removes the oldFileCacheMutex ->
// bitmapMutex edge of the F1 cycle, at the compiler rather than at review.
//
// The consequence is that closeOldFileHandle may close the returned handle
// between this return and the caller's ReadAt. That is SAFE, and it is asserted
// rather than documented (TestClosedOldFileHandle_ReadAtYieldsTypedErrClosed):
// os.File.ReadAt -> poll.FD.Pread takes an incref that fails once Close has
// marked the descriptor closed, so the read returns a typed os.ErrClosed without
// ever reaching Sysfd — it can neither read through a recycled descriptor nor
// race Close's deferred destroy(). Callers go through withOldFileHandle, which
// turns that error into a bounded retry.
func (qw *QueueWAL) getOldFileHandle(fileNum uint64) (*os.File, error) {
	// Fast path: cache hit (RLock allows concurrent readers)
	qw.oldFileCacheMutex.RLock()
	cached, hit := qw.oldFileCache[fileNum]
	qw.oldFileCacheMutex.RUnlock()
	if hit {
		return cached, nil
	}

	// Cache miss: get file path first (separate lock to avoid lock order inversion)
	qw.oldFilesMutex.RLock()
	info, ok := qw.oldFiles[fileNum]
	filePath := ""
	if ok {
		filePath = info.path
	}
	qw.oldFilesMutex.RUnlock()
	if !ok {
		return nil, fmt.Errorf("WAL file not found for fileNum %d", fileNum)
	}

	// Open file outside any locks
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// Add to cache (write lock)
	qw.oldFileCacheMutex.Lock()
	defer qw.oldFileCacheMutex.Unlock()

	// Double-check: another goroutine may have opened and cached it
	if existing, ok := qw.oldFileCache[fileNum]; ok {
		_ = file.Close() // close our redundant handle
		return existing, nil
	}

	if qw.oldFileCache == nil {
		qw.oldFileCache = make(map[uint64]*os.File)
	}
	qw.oldFileCache[fileNum] = file
	return file, nil
}

// withOldFileHandle runs read against the cached handle for fileNum, retrying
// once on os.ErrClosed.
//
// The retry is what makes the safe direction structural instead of dependent on
// which goroutine happens to run first: a reader that meets a handle closed
// under it re-acquires and reads again, rather than reporting a transient
// reclaim to its caller as a read fault. It is bounded, so a file that is really
// gone still fails, promptly.
func (qw *QueueWAL) withOldFileHandle(fileNum uint64, read func(*os.File) error) error {
	var err error
	for attempt := 0; attempt < oldFileReadAttempts; attempt++ {
		var file *os.File
		file, err = qw.getOldFileHandle(fileNum)
		if err != nil {
			return err
		}
		err = read(file)
		if !errors.Is(err, os.ErrClosed) {
			return err
		}
		// Drop the handle we just read through before trying again. In the
		// production race closeOldFileHandle has already removed it and this is
		// a no-op; it matters when the closed entry is still installed, which is
		// the only state a second attempt can actually repair.
		qw.closeOldFileHandle(fileNum)
	}
	return err
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
// Current file: uses the already-open handle. Old files: uses a file handle cache,
// through withOldFileHandle so a handle closed under the read is retried rather
// than reported as a read fault.
func (qw *QueueWAL) readMessageAtPosition(queueName string, fileNum uint64, filePosition int64, expectedOffset uint64) (*protocol.Message, error) {
	// Load currentFileNum inside fileMutex to prevent TOCTOU race with rollFile
	qw.fileMutex.Lock()
	currentFileNum := qw.fileNum.Load()
	if fileNum == currentFileNum {
		file := qw.currentReadFile
		qw.fileMutex.Unlock()
		if file == nil {
			return nil, fmt.Errorf("WAL current read file not open for fileNum %d", fileNum)
		}
		return qw.parseMessageAt(file, queueName, filePosition, expectedOffset)
	}
	qw.fileMutex.Unlock()

	var msg *protocol.Message
	err := qw.withOldFileHandle(fileNum, func(file *os.File) error {
		var perr error
		msg, perr = qw.parseMessageAt(file, queueName, filePosition, expectedOffset)
		return perr
	})
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// parseMessageAt reads and decodes the single record at filePosition in file. It
// is split out of readMessageAtPosition so both the current-file arm and the
// retrying old-file arm decode identically — a second copy of this parse is how
// the two arms would drift.
func (qw *QueueWAL) parseMessageAt(file *os.File, queueName string, filePosition int64, expectedOffset uint64) (*protocol.Message, error) {
	// Read CRC + length in a single ReadAt (8 bytes at filePosition)
	header := make([]byte, 8)
	_, err := walReadAt(file, header, filePosition)
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	// Read data portion in a single ReadAt (dataLen bytes at filePosition+8)
	data := make([]byte, dataLen)
	_, err = walReadAt(file, data, filePosition+8)
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
	// SECOND ReadAt for the co-located BodyBlock on the SAME open handle. The
	// block is in the same file as the reference by the co-location invariant, so
	// `file` is correct.
	//
	// This second read is NOT protected by any handle-lifetime lock on the
	// old-file arm — nothing holds oldFileCacheMutex here, by design. A
	// concurrent closeOldFileHandle makes this ReadAt return os.ErrClosed, which
	// propagates out of this function and is retried by withOldFileHandle
	// against a fresh handle. The %w below is load-bearing for that: errors.Is
	// traverses it, so wrapping keeps the diagnostic without hiding the cause.
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
	if _, err := walReadAt(file, header, position); err != nil {
		return nil, err
	}
	crc := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	data := make([]byte, dataLen)
	if _, err := walReadAt(file, data, position+8); err != nil {
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
	// NOTE: the acknowledgement gate is NOT repeated here. readMessage is this
	// function's only caller and applies it at the entry point, which strictly
	// dominates: a gate here could not see a probe answered by the O(1) fast path.
	// Duplicating it would be one observable written twice and no mutation could
	// redden this copy independently.

	// The CURRENT file is searched FIRST. currentFileOffsets is reset by rollFile
	// and added to only by commitBatch for records written to the file open now, so
	// an offset in it names a record THIS incarnation wrote to THIS file — by
	// construction the newest record carrying that offset. Searching oldFiles ahead
	// of it handed back a previous incarnation's payload for a live tag whenever
	// both files carried the same offset, which this WAL permits: it is globally
	// tag-keyed with no queue discriminator, and a redeclared queue re-mints a tag
	// band whose records its predecessor still occupies on disk.
	//
	// Membership also gates out a record whose bytes reached the file but whose
	// fsync FAILED, because commitBatch populates currentFileOffsets only AFTER a
	// successful fsync. Such a record is in neither that set nor the index and must
	// not be returned: the frontier advances head past its tag, so the cursor is
	// claimed, and a raw scan would otherwise deliver a message that was nacked.
	qw.currentFileOffsetsMutex.Lock()
	durableInCurrent := qw.currentFileOffsets.Contains(offset)
	qw.currentFileOffsetsMutex.Unlock()

	if durableInCurrent {
		qw.fileMutex.Lock()
		currentPath := ""
		if qw.currentFile != nil {
			currentPath = qw.currentFile.Name()
		}
		qw.fileMutex.Unlock()

		if currentPath != "" {
			// Deliberately NOT falling back to oldFiles on failure. Membership in
			// currentFileOffsets is a positive statement that this incarnation
			// durably wrote this offset to the open file; if it cannot be read
			// there, that is an integrity problem, and answering with a different
			// record that happens to share the tag is the corruption this ordering
			// exists to prevent.
			return qw.readMessageFromFile(queueName, currentPath, offset)
		}
	}

	// Not in the current file: fall back to the rolled and inherited files. This
	// is the ordinary cold read of a record written before the last roll.
	qw.oldFilesMutex.RLock()
	defer qw.oldFilesMutex.RUnlock()

	for _, fileInfo := range qw.oldFiles {
		if fileInfo.offsets != nil && fileInfo.offsets.Contains(offset) {
			// Found the file containing this offset
			return qw.readMessageFromFile(queueName, fileInfo.path, offset)
		}
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

		if walBlankRecordHeader(crc, dataLen) {
			// Zero-filled tail: this file's data ends here (see
			// walBlankRecordHeader), so the offset is not in it.
			return nil, fmt.Errorf("message not in WAL")
		}

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

// close stops background goroutines and closes files.
//
// C9 ORDERING: closing is published under enqueueMu BEFORE stopChan is
// closed, so no writeChan send can race past the stager's final drain; and
// fileClosed is set only after wg.Wait(), so the final drain's commits
// still run normally and deliver exactly one completion per enqueued
// request. See enqueue.
func (qw *QueueWAL) close() {
	qw.enqueueMu.Lock()
	qw.closing = true
	qw.enqueueMu.Unlock()

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
	qw.fileClosed = true
	defer qw.fileMutex.Unlock()

	if qw.currentFile != nil {
		_ = qw.currentFile.Sync()
		_ = qw.currentFile.Close()
	}
	if qw.currentReadFile != nil {
		_ = qw.currentReadFile.Close()
	}
}
