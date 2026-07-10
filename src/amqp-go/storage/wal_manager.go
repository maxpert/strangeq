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

	// Shared WAL message (record) format:
	// [4 bytes CRC32][4 bytes length][4 bytes queueNameLen][queueName][8 bytes offset]
	// [4 bytes exchangeLen][exchange][4 bytes routingKeyLen][routingKey][1 byte deliveryMode][4 bytes bodyLen][body]
	// Note: The record is variable-length (depends on queue name, exchange, and routing key lengths)

	// WAL file-level header (SQ-4): written once at the start of every NEW WAL
	// file. It lets recovery detect the on-disk format version before parsing
	// records. The magic is deliberately not a valid start of a record (a real
	// record begins with a 4-byte CRC then a 4-byte length); a first-byte
	// collision with an actual CRC value is astronomically unlikely, so a byte
	// prefix compare is a safe way to sniff versioned vs. legacy files.
	WALMagic         = "SQWAL"           // 5-byte file magic
	WALHeaderSize    = len(WALMagic) + 1 // magic + 1 version byte = 6 bytes
	WALLegacyVersion = uint8(0)          // headerless files predating SQ-4
	WALVersion1      = uint8(1)          // SQ-4 versioned files, no per-record type tag
	WALVersion2      = uint8(2)          // SQ-8 files: per-record type tag, no message extensions
	WALVersion3      = uint8(3)          // W2 files: per-record type tag + persisted message extensions (Headers/Expiration/EnqueueUnixMilli)
	WALFormatVersion = WALVersion3       // current file format version

	// Per-record type mechanism (SQ-4 reserved, SQ-8 implemented) and message
	// extensions (W2).
	//
	// v0/v1 files have no per-record type tag on disk: every record is
	// implicitly a message record (WALRecordTypeMessage). v2 adds a 1-byte
	// record-type tag immediately after the [CRC][length] framing, so that
	// non-message records (transaction-boundary markers) can be interleaved
	// with message records in the same file. v3 additionally persists a message
	// record's Headers/Expiration/EnqueueUnixMilli as an extension block after
	// the body (see serializeMessageVersioned). Two independent, version-gated
	// aspects therefore exist:
	//   - the per-record type tag is present iff version >= WALVersion2;
	//   - message extensions are present iff version >= WALVersion3 on the WRITE
	//     path, but on the READ path their presence is detected structurally
	//     (trailing bytes after the body) rather than by version, so a single
	//     physical file that mixes older-version records with v3 records — which
	//     happens when a pre-W2 file is reopened and appended to on restart — is
	//     recovered correctly regardless of the file-level header version.
	//   v0/v1: [CRC][len][<message payload, no extensions>]
	//   v2:    [CRC][len][1-byte recordType][<message payload, no extensions>]
	//   v3:    [CRC][len][1-byte recordType][<message payload>[extensions]]
	WALRecordTypeMessage    = uint8(0) // implicit type of every v0/v1 record
	WALRecordTypeTxBoundary = uint8(1) // v2 transaction-boundary marker (SQ-8)

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

// ErrUnsupportedWALVersion indicates a WAL file begins with the SQWAL magic but
// carries a format version this build cannot parse. Recovery surfaces this as a
// hard error rather than silently skipping the file, since silently ignoring an
// unknown-version file could drop durable messages.
var ErrUnsupportedWALVersion = errors.New("unsupported WAL file format version")

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
	mu        sync.Mutex
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
}

type ackRequest struct {
	queueName string
	offset    uint64
}

type walFileInfo struct {
	path      string
	offsets   *roaring64.Bitmap // actual message offsets in this file
	createdAt time.Time         // when this file was rolled (for retention)
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
	if wm.sharedWAL == nil {
		return fmt.Errorf("shared WAL not initialized")
	}

	// Get a response channel from the pool (avoids make(chan error, 1) per write)
	doneChan := getDoneChannel(&doneChannelPool)
	defer putDoneChannel(&doneChannelPool, doneChan)

	// Blocking send to channel - callers will wait if channel is full (true backpressure)
	wm.sharedWAL.writeChan <- &writeRequest{
		queueName: queueName,
		message:   message,
		offset:    offset,
		done:      doneChan,
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
	if wm.sharedWAL == nil {
		if onDone != nil {
			onDone(fmt.Errorf("shared WAL not initialized"))
		}
		return
	}

	wm.sharedWAL.writeChan <- &writeRequest{
		queueName: queueName,
		message:   message,
		offset:    offset,
		onDone:    onDone,
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
	if wm.sharedWAL == nil {
		return fmt.Errorf("shared WAL not initialized")
	}
	return wm.sharedWAL.writeTxAtomic(records)
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
	buf := serializeTxMarker(WALTxMarkerBegin, epoch)
	positions := make([]int64, len(records))
	for i, rec := range records {
		positions[i] = startPos + int64(len(buf))
		buf = append(buf, qw.serializeMessageVersioned(WALFormatVersion, rec.QueueName, rec.Message, rec.Offset)...)
	}
	buf = append(buf, serializeTxMarker(WALTxMarkerCommit, epoch)...)

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
	if wm.sharedWAL == nil {
		return
	}

	// Send to ACK channel (blocking with stopChan to prevent silent drops)
	select {
	case wm.sharedWAL.ackChan <- &ackRequest{
		queueName: queueName,
		offset:    offset,
	}:
	case <-wm.sharedWAL.stopChan:
		// WAL is shutting down, discard ACK
	}
}

// Read reads a message from shared WAL by queue+offset
func (wm *WALManager) Read(queueName string, offset uint64) (*protocol.Message, error) {
	if wm.sharedWAL == nil {
		return nil, fmt.Errorf("shared WAL not initialized")
	}

	return wm.sharedWAL.readMessage(queueName, offset)
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

	// Detect the file-level header (SQ-4) and start parsing records after it.
	// Legacy (v0) files have no header and parse from offset 0. An
	// unknown-version header returns a hard error rather than skipping. The
	// version drives whether records carry a per-record type tag (v2, SQ-8).
	dataStart, version, err := walFileDataStart(file)
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

	for {
		// Read CRC field (4 bytes)
		crcBytes := make([]byte, 4)
		_, err := file.Read(crcBytes)
		if err != nil {
			// End of file or read error
			break
		}
		crc := binary.BigEndian.Uint32(crcBytes)

		// Read length field (4 bytes)
		lengthBytes := make([]byte, 4)
		_, err = file.Read(lengthBytes)
		if err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(lengthBytes)

		// Read remaining data
		data := make([]byte, dataLen)
		_, err = file.Read(data)
		if err != nil {
			break
		}

		// Verify CRC
		crcCheckData := make([]byte, 4+dataLen)
		copy(crcCheckData[0:4], lengthBytes)
		copy(crcCheckData[4:], data)
		calculatedCRC := crc32.ChecksumIEEE(crcCheckData)

		if crc != calculatedCRC {
			// CRC mismatch - skip this entry
			continue
		}

		recType, payload := recordTypeAndPayload(version, data)

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

		default: // WALRecordTypeMessage
			recoveryMsg, ok := deserializeMessagePayload(payload)
			if !ok {
				continue
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
	for i, req := range batch {
		positions[i] = currentFilePos + int64(len(buf)) // Position in file
		buf = appendMessageRecord(buf, WALFormatVersion, req.queueName, req.message, req.offset)
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
		qw.deliverCompletions(batch, fmt.Errorf("WAL write failed: %w", err))
		return
	}

	// Record successful WAL writes
	if qw.metrics != nil {
		for range batch {
			qw.metrics.RecordWALWrite()
		}
	}

	// Fdatasync for durability (one sync per batch!)
	// Uses fdatasync on Linux (skips metadata sync for ~2-5x speedup vs fsync).
	// This is the critical durability guarantee - messages are only durable after this completes.
	fsyncStart := time.Now()
	fsyncErr := walGroupCommitFsync(qw.currentFile)
	fsyncDuration := time.Since(fsyncStart).Seconds()

	// Record fsync metrics
	if qw.metrics != nil {
		qw.metrics.RecordWALFsync(fsyncDuration)
		if fsyncErr != nil {
			qw.metrics.RecordWALWriteError()
		}
	}

	// If fsync failed, messages are NOT durable: do NOT update the index (so a
	// reader can never observe a non-durable record and no async caller ever
	// advances visibility), and deliver the error to every caller.
	if fsyncErr != nil {
		qw.fileMutex.Unlock()
		qw.deliverCompletions(batch, fsyncErr)
		return
	}

	// Durable. Update the offset index / current-file offset set / file offset
	// and roll BEFORE releasing the lock and BEFORE firing completions, so that
	// when a completion advances a message's visibility (async path), a woken
	// consumer's wal.Read finds the record.
	qw.offsetIndexMutex.Lock()
	for i, req := range batch {
		qw.offsetIndex[req.offset] = &offsetLocation{
			fileNum:      currentFileNum,
			filePosition: positions[i],
		}
	}
	qw.offsetIndexMutex.Unlock()

	// Track offsets written to current file
	qw.currentFileOffsetsMutex.Lock()
	for _, req := range batch {
		qw.currentFileOffsets.Add(req.offset)
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
	qw.deliverCompletions(batch, nil)
}

// deliverCompletions signals every request in the batch with err, iterating in
// enqueue order. A sync request's done is a cap-1 pooled channel (the send
// never blocks). An async request's onDone runs inline on the batch-writer
// goroutine; its contract is non-blocking (atomics + non-blocking pokes only),
// so it never stalls the shared writer. Exactly one of done/onDone is set per
// request. Enqueue-order iteration gives, for any single channel, non-
// decreasing confirm-tag completion order for single-target publishes — the
// property the per-channel confirm fold relies on.
func (qw *QueueWAL) deliverCompletions(batch []*writeRequest, err error) {
	for _, req := range batch {
		if req.onDone != nil {
			req.onDone(err)
		} else if req.done != nil {
			req.done <- err
		}
	}
}

// serializeMessageVersioned serializes a message with CRC, length, and (for v2+)
// a record-type tag, followed by queue name and offset.
//
// Framing:
//
//	v0/v1: [CRC][length][queue_len][queue][offset][exchange_len][exchange][routing_key_len][routing_key][delivery_mode][body_len][body]
//	v2:    [CRC][length][type=Message][queue_len]...[body]
//
// The version parameter exists so tests can craft legacy (v0/v1) records; the
// production write path always uses WALFormatVersion. Single allocation.
func (qw *QueueWAL) serializeMessageVersioned(version uint8, queueName string, message *protocol.Message, offset uint64) []byte {
	buf := make([]byte, 0, len(message.Body)+len(message.Exchange)+len(message.RoutingKey)+len(queueName)+256)
	return appendMessageRecord(buf, version, queueName, message, offset)
}

// appendMessageRecord serializes ONE message record onto the TAIL of buf (grow
// in place) and returns the grown buffer. It is the shared serializer for both
// the single-allocation serializeMessageVersioned wrapper (fresh buf, recStart
// == 0) and the group-commit batch writer (one persistent buf, many records
// packed back-to-back). The emitted bytes are byte-for-byte identical to the
// former in-line serializeMessageVersioned implementation — see the A1
// byte-identity golden test.
//
// Correctness of packing many records into one buffer rests on RECORD-RELATIVE
// backpatch: the CRC and length placeholders are patched at buf[recStart:] and
// the CRC is computed over buf[recStart+4:] (the record's own [length][data]
// span, which is at the tail because this record was just appended). recStart is
// an index, not a pointer, so it survives any growslice reallocation during the
// appends. The body is copied exactly once (the single inherent store-path copy).
func appendMessageRecord(buf []byte, version uint8, queueName string, message *protocol.Message, offset uint64) []byte {
	recStart := len(buf)

	// Reserve space for CRC and length (patched below, record-relative).
	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder

	// v2+: per-record type tag immediately after the framing.
	if version >= WALVersion2 {
		buf = append(buf, WALRecordTypeMessage)
	}

	// Write queue name
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(queueName)))
	buf = append(buf, queueName...)

	// Write offset
	buf = binary.BigEndian.AppendUint64(buf, offset)

	// Write exchange
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Exchange)))
	buf = append(buf, message.Exchange...)

	// Write routing key
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.RoutingKey)))
	buf = append(buf, message.RoutingKey...)

	// Write delivery mode (1 byte)
	buf = append(buf, message.DeliveryMode)

	// Write body (the ONE inherent store-path body copy)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Body)))
	buf = append(buf, message.Body...)

	// v3+: persist the message extension block (Headers/Expiration/
	// EnqueueUnixMilli) after the body. Older versions stop at the body; on the
	// read path the block's presence is detected structurally (trailing bytes),
	// so an old record and a v3 record can coexist in one physical file.
	if version >= WALVersion3 {
		buf = appendMessageExtensions(buf, message)
	}

	// Calculate actual length (excluding CRC and length fields), record-relative.
	dataLen := len(buf) - recStart - 8
	binary.BigEndian.PutUint32(buf[recStart+4:recStart+8], uint32(dataLen))

	// Calculate CRC over everything after this record's CRC field.
	crc := crc32.ChecksumIEEE(buf[recStart+4:])
	binary.BigEndian.PutUint32(buf[recStart:recStart+4], crc)

	return buf
}

// serializeTxMarker serializes a transaction-boundary marker record (SQ-8, v2
// only): [CRC][length][type=TxBoundary][kind][epoch]. kind is Begin or Commit.
func serializeTxMarker(kind uint8, epoch uint64) []byte {
	buf := make([]byte, 0, 8+1+walTxMarkerPayloadLen)
	buf = append(buf, 0, 0, 0, 0) // CRC placeholder
	buf = append(buf, 0, 0, 0, 0) // length placeholder
	buf = append(buf, WALRecordTypeTxBoundary)
	buf = append(buf, kind)
	buf = binary.BigEndian.AppendUint64(buf, epoch)

	dataLen := len(buf) - 8
	binary.BigEndian.PutUint32(buf[4:8], uint32(dataLen))
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)
	return buf
}

// recordTypeAndPayload splits a CRC-verified record's data region into its
// record type and the type-specific payload, honoring the file format version.
// The per-record type tag is present iff version >= WALVersion2; for v0/v1 (no
// on-disk type tag) every record is implicitly a message.
func recordTypeAndPayload(version uint8, data []byte) (recType uint8, payload []byte) {
	if version >= WALVersion2 {
		if len(data) < 1 {
			return WALRecordTypeMessage, data
		}
		return data[0], data[1:]
	}
	return WALRecordTypeMessage, data
}

// deserializeMessagePayload parses a message record's payload (everything after
// the record-type tag) into a RecoveryMessage. Returns ok=false on a truncated
// or malformed payload so callers can skip it.
func deserializeMessagePayload(payload []byte) (*RecoveryMessage, bool) {
	pos := 0
	if pos+4 > len(payload) {
		return nil, false
	}
	queueLen := binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	if pos+int(queueLen) > len(payload) {
		return nil, false
	}
	queueName := string(payload[pos : pos+int(queueLen)])
	pos += int(queueLen)

	if pos+8 > len(payload) {
		return nil, false
	}
	offset := binary.BigEndian.Uint64(payload[pos : pos+8])
	pos += 8

	if pos+4 > len(payload) {
		return nil, false
	}
	exchangeLen := binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	if pos+int(exchangeLen) > len(payload) {
		return nil, false
	}
	exchange := string(payload[pos : pos+int(exchangeLen)])
	pos += int(exchangeLen)

	if pos+4 > len(payload) {
		return nil, false
	}
	routingKeyLen := binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	if pos+int(routingKeyLen) > len(payload) {
		return nil, false
	}
	routingKey := string(payload[pos : pos+int(routingKeyLen)])
	pos += int(routingKeyLen)

	if pos+1 > len(payload) {
		return nil, false
	}
	deliveryMode := payload[pos]
	pos++

	if pos+4 > len(payload) {
		return nil, false
	}
	bodyLen := binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	if pos+int(bodyLen) > len(payload) {
		return nil, false
	}
	body := payload[pos : pos+int(bodyLen)]
	pos += int(bodyLen)

	msg := &protocol.Message{
		Exchange:     exchange,
		RoutingKey:   routingKey,
		DeliveryMode: deliveryMode,
		Body:         body,
		DeliveryTag:  offset,
		Redelivered:  true,
	}
	// v3+ records carry a Headers/Expiration/EnqueueUnixMilli extension block
	// after the body; older records stop here and default those fields.
	readMessageExtensions(payload, pos, msg)

	return &RecoveryMessage{
		QueueName: queueName,
		Offset:    offset,
		Message:   msg,
	}, true
}

// appendMessageExtensions appends the W2 message extension block — Expiration,
// EnqueueUnixMilli, and Headers — to buf, in that order:
//
//	[expiration_len:4][expiration][enqueue_unix_milli:8][headers_field_table]
//
// The headers field table is self-describing (its own uint32 length prefix, as
// produced by protocol.EncodeFieldTable), so an empty/nil Headers costs 4
// bytes. Shared by the WAL (v3+) and segment serializers so both persist an
// identical block. A Headers map holding a value the AMQP field-table encoder
// cannot emit (e.g. a Decimal, which the wire codec decodes but does not
// re-encode) is persisted as an empty table rather than corrupting the record
// or losing the message body — a bounded, pre-existing encoder limitation, not
// one W2 introduces.
func appendMessageExtensions(buf []byte, message *protocol.Message) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Expiration)))
	buf = append(buf, message.Expiration...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(message.EnqueueUnixMilli))
	if len(message.Headers) == 0 {
		// Fast path for the common no-headers message: append an empty field
		// table (uint32 length 0) directly, avoiding the EncodeFieldTable
		// allocation on the durable write.
		return binary.BigEndian.AppendUint32(buf, 0)
	}
	headerBytes, err := protocol.EncodeFieldTable(message.Headers)
	if err != nil {
		return binary.BigEndian.AppendUint32(buf, 0) // empty field table
	}
	return append(buf, headerBytes...)
}

// readMessageExtensions reconstructs the W2 extension block (Expiration,
// EnqueueUnixMilli, Headers) from payload starting at pos and populates them on
// message. It is best-effort and never fails the surrounding record: pos at the
// end of payload means an old-format record with no extension block (the fields
// stay at their zero values), and any malformed/truncated extension bytes stop
// parsing with whatever was reconstructed so far — a durable message is never
// dropped for the sake of its extension metadata (the record already passed a
// CRC check, so this is defensive). An empty decoded headers table is
// normalized to nil so a message with no headers reconstructs identically
// whether it was written before or after W2.
func readMessageExtensions(payload []byte, pos int, message *protocol.Message) {
	if pos >= len(payload) {
		return // old-format record: no extension block
	}
	if pos+4 > len(payload) {
		return
	}
	expLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if expLen < 0 || pos+expLen > len(payload) {
		return
	}
	message.Expiration = string(payload[pos : pos+expLen])
	pos += expLen

	if pos+8 > len(payload) {
		return
	}
	message.EnqueueUnixMilli = int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
	pos += 8

	if pos+4 > len(payload) {
		return
	}
	tblLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
	end := pos + 4 + tblLen
	if tblLen < 0 || end > len(payload) {
		return
	}
	if headers, err := protocol.DecodeFieldTable(payload[pos:end]); err == nil && len(headers) > 0 {
		message.Headers = headers
	}
}

// rollFile rolls to a new WAL file when size limit reached
func (qw *QueueWAL) rollFile() {
	currentFileNum := qw.fileNum.Load()
	currentPath := qw.currentFile.Name()

	qw.currentFileOffsetsMutex.Lock()
	fileOffsets := qw.currentFileOffsets
	qw.currentFileOffsets = roaring64.New()
	qw.currentFileOffsetsMutex.Unlock()

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
		path:      currentPath,
		offsets:   fileOffsets,
		createdAt: time.Now(),
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
// byte offset at which record data begins, plus the detected format version.
//
// Detection (SQ-4):
//   - If the first WALHeaderSize bytes start with WALMagic, the file is
//     versioned. The trailing byte is the version; if it is not a version this
//     build understands, ErrUnsupportedWALVersion is returned (a clear error,
//     never a silent skip). Records begin at WALHeaderSize.
//   - Otherwise the file is a legacy (v0) headerless file written before SQ-4;
//     records begin at offset 0 so existing data dirs keep working.
//   - A file too short to hold a header is treated as legacy/empty (start 0).
func walFileDataStart(f *os.File) (dataStart int64, version uint8, err error) {
	hdr := make([]byte, WALHeaderSize)
	n, rerr := f.ReadAt(hdr, 0)
	if n < WALHeaderSize {
		// Too short to contain a header (possibly empty). Non-EOF read errors
		// are surfaced; short/empty files parse from offset 0 as legacy.
		if rerr != nil && rerr != io.EOF {
			return 0, WALLegacyVersion, rerr
		}
		return 0, WALLegacyVersion, nil
	}
	if string(hdr[:len(WALMagic)]) != WALMagic {
		// No magic → legacy headerless file.
		return 0, WALLegacyVersion, nil
	}
	v := hdr[len(WALMagic)]
	// This build understands v1 (SQ-4, no per-record type tag), v2 (SQ-8,
	// per-record type tag), and v3 (W2, per-record type tag + message
	// extensions). Any other version is a hard error, never a silent skip. The
	// returned version drives record parsing (type tag present iff >= v2;
	// message extensions are detected structurally, not by version).
	if v != WALVersion1 && v != WALVersion2 && v != WALVersion3 {
		return 0, v, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedWALVersion, v, WALFormatVersion)
	}
	return int64(WALHeaderSize), v, nil
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

	// Verify CRC (calculated over [length_bytes][data])
	crcCheckData := make([]byte, 4+dataLen)
	copy(crcCheckData[0:4], header[4:8])
	copy(crcCheckData[4:], data)
	calculatedCRC := crc32.ChecksumIEEE(crcCheckData)

	if crc != calculatedCRC {
		return nil, fmt.Errorf("CRC mismatch for offset %d (expected %d, got %d)", expectedOffset, crc, calculatedCRC)
	}

	// The offset index is populated only by writes made in the current process,
	// which always use the current WAL format (v3). Strip the per-record type
	// tag, confirm it is a message record, and reconstruct via the shared
	// payload parser so the live read path recovers the message extensions
	// (Headers/Expiration/EnqueueUnixMilli) exactly like crash recovery does.
	recType, payload := recordTypeAndPayload(WALFormatVersion, data)
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

	// This is a positional re-read, not a post-restart redelivery, so the
	// message is not marked redelivered (deserializeMessagePayload defaults it
	// to true for the crash-recovery path).
	rm.Message.Redelivered = false
	return rm.Message, nil
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

	// Skip the file-level header (SQ-4) before scanning records. Legacy files
	// parse from offset 0. The version drives per-record type-tag parsing (v2).
	dataStart, version, err := walFileDataStart(file)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(dataStart, io.SeekStart); err != nil {
		return nil, err
	}

	// Sequential scan through shared WAL entries
	for {
		// Read CRC field (4 bytes)
		crcBytes := make([]byte, 4)
		_, err := file.Read(crcBytes)
		if err != nil {
			return nil, err
		}
		crc := binary.BigEndian.Uint32(crcBytes)

		// Read length field (4 bytes)
		lengthBytes := make([]byte, 4)
		_, err = file.Read(lengthBytes)
		if err != nil {
			return nil, err
		}
		dataLen := binary.BigEndian.Uint32(lengthBytes)

		// Read remaining data (queue + offset + message)
		data := make([]byte, dataLen)
		_, err = file.Read(data)
		if err != nil {
			return nil, err
		}

		// Verify CRC (calculated over [length_bytes][data])
		crcCheckData := make([]byte, 4+dataLen)
		copy(crcCheckData[0:4], lengthBytes)
		copy(crcCheckData[4:], data)
		calculatedCRC := crc32.ChecksumIEEE(crcCheckData)

		if crc != calculatedCRC {
			return nil, fmt.Errorf("CRC mismatch in sequential scan")
		}

		recType, payload := recordTypeAndPayload(version, data)
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
