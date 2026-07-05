package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	// Segment settings
	DefaultSegmentSize         = 1024 * 1024 * 1024 // 1 GB per segment
	SegmentCheckpointInterval  = 5 * time.Minute    // Checkpoint WAL to segments every 5 minutes
	DefaultCompactionThreshold = 0.5                // Compact when >50% messages deleted
	SegmentFileExtension       = ".seg"
	SegmentIndexFileExtension  = ".idx"

	// Segment message format: [4 bytes CRC32][4 bytes length][8 bytes offset][N bytes message]
	SegmentHeaderSize = 16

	// SQ-4 NOTE: segment files intentionally do NOT carry a file-level format
	// version header (unlike WAL files). Adding one here is not trivially
	// symmetric with the WAL change: segment index positions are absolute file
	// offsets, and compaction (compactSegment) rewrites a segment in place via
	// serializeSegmentMessage + rename, which would need to re-emit and re-skip
	// a header on every rewrite. Versioning segments is deferred until there is
	// a concrete format change that requires it; the WAL header alone satisfies
	// the Wave 0 durability requirement.
)

// SegmentConfig holds configurable parameters for the segment manager
type SegmentConfig struct {
	SegmentSize         int64
	CompactionThreshold float64
	CompactionInterval  time.Duration
	CheckpointInterval  time.Duration
}

// DefaultSegmentConfig returns a SegmentConfig with production defaults
func DefaultSegmentConfig() SegmentConfig {
	return SegmentConfig{
		SegmentSize:         DefaultSegmentSize,
		CompactionThreshold: DefaultCompactionThreshold,
		CompactionInterval:  1 * time.Minute,
		CheckpointInterval:  SegmentCheckpointInterval,
	}
}

// SegmentMetrics interface for metrics collection
type SegmentMetrics interface {
	UpdateSegmentMetrics(queueName string, count, sizeBytes float64)
	RecordSegmentCompaction()
	RecordSegmentReadError()
}

// SegmentManager manages long-term cold storage with compaction
// Messages are checkpointed from WAL to segments periodically
type SegmentManager struct {
	dataDir       string
	queueSegments sync.Map // queueName -> *QueueSegments
	metrics       SegmentMetrics
	cfg           SegmentConfig
}

// QueueSegments manages segments for a single queue
type QueueSegments struct {
	queueName string
	dataDir   string
	cfg       SegmentConfig

	// Active segment being written to
	currentSegment *SegmentFile
	currentIndex   *SegmentIndex
	mutex          sync.Mutex

	// Sealed segments (read-only)
	sealedSegments map[uint64]*SegmentFile
	sealedMutex    sync.RWMutex

	// ACK tracking for compaction
	ackBitmap   *roaring64.Bitmap
	bitmapMutex sync.RWMutex

	// Batch ACK channel (M2: reduces bitmapMutex contention from per-ACK to per-batch)
	ackChan      chan uint64
	ackBatchSize int

	// Compaction state
	lastCompaction time.Time
	compactionMux  sync.Mutex

	// Metrics collector
	metrics SegmentMetrics

	// Background goroutines
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// SegmentFile represents a single segment file
type SegmentFile struct {
	segmentNum uint64
	path       string
	indexPath  string
	file       *os.File

	// Metadata
	minOffset    uint64
	maxOffset    uint64
	messageCount atomic.Uint64
	deletedCount atomic.Uint64
	fileSize     atomic.Int64

	// Index for fast lookups
	index map[uint64]int64 // offset -> file position
	mutex sync.RWMutex
}

// SegmentIndex provides fast offset -> file position lookups
type SegmentIndex struct {
	segmentNum uint64
	entries    map[uint64]int64 // offset -> file position
	mutex      sync.RWMutex
}

// NewSegmentManager creates a new segment manager with default config
func NewSegmentManager(dataDir string) (*SegmentManager, error) {
	return NewSegmentManagerWithConfig(dataDir, DefaultSegmentConfig())
}

// NewSegmentManagerWithConfig creates a new segment manager with custom config
func NewSegmentManagerWithConfig(dataDir string, cfg SegmentConfig) (*SegmentManager, error) {
	segDir := filepath.Join(dataDir, "segments")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segments directory: %w", err)
	}

	return &SegmentManager{
		dataDir: segDir,
		cfg:     cfg,
	}, nil
}

// SetMetrics sets the metrics collector for the segment manager
func (sm *SegmentManager) SetMetrics(metrics SegmentMetrics) {
	sm.metrics = metrics
	// Update metrics for existing queues
	sm.queueSegments.Range(func(key, value interface{}) bool {
		if qs, ok := value.(*QueueSegments); ok {
			qs.metrics = metrics
		}
		return true
	})
}

// Write writes a message to segments (called during checkpoint from WAL)
func (sm *SegmentManager) Write(queueName string, message *protocol.Message, offset uint64) error {
	segments := sm.getOrCreateQueueSegments(queueName)
	return segments.writeMessage(message, offset)
}

// Read reads a message from segments by offset
func (sm *SegmentManager) Read(queueName string, offset uint64) (*protocol.Message, error) {
	val, ok := sm.queueSegments.Load(queueName)
	if !ok {
		return nil, fmt.Errorf("segments not found for queue %s", queueName)
	}
	segments := val.(*QueueSegments)

	return segments.readMessage(offset)
}

// Acknowledge marks a message as ACKed for future compaction.
// M2: Sends the offset to a batch ACK channel instead of locking bitmapMutex
// per call. The batchAckLoop goroutine collects ACKs and applies them in
// batches, reducing lock contention from O(N) to O(N/batchSize).
func (sm *SegmentManager) Acknowledge(queueName string, offset uint64) {
	val, ok := sm.queueSegments.Load(queueName)
	if !ok {
		return
	}
	segments := val.(*QueueSegments)

	// Non-blocking send to batch ACK channel (drop if full to prevent backpressure)
	select {
	case segments.ackChan <- offset:
	default:
		// Channel full — apply directly as fallback
		segments.applyAck(offset)
	}
}

// applyAck applies a single ACK to the bitmap and segment counters.
func (qs *QueueSegments) applyAck(offset uint64) {
	qs.bitmapMutex.Lock()
	qs.ackBitmap.Add(offset)
	qs.bitmapMutex.Unlock()
	qs.acknowledgeInSegment(offset)
}

// batchAckLoop collects ACKs from the channel and applies them in batches,
// reducing bitmapMutex lock contention from per-ACK to per-batch.
func (qs *QueueSegments) batchAckLoop() {
	defer qs.wg.Done()

	batch := make([]uint64, 0, qs.ackBatchSize)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Single lock acquisition for the entire batch
		qs.bitmapMutex.Lock()
		for _, offset := range batch {
			qs.ackBitmap.Add(offset)
		}
		qs.bitmapMutex.Unlock()

		// Update segment-level deleted counts (no bitmap lock needed)
		for _, offset := range batch {
			qs.acknowledgeInSegment(offset)
		}
		batch = batch[:0]
	}

	for {
		select {
		case offset := <-qs.ackChan:
			batch = append(batch, offset)
			if len(batch) >= qs.ackBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-qs.stopChan:
			// Drain remaining ACKs before exit
			for {
				select {
				case offset := <-qs.ackChan:
					batch = append(batch, offset)
				default:
					flush()
					return
				}
			}
		}
	}
}

// acknowledgeInSegment finds the segment containing the offset and increments its deletedCount
func (qs *QueueSegments) acknowledgeInSegment(offset uint64) {
	// Check current segment first
	qs.mutex.Lock()
	if qs.currentSegment != nil {
		if qs.currentIndex != nil {
			qs.currentIndex.mutex.RLock()
			_, exists := qs.currentIndex.entries[offset]
			qs.currentIndex.mutex.RUnlock()
			if exists {
				qs.currentSegment.deletedCount.Add(1)
				qs.mutex.Unlock()
				return
			}
		}
	}
	qs.mutex.Unlock()

	// Check sealed segments
	qs.sealedMutex.RLock()
	defer qs.sealedMutex.RUnlock()

	for _, seg := range qs.sealedSegments {
		if offset >= seg.minOffset && offset <= seg.maxOffset {
			seg.mutex.RLock()
			_, exists := seg.index[offset]
			seg.mutex.RUnlock()
			if exists {
				seg.deletedCount.Add(1)
				return
			}
		}
	}
}

// CheckpointBatch writes a batch of recovery messages to segments for a given queue.
// M5: All messages are written in a single file.Write() call with a single
// mutex acquisition, reducing lock contention and write syscalls from O(N) to O(1).
// Used during WAL checkpoint to migrate messages from WAL to cold storage.
func (sm *SegmentManager) CheckpointBatch(queueName string, messages []*RecoveryMessage) error {
	if len(messages) == 0 {
		return nil
	}
	segments := sm.getOrCreateQueueSegments(queueName)
	if err := segments.writeMessageBatch(messages); err != nil {
		return fmt.Errorf("checkpoint batch write failed for queue %s: %w", queueName, err)
	}
	// Fdatasync the segment file to ensure durability before the WAL file is deleted.
	// Without this, a crash after WAL deletion but before segment fsync would lose messages.
	if err := segments.sync(); err != nil {
		return fmt.Errorf("checkpoint fsync failed for queue %s: %w", queueName, err)
	}
	return nil
}

// writeMessageBatch writes multiple messages to the current segment in a single
// file write, with a single mutex acquisition. This is O(1) in lock acquisitions
// and write syscalls vs O(N) for individual writeMessage calls.
func (qs *QueueSegments) writeMessageBatch(messages []*RecoveryMessage) error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	if qs.currentSegment == nil {
		return fmt.Errorf("no active segment")
	}

	// Serialize all messages and build a single write buffer + index updates
	type indexUpdate struct {
		offset   uint64
		position int64
	}
	updates := make([]indexUpdate, 0, len(messages))
	var buf []byte

	for _, rm := range messages {
		position := qs.currentSegment.fileSize.Load() + int64(len(buf))
		msgBytes := serializeSegmentMessage(rm.Message, rm.Offset)
		buf = append(buf, msgBytes...)
		updates = append(updates, indexUpdate{offset: rm.Offset, position: position})
	}

	// Single write syscall for all messages
	n, err := qs.currentSegment.file.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write batch to segment: %w", err)
	}

	// Update index entries in a single lock acquisition
	qs.currentIndex.mutex.Lock()
	for _, u := range updates {
		qs.currentIndex.entries[u.offset] = u.position
	}
	qs.currentIndex.mutex.Unlock()

	// Update segment metadata
	qs.currentSegment.fileSize.Add(int64(n))
	qs.currentSegment.messageCount.Add(uint64(len(messages)))
	for _, rm := range messages {
		if rm.Offset < qs.currentSegment.minOffset || qs.currentSegment.minOffset == 0 {
			qs.currentSegment.minOffset = rm.Offset
		}
		if rm.Offset > qs.currentSegment.maxOffset {
			qs.currentSegment.maxOffset = rm.Offset
		}
	}

	// Check if we need to roll to new segment
	if qs.currentSegment.fileSize.Load() >= qs.cfg.SegmentSize {
		if err := qs.sealSegment(); err != nil {
			return fmt.Errorf("failed to seal segment during batch write: %w", err)
		}
		if err := qs.openNextSegmentLocked(); err != nil {
			return fmt.Errorf("failed to open new segment during batch write: %w", err)
		}
	}

	return nil
}

// sync flushes the current segment file to disk
func (qs *QueueSegments) sync() error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()
	if qs.currentSegment != nil && qs.currentSegment.file != nil {
		return fdatasyncFile(qs.currentSegment.file)
	}
	return nil
}

// Close closes all segments
func (sm *SegmentManager) Close() error {
	sm.queueSegments.Range(func(key, value interface{}) bool {
		segments := value.(*QueueSegments)
		segments.close()
		return true
	})
	return nil
}

// getOrCreateQueueSegments returns or creates segments for a queue
func (sm *SegmentManager) getOrCreateQueueSegments(queueName string) *QueueSegments {
	val, ok := sm.queueSegments.Load(queueName)
	if ok {
		return val.(*QueueSegments)
	}

	// Create new QueueSegments
	queueDir := filepath.Join(sm.dataDir, queueName)
	_ = os.MkdirAll(queueDir, 0755)

	// Batch ACK settings
	const ackChannelBuffer = 1000
	const ackBatchSize = 100

	segments := &QueueSegments{
		queueName:      queueName,
		dataDir:        queueDir,
		cfg:            sm.cfg,
		sealedSegments: make(map[uint64]*SegmentFile),
		ackBitmap:      roaring64.New(),
		ackChan:        make(chan uint64, ackChannelBuffer),
		ackBatchSize:   ackBatchSize,
		stopChan:       make(chan struct{}),
		metrics:        sm.metrics,
	}

	// Load existing segments before opening a new active segment
	segments.loadExistingSegments()

	// Create first segment
	if err := segments.openNextSegment(); err != nil {
		return nil
	}

	// Start background goroutines
	segments.wg.Add(2)
	go segments.compactionLoop()
	go segments.batchAckLoop()

	// Store and return
	actual, loaded := sm.queueSegments.LoadOrStore(queueName, segments)
	if loaded {
		segments.close()
		return actual.(*QueueSegments)
	}

	return segments
}

// writeMessage writes a message to the current active segment
func (qs *QueueSegments) writeMessage(message *protocol.Message, offset uint64) error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	if qs.currentSegment == nil {
		return fmt.Errorf("no active segment")
	}

	// Serialize message
	msgBytes := serializeSegmentMessage(message, offset)

	// Get current file position
	position := qs.currentSegment.fileSize.Load()

	// Write to file
	n, err := qs.currentSegment.file.Write(msgBytes)
	if err != nil {
		return fmt.Errorf("failed to write to segment: %w", err)
	}

	// Update index
	qs.currentIndex.mutex.Lock()
	qs.currentIndex.entries[offset] = position
	qs.currentIndex.mutex.Unlock()

	// Update metadata
	qs.currentSegment.fileSize.Add(int64(n))
	qs.currentSegment.messageCount.Add(1)
	if offset < qs.currentSegment.minOffset || qs.currentSegment.minOffset == 0 {
		qs.currentSegment.minOffset = offset
	}
	if offset > qs.currentSegment.maxOffset {
		qs.currentSegment.maxOffset = offset
	}

	// Check if we need to roll to new segment.
	// openNextSegmentLocked is called instead of openNextSegment because we
	// already hold qs.mutex here — openNextSegment would deadlock.
	if qs.currentSegment.fileSize.Load() >= qs.cfg.SegmentSize {
		if err := qs.sealSegment(); err != nil {
			return fmt.Errorf("failed to seal segment: %w", err)
		}
		if err := qs.openNextSegmentLocked(); err != nil {
			return fmt.Errorf("failed to open new segment: %w", err)
		}
	}

	return nil
}

// readMessage reads a message from segments by offset
func (qs *QueueSegments) readMessage(offset uint64) (*protocol.Message, error) {
	// Try current segment first
	qs.mutex.Lock()
	currentSeg := qs.currentSegment
	currentIdx := qs.currentIndex
	qs.mutex.Unlock()

	if currentSeg != nil {
		currentIdx.mutex.RLock()
		position, found := currentIdx.entries[offset]
		currentIdx.mutex.RUnlock()

		if found {
			msg, err := readSegmentMessageAt(currentSeg.file, position)
			if err != nil && qs.metrics != nil {
				qs.metrics.RecordSegmentReadError()
			}
			return msg, err
		}
	}

	// Try sealed segments
	qs.sealedMutex.RLock()
	defer qs.sealedMutex.RUnlock()

	for _, segment := range qs.sealedSegments {
		if offset >= segment.minOffset && offset <= segment.maxOffset {
			segment.mutex.RLock()
			position, found := segment.index[offset]
			segment.mutex.RUnlock()

			if found {
				msg, err := readSegmentMessageAt(segment.file, position)
				if err != nil && qs.metrics != nil {
					qs.metrics.RecordSegmentReadError()
				}
				return msg, err
			}
		}
	}

	// Record as error since message wasn't found
	if qs.metrics != nil {
		qs.metrics.RecordSegmentReadError()
	}
	return nil, fmt.Errorf("message not in segments")
}

// sealSegment moves current segment to sealed segments.
// Returns an error if the segment file cannot be synced to disk.
func (qs *QueueSegments) sealSegment() error {
	if qs.currentSegment == nil {
		return nil
	}

	// Sync to disk — must succeed for durability before sealing
	if err := qs.currentSegment.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment before sealing: %w", err)
	}

	// Copy index entries to the segment's own index for sealed-segment reads
	if qs.currentIndex != nil {
		qs.currentIndex.mutex.RLock()
		qs.currentSegment.mutex.Lock()
		for offset, position := range qs.currentIndex.entries {
			qs.currentSegment.index[offset] = position
		}
		qs.currentSegment.mutex.Unlock()
		qs.currentIndex.mutex.RUnlock()
	}

	// Reopen file as read-only for sealed segment reads
	readFile, err := os.Open(qs.currentSegment.path)
	if err == nil {
		_ = qs.currentSegment.file.Close()
		qs.currentSegment.file = readFile
	}

	// Move to sealed segments
	qs.sealedMutex.Lock()
	qs.sealedSegments[qs.currentSegment.segmentNum] = qs.currentSegment
	qs.sealedMutex.Unlock()

	// Write index to disk
	qs.writeIndexToDisk(qs.currentSegment.segmentNum, qs.currentIndex)

	return nil
}

// openNextSegment creates and opens the next segment file (acquires qs.mutex).
// Use openNextSegmentLocked when the caller already holds qs.mutex.
func (qs *QueueSegments) openNextSegment() error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()
	return qs.openNextSegmentLocked()
}

// openNextSegmentLocked creates and opens the next segment file.
// Precondition: caller must hold qs.mutex.
func (qs *QueueSegments) openNextSegmentLocked() error {
	segmentNum := uint64(time.Now().UnixNano())
	filename := filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", segmentNum, SegmentFileExtension))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}

	qs.currentSegment = &SegmentFile{
		segmentNum: segmentNum,
		path:       filename,
		indexPath:  filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", segmentNum, SegmentIndexFileExtension)),
		file:       file,
		index:      make(map[uint64]int64),
	}

	qs.currentIndex = &SegmentIndex{
		segmentNum: segmentNum,
		entries:    make(map[uint64]int64),
	}

	return nil
}

// compactionLoop periodically checks if compaction is needed
func (qs *QueueSegments) compactionLoop() {
	defer qs.wg.Done()

	ticker := time.NewTicker(qs.cfg.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qs.tryCompaction()

		case <-qs.stopChan:
			return
		}
	}
}

// tryCompaction checks if any segments need compaction
func (qs *QueueSegments) tryCompaction() {
	qs.sealedMutex.RLock()
	segmentsToCompact := make([]*SegmentFile, 0)

	for _, segment := range qs.sealedSegments {
		deletedCount := segment.deletedCount.Load()
		totalCount := segment.messageCount.Load()

		if totalCount > 0 {
			deletionRatio := float64(deletedCount) / float64(totalCount)
			if deletionRatio > qs.cfg.CompactionThreshold {
				segmentsToCompact = append(segmentsToCompact, segment)
			}
		}
	}
	qs.sealedMutex.RUnlock()

	// Compact segments
	for _, segment := range segmentsToCompact {
		qs.compactSegment(segment)
	}
}

// compactSegment rewrites a segment excluding deleted messages
func (qs *QueueSegments) compactSegment(segment *SegmentFile) {
	qs.compactionMux.Lock()
	defer qs.compactionMux.Unlock()

	// Record compaction start
	if qs.metrics != nil {
		qs.metrics.RecordSegmentCompaction()
	}

	// Create new temporary segment
	tempPath := segment.path + ".compact"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer tempFile.Close()

	// Read all messages from old segment
	segment.mutex.RLock()
	oldIndex := make(map[uint64]int64)
	for offset, position := range segment.index {
		oldIndex[offset] = position
	}
	segment.mutex.RUnlock()

	// Write non-deleted messages to new segment
	newIndex := make(map[uint64]int64)
	var newPosition int64

	qs.bitmapMutex.RLock()
	for offset, oldPosition := range oldIndex {
		if !qs.ackBitmap.Contains(offset) {
			// Message not ACKed - copy to new segment
			msg, err := readSegmentMessageAt(segment.file, oldPosition)
			if err == nil {
				msgBytes := serializeSegmentMessage(msg, offset)
				n, err := tempFile.Write(msgBytes)
				if err == nil {
					newIndex[offset] = newPosition
					newPosition += int64(n)
				}
			}
		}
	}
	qs.bitmapMutex.RUnlock()

	// Sync new file
	_ = tempFile.Sync()
	tempFile.Close()

	// Atomically replace old segment with compacted one
	_ = os.Rename(tempPath, segment.path)

	// Reopen segment file
	newFile, err := os.OpenFile(segment.path, os.O_RDONLY, 0644)
	if err != nil {
		return
	}

	// Update segment
	segment.mutex.Lock()
	_ = segment.file.Close()
	segment.file = newFile
	segment.index = newIndex
	segment.messageCount.Store(uint64(len(newIndex)))
	segment.deletedCount.Store(0)
	segment.mutex.Unlock()
}

// writeIndexToDisk writes segment index to disk
func (qs *QueueSegments) writeIndexToDisk(segmentNum uint64, index *SegmentIndex) error {
	indexPath := filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", segmentNum, SegmentIndexFileExtension))

	file, err := os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	index.mutex.RLock()
	defer index.mutex.RUnlock()

	// Write number of entries
	numEntries := uint64(len(index.entries))
	if err := binary.Write(file, binary.BigEndian, numEntries); err != nil {
		return fmt.Errorf("failed to write index entry count: %w", err)
	}

	// Write each entry: [8 bytes offset][8 bytes position]
	for offset, position := range index.entries {
		if err := binary.Write(file, binary.BigEndian, offset); err != nil {
			return fmt.Errorf("failed to write index offset: %w", err)
		}
		if err := binary.Write(file, binary.BigEndian, uint64(position)); err != nil {
			return fmt.Errorf("failed to write index position: %w", err)
		}
	}

	return file.Sync()
}

func (qs *QueueSegments) loadExistingSegments() {
	entries, err := os.ReadDir(qs.dataDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), SegmentFileExtension) {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), SegmentFileExtension)
		segmentNum, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}

		segPath := filepath.Join(qs.dataDir, entry.Name())
		_ = qs.loadSegmentFile(segmentNum, segPath)
	}
}

func (qs *QueueSegments) loadSegmentFile(segmentNum uint64, segPath string) error {
	file, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	index := make(map[uint64]int64)
	var minOffset, maxOffset uint64
	var messageCount uint64
	var pos int64

	for {
		if pos+8 > fileSize {
			break
		}

		header := make([]byte, 8)
		if _, err := file.ReadAt(header, pos); err != nil {
			break
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		dataLen := binary.BigEndian.Uint32(header[4:8])

		if pos+8+int64(dataLen) > fileSize {
			break
		}

		data := make([]byte, dataLen)
		if _, err := file.ReadAt(data, pos+8); err != nil {
			break
		}

		crcData := make([]byte, 4+dataLen)
		copy(crcData[0:4], header[4:8])
		copy(crcData[4:], data)
		if crc32.ChecksumIEEE(crcData) != storedCRC {
			break
		}

		if len(data) < 4 {
			break
		}
		queueLen := binary.BigEndian.Uint32(data[0:4])
		offStart := 4 + int(queueLen)
		if offStart+8 > len(data) {
			break
		}
		offset := binary.BigEndian.Uint64(data[offStart : offStart+8])

		index[offset] = pos
		if minOffset == 0 || offset < minOffset {
			minOffset = offset
		}
		if offset > maxOffset {
			maxOffset = offset
		}
		messageCount++
		pos += 8 + int64(dataLen)
	}

	if pos < fileSize {
		_ = file.Truncate(pos)
	}

	readFile, err := os.Open(segPath)
	if err != nil {
		return err
	}

	seg := &SegmentFile{
		segmentNum: segmentNum,
		path:       segPath,
		indexPath:  filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", segmentNum, SegmentIndexFileExtension)),
		file:       readFile,
		index:      index,
		minOffset:  minOffset,
		maxOffset:  maxOffset,
	}
	seg.messageCount.Store(messageCount)
	seg.fileSize.Store(pos)

	qs.sealedMutex.Lock()
	qs.sealedSegments[segmentNum] = seg
	qs.sealedMutex.Unlock()

	return nil
}

func (qs *QueueSegments) collectAllMessages() []*RecoveryMessage {
	var messages []*RecoveryMessage

	qs.sealedMutex.RLock()
	for _, seg := range qs.sealedSegments {
		seg.mutex.RLock()
		for offset, position := range seg.index {
			msg, err := readSegmentMessageAt(seg.file, position)
			if err == nil {
				messages = append(messages, &RecoveryMessage{
					QueueName: qs.queueName,
					Offset:    offset,
					Message:   msg,
				})
			}
		}
		seg.mutex.RUnlock()
	}
	qs.sealedMutex.RUnlock()

	qs.mutex.Lock()
	currentSeg := qs.currentSegment
	currentIdx := qs.currentIndex
	qs.mutex.Unlock()

	if currentSeg != nil && currentIdx != nil {
		currentIdx.mutex.RLock()
		for offset, position := range currentIdx.entries {
			msg, err := readSegmentMessageAt(currentSeg.file, position)
			if err == nil {
				messages = append(messages, &RecoveryMessage{
					QueueName: qs.queueName,
					Offset:    offset,
					Message:   msg,
				})
			}
		}
		currentIdx.mutex.RUnlock()
	}

	return messages
}

func (sm *SegmentManager) RecoverFromSegments() (map[string][]*RecoveryMessage, error) {
	result := make(map[string][]*RecoveryMessage)

	entries, err := os.ReadDir(sm.dataDir)
	if err != nil {
		return result, nil
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		queueName := entry.Name()
		qs := sm.getOrCreateQueueSegments(queueName)
		if qs == nil {
			continue
		}

		messages := qs.collectAllMessages()
		if len(messages) > 0 {
			result[queueName] = messages
		}
	}

	return result, nil
}

// close stops background goroutines and closes files
func (qs *QueueSegments) close() {
	close(qs.stopChan)
	qs.wg.Wait()

	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	if qs.currentSegment != nil {
		_ = qs.currentSegment.file.Sync()
		_ = qs.currentSegment.file.Close()
	}

	qs.sealedMutex.Lock()
	defer qs.sealedMutex.Unlock()

	for _, segment := range qs.sealedSegments {
		_ = segment.file.Close()
	}
}

// Helper functions

// serializeSegmentMessage serializes a message with full AMQP metadata.
// Format: [CRC][length][queue_len][queue][offset][exchange_len][exchange][routing_key_len][routing_key][delivery_mode][body_len][body]
// This matches the WAL serialization format to preserve Exchange, RoutingKey, DeliveryMode.
func serializeSegmentMessage(message *protocol.Message, offset uint64) []byte {
	bodySize := len(message.Body)
	totalSize := 8 + 4 + len(message.Exchange) + 4 + len(message.RoutingKey) + 1 + 4 + bodySize + 256

	buf := make([]byte, 0, totalSize)

	// Reserve space for CRC and length
	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder

	// Write queue name (empty for segment messages — queue is known by the segment's queue)
	queueName := ""
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(queueName)))
	buf = append(buf, []byte(queueName)...)

	// Write offset
	buf = binary.BigEndian.AppendUint64(buf, offset)

	// Write exchange
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.Exchange)))
	buf = append(buf, []byte(message.Exchange)...)

	// Write routing key
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(message.RoutingKey)))
	buf = append(buf, []byte(message.RoutingKey)...)

	// Write delivery mode (1 byte)
	buf = append(buf, message.DeliveryMode)

	// Write body
	buf = binary.BigEndian.AppendUint32(buf, uint32(bodySize))
	buf = append(buf, message.Body...)

	// Calculate actual length (excluding CRC and length fields)
	dataLen := len(buf) - 8
	binary.BigEndian.PutUint32(buf[4:8], uint32(dataLen))

	// Calculate CRC over everything after CRC field
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf
}

func readSegmentMessageAt(file *os.File, position int64) (*protocol.Message, error) {
	// Read header (CRC + length, 8 bytes)
	header := make([]byte, 8)
	_, err := file.ReadAt(header, position)
	if err != nil {
		return nil, err
	}

	// Parse header
	storedCRC := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	// Read data portion: [length][queue_len][queue][offset][exchange_len][exchange][routing_key_len][routing_key][delivery_mode][body_len][body]
	crcData := make([]byte, 4+dataLen)
	copy(crcData[0:4], header[4:8]) // length field
	_, err = file.ReadAt(crcData[4:], position+8)
	if err != nil {
		return nil, err
	}

	// Verify CRC over [length][data...]
	calculatedCRC := crc32.ChecksumIEEE(crcData)
	if calculatedCRC != storedCRC {
		return nil, fmt.Errorf("CRC mismatch")
	}

	// Parse data: skip length field, then [queue_len][queue][offset][exchange_len][exchange][routing_key_len][routing_key][delivery_mode][body_len][body]
	data := crcData[4:] // skip length field
	pos := 0

	// Read queue name (ignored — queue is known by the segment's queue)
	if len(data) < pos+4 {
		return nil, fmt.Errorf("invalid segment data: too short for queue length")
	}
	queueLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(queueLen) {
		return nil, fmt.Errorf("invalid segment data: too short for queue name")
	}
	pos += int(queueLen) // skip queue name

	// Read offset
	if len(data) < pos+8 {
		return nil, fmt.Errorf("invalid segment data: too short for offset")
	}
	offset := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	// Read exchange
	if len(data) < pos+4 {
		return nil, fmt.Errorf("invalid segment data: too short for exchange length")
	}
	exchangeLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(exchangeLen) {
		return nil, fmt.Errorf("invalid segment data: too short for exchange")
	}
	exchange := string(data[pos : pos+int(exchangeLen)])
	pos += int(exchangeLen)

	// Read routing key
	if len(data) < pos+4 {
		return nil, fmt.Errorf("invalid segment data: too short for routing key length")
	}
	routingKeyLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(routingKeyLen) {
		return nil, fmt.Errorf("invalid segment data: too short for routing key")
	}
	routingKey := string(data[pos : pos+int(routingKeyLen)])
	pos += int(routingKeyLen)

	// Read delivery mode
	if len(data) < pos+1 {
		return nil, fmt.Errorf("invalid segment data: too short for delivery mode")
	}
	deliveryMode := data[pos]
	pos += 1

	// Read body
	if len(data) < pos+4 {
		return nil, fmt.Errorf("invalid segment data: too short for body length")
	}
	bodyLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(bodyLen) {
		return nil, fmt.Errorf("invalid segment data: too short for body")
	}
	body := data[pos : pos+int(bodyLen)]

	return &protocol.Message{
		DeliveryTag:  offset,
		Body:         body,
		Exchange:     exchange,
		RoutingKey:   routingKey,
		DeliveryMode: deliveryMode,
	}, nil
}
