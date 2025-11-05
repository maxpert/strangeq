package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
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
)

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
}

// QueueSegments manages segments for a single queue
type QueueSegments struct {
	queueName string
	dataDir   string

	// Active segment being written to
	currentSegment *SegmentFile
	currentIndex   *SegmentIndex
	mutex          sync.Mutex

	// Sealed segments (read-only)
	sealedSegments map[uint64]*SegmentFile
	sealedMutex    sync.RWMutex

	// ACK tracking for compaction
	ackBitmap   *roaring.Bitmap
	bitmapMutex sync.RWMutex

	// Compaction state
	lastCompaction time.Time
	compactionMux  sync.Mutex

	// Checkpoint state
	lastCheckpoint       time.Time
	lastCheckpointOffset uint64

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

// NewSegmentManager creates a new segment manager
func NewSegmentManager(dataDir string) (*SegmentManager, error) {
	segDir := filepath.Join(dataDir, "segments")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segments directory: %w", err)
	}

	return &SegmentManager{
		dataDir: segDir,
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

// Acknowledge marks a message as ACKed for future compaction
func (sm *SegmentManager) Acknowledge(queueName string, offset uint64) {
	val, ok := sm.queueSegments.Load(queueName)
	if !ok {
		return
	}
	segments := val.(*QueueSegments)

	segments.bitmapMutex.Lock()
	segments.ackBitmap.Add(uint32(offset))
	segments.bitmapMutex.Unlock()
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

	segments := &QueueSegments{
		queueName:      queueName,
		dataDir:        queueDir,
		sealedSegments: make(map[uint64]*SegmentFile),
		ackBitmap:      roaring.New(),
		stopChan:       make(chan struct{}),
		metrics:        sm.metrics,
	}

	// Create first segment
	if err := segments.openNextSegment(); err != nil {
		return nil
	}

	// Start background goroutines
	segments.wg.Add(2)
	go segments.checkpointLoop()
	go segments.compactionLoop()

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

	// Check if we need to roll to new segment
	if qs.currentSegment.fileSize.Load() >= DefaultSegmentSize {
		qs.sealSegment()
		_ = qs.openNextSegment()
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

// sealSegment moves current segment to sealed segments
func (qs *QueueSegments) sealSegment() {
	if qs.currentSegment == nil {
		return
	}

	// Sync to disk
	_ = qs.currentSegment.file.Sync()

	// Move to sealed segments
	qs.sealedMutex.Lock()
	qs.sealedSegments[qs.currentSegment.segmentNum] = qs.currentSegment
	qs.sealedMutex.Unlock()

	// Write index to disk
	qs.writeIndexToDisk(qs.currentSegment.segmentNum, qs.currentIndex)
}

// openNextSegment creates and opens the next segment file
func (qs *QueueSegments) openNextSegment() error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	segmentNum := uint64(time.Now().UnixNano())
	filename := filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", segmentNum, SegmentFileExtension))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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

// checkpointLoop periodically checkpoints WAL to segments
func (qs *QueueSegments) checkpointLoop() {
	defer qs.wg.Done()

	ticker := time.NewTicker(SegmentCheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Phase 6C will implement actual checkpointing from WAL
			// For now, this is a placeholder
			qs.lastCheckpoint = time.Now()

		case <-qs.stopChan:
			return
		}
	}
}

// compactionLoop periodically checks if compaction is needed
func (qs *QueueSegments) compactionLoop() {
	defer qs.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
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
			if deletionRatio > DefaultCompactionThreshold {
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
		if !qs.ackBitmap.Contains(uint32(offset)) {
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
	binary.Write(file, binary.BigEndian, numEntries)

	// Write each entry: [8 bytes offset][8 bytes position]
	for offset, position := range index.entries {
		binary.Write(file, binary.BigEndian, offset)
		binary.Write(file, binary.BigEndian, uint64(position))
	}

	return file.Sync()
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

func serializeSegmentMessage(message *protocol.Message, offset uint64) []byte {
	bodySize := len(message.Body)
	totalSize := SegmentHeaderSize + bodySize + 256 // Extra for metadata

	buf := make([]byte, 0, totalSize)

	// Reserve space for CRC and length
	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder

	// Write offset
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, offset)
	buf = append(buf, offsetBytes...)

	// Write message body
	buf = append(buf, message.Body...)

	// Calculate length
	dataLen := len(buf) - 8
	binary.BigEndian.PutUint32(buf[4:8], uint32(dataLen))

	// Calculate CRC
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf
}

func readSegmentMessageAt(file *os.File, position int64) (*protocol.Message, error) {
	// Read header
	header := make([]byte, SegmentHeaderSize)
	_, err := file.ReadAt(header, position)
	if err != nil {
		return nil, err
	}

	// Parse header
	crc := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])
	offset := binary.BigEndian.Uint64(header[8:16])

	// Read message data
	data := make([]byte, dataLen)
	_, err = file.ReadAt(data, position+8) // +8 to skip CRC and length
	if err != nil {
		return nil, err
	}

	// Verify CRC
	calculatedCRC := crc32.ChecksumIEEE(data)
	if calculatedCRC != crc {
		return nil, fmt.Errorf("CRC mismatch")
	}

	// Parse message (simplified - skip offset field in data)
	body := data[8:] // Skip offset field

	return &protocol.Message{
		DeliveryTag: offset,
		Body:        body,
	}, nil
}
