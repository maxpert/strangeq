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
	// WAL settings
	DefaultWALBatchSize    = 1000                  // Messages per batch
	DefaultWALBatchTimeout = 10 * time.Millisecond // Batch flush timeout
	DefaultWALFileSize     = 512 * 1024 * 1024     // 512 MB per WAL file
	WALFileExtension       = ".wal"

	// Shared WAL message format:
	// [4 bytes CRC32][4 bytes length][4 bytes queueNameLen][queueName][8 bytes offset]
	// [4 bytes exchangeLen][exchange][4 bytes routingKeyLen][routingKey][1 byte deliveryMode][4 bytes bodyLen][body]
	// Note: Header is variable-length (depends on queue name, exchange, and routing key lengths)
)

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
}

// QueueWAL manages a SHARED Write-Ahead Log with lock-free write path
// Note: Despite the name, this now serves ALL queues (naming kept for compatibility)
type QueueWAL struct {
	name    string // "shared" for the single shared WAL
	dataDir string

	// Lock-free write path
	writeChan chan *writeRequest // Buffered channel for lock-free writes
	ackChan   chan *ackRequest   // Buffered channel for ACKs (queue+offset)

	// ACK tracking
	ackBitmap      *roaring.Bitmap
	bitmapMutex    sync.RWMutex
	lastCheckpoint uint64 // Last offset checkpointed to segments

	// Current active file
	currentFile *os.File
	fileNum     atomic.Uint64
	fileOffset  atomic.Int64
	fileMutex   sync.Mutex

	// Old inactive files
	oldFiles      map[uint64]*walFileInfo
	oldFilesMutex sync.RWMutex

	// Offset index for O(1) lookups (Phase 6D: Fix consumer bottleneck)
	// Maps offset → (fileNum, filePosition) for fast reads
	offsetIndex      map[uint64]*offsetLocation
	offsetIndexMutex sync.RWMutex

	// Segment manager for checkpointing
	segmentMgr *SegmentManager

	// Metrics collector
	metrics WALMetrics

	// Background goroutines
	stopChan chan struct{}
	wg       sync.WaitGroup
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
	done      chan error // Caller blocks on this until batch is fsynced (Option 2: Batch Synchronous)
}

type ackRequest struct {
	queueName string
	offset    uint64
}

type walFileInfo struct {
	path      string
	minOffset uint64
	maxOffset uint64
	totalMsgs int
	ackedMsgs int
}

// NewWALManager creates a new WAL manager with shared WAL
func NewWALManager(dataDir string) (*WALManager, error) {
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wm := &WALManager{
		dataDir: walDir,
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
		wm.sharedWAL.segmentMgr = segmentMgr
	}
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

// Write writes a message to the shared WAL (synchronous batch write)
// Blocks until the batch containing this message is fsynced to disk.
// This ensures proper AMQP durability semantics (Option 2: Batch Synchronous).
func (wm *WALManager) Write(queueName string, message *protocol.Message, offset uint64) error {
	if wm.sharedWAL == nil {
		return fmt.Errorf("shared WAL not initialized")
	}

	// Create response channel for this write
	doneChan := make(chan error, 1)

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

// Acknowledge marks a message as ACKed (lock-free)
func (wm *WALManager) Acknowledge(queueName string, offset uint64) {
	if wm.sharedWAL == nil {
		return
	}

	// Send to ACK channel (non-blocking)
	select {
	case wm.sharedWAL.ackChan <- &ackRequest{
		queueName: queueName,
		offset:    offset,
	}:
	default:
		// ACK channel full, skip (will be processed on next cleanup)
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
			// Log error but continue with other files
			continue
		}

		// Filter out acknowledged messages
		for _, msg := range messages {
			wm.sharedWAL.bitmapMutex.RLock()
			isAcked := wm.sharedWAL.ackBitmap.Contains(uint32(msg.Offset))
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

	var messages []*RecoveryMessage

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

		// Deserialize entry
		pos := 0

		// Read queue name
		if pos+4 > len(data) {
			continue
		}
		queueLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+int(queueLen) > len(data) {
			continue
		}
		queueName := string(data[pos : pos+int(queueLen)])
		pos += int(queueLen)

		// Read offset
		if pos+8 > len(data) {
			continue
		}
		offset := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8

		// Read exchange
		if pos+4 > len(data) {
			continue
		}
		exchangeLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+int(exchangeLen) > len(data) {
			continue
		}
		exchange := string(data[pos : pos+int(exchangeLen)])
		pos += int(exchangeLen)

		// Read routing key
		if pos+4 > len(data) {
			continue
		}
		routingKeyLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+int(routingKeyLen) > len(data) {
			continue
		}
		routingKey := string(data[pos : pos+int(routingKeyLen)])
		pos += int(routingKeyLen)

		// Read delivery mode
		if pos+1 > len(data) {
			continue
		}
		deliveryMode := data[pos]
		pos += 1

		// Read body
		if pos+4 > len(data) {
			continue
		}
		bodyLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+int(bodyLen) > len(data) {
			continue
		}
		body := data[pos : pos+int(bodyLen)]

		// Create recovery message
		recoveryMsg := &RecoveryMessage{
			QueueName: queueName,
			Offset:    offset,
			Message: &protocol.Message{
				Exchange:     exchange,
				RoutingKey:   routingKey,
				DeliveryMode: deliveryMode,
				Body:         body,
				DeliveryTag:  offset,
				Redelivered:  true, // Mark as redelivered for recovery
			},
		}

		messages = append(messages, recoveryMsg)
	}

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
		name:        "shared",
		dataDir:     sharedDir,
		writeChan:   make(chan *writeRequest, 10000), // Large buffer for high throughput
		ackChan:     make(chan *ackRequest, 10000),   // Now holds queue+offset
		ackBitmap:   roaring.New(),
		oldFiles:    make(map[uint64]*walFileInfo),
		offsetIndex: make(map[uint64]*offsetLocation), // Phase 6D: Offset index for O(1) reads
		stopChan:    make(chan struct{}),
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

// batchWriterLoop batches writes and flushes periodically
func (qw *QueueWAL) batchWriterLoop() {
	defer qw.wg.Done()

	batch := make([]*writeRequest, 0, DefaultWALBatchSize)
	ticker := time.NewTicker(DefaultWALBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case req := <-qw.writeChan:
			batch = append(batch, req)
			if len(batch) >= DefaultWALBatchSize {
				qw.flushBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				qw.flushBatch(batch)
				batch = batch[:0]
			}

		case <-qw.stopChan:
			// Final flush
			if len(batch) > 0 {
				qw.flushBatch(batch)
			}
			return
		}
	}
}

// flushBatch writes a batch of messages to the current WAL file
func (qw *QueueWAL) flushBatch(batch []*writeRequest) {
	if len(batch) == 0 {
		return
	}

	qw.fileMutex.Lock()
	defer qw.fileMutex.Unlock()

	// Get current file number and position BEFORE write
	currentFileNum := qw.fileNum.Load()
	currentFilePos := qw.fileOffset.Load()

	// Track positions for index (Phase 6D)
	positions := make([]int64, len(batch))
	var buf []byte

	// Serialize all messages in batch and track their positions
	for i, req := range batch {
		positions[i] = currentFilePos + int64(len(buf)) // Position in file
		msgBytes := qw.serializeMessage(req.queueName, req.message, req.offset)
		buf = append(buf, msgBytes...)
	}

	// Single write (O_APPEND makes this atomic across processes)
	n, err := qw.currentFile.Write(buf)
	if err != nil {
		// Record WAL write error
		if qw.metrics != nil {
			qw.metrics.RecordWALWriteError()
		}
		// Write failed - signal all callers with error
		for _, req := range batch {
			if req.done != nil {
				req.done <- fmt.Errorf("WAL write failed: %w", err)
			}
		}
		return
	}

	// Record successful WAL writes
	if qw.metrics != nil {
		for range batch {
			qw.metrics.RecordWALWrite()
		}
	}

	// Fsync for durability (one sync per batch!)
	// This is the critical durability guarantee - messages are only durable after this completes
	fsyncStart := time.Now()
	fsyncErr := qw.currentFile.Sync()
	fsyncDuration := time.Since(fsyncStart).Seconds()

	// Record fsync metrics
	if qw.metrics != nil {
		qw.metrics.RecordWALFsync(fsyncDuration)
		if fsyncErr != nil {
			qw.metrics.RecordWALWriteError()
		}
	}

	// Signal ALL callers in this batch with the fsync result
	// This unblocks all Write() calls waiting for this batch
	for _, req := range batch {
		if req.done != nil {
			req.done <- fsyncErr
		}
	}

	// If fsync failed, don't update index (messages not durable)
	if fsyncErr != nil {
		return
	}

	// Update offset index (Phase 6D: O(1) reads!)
	qw.offsetIndexMutex.Lock()
	for i, req := range batch {
		qw.offsetIndex[req.offset] = &offsetLocation{
			fileNum:      currentFileNum,
			filePosition: positions[i],
		}
	}
	qw.offsetIndexMutex.Unlock()

	// Update offset
	newOffset := qw.fileOffset.Add(int64(n))

	// Check if we need to roll to new file
	if newOffset >= DefaultWALFileSize {
		qw.rollFile(batch[0].offset, batch[len(batch)-1].offset, len(batch))
	}
}

// serializeMessage serializes a message with CRC, length, queue name, and offset
// SHARED WAL Format: [CRC][length][queue_len][queue][offset][exchange_len][exchange][routing_key_len][routing_key][delivery_mode][body_len][body]
func (qw *QueueWAL) serializeMessage(queueName string, message *protocol.Message, offset uint64) []byte {
	buf := make([]byte, 0, len(message.Body)+len(message.Exchange)+len(message.RoutingKey)+len(queueName)+256)

	// Reserve space for CRC and length
	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder

	// Write queue name (NEW for shared WAL!)
	queueLen := make([]byte, 4)
	binary.BigEndian.PutUint32(queueLen, uint32(len(queueName)))
	buf = append(buf, queueLen...)
	buf = append(buf, []byte(queueName)...)

	// Write offset
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, offset)
	buf = append(buf, offsetBytes...)

	// Write exchange
	exchangeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(exchangeLen, uint32(len(message.Exchange)))
	buf = append(buf, exchangeLen...)
	buf = append(buf, []byte(message.Exchange)...)

	// Write routing key
	routingKeyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(routingKeyLen, uint32(len(message.RoutingKey)))
	buf = append(buf, routingKeyLen...)
	buf = append(buf, []byte(message.RoutingKey)...)

	// Write delivery mode (1 byte)
	buf = append(buf, message.DeliveryMode)

	// Write body
	bodyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLen, uint32(len(message.Body)))
	buf = append(buf, bodyLen...)
	buf = append(buf, message.Body...)

	// Calculate actual length (excluding CRC and length fields)
	dataLen := len(buf) - 8
	binary.BigEndian.PutUint32(buf[4:8], uint32(dataLen))

	// Calculate CRC over everything after CRC field
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf
}

// rollFile rolls to a new WAL file when size limit reached
func (qw *QueueWAL) rollFile(minOffset, maxOffset uint64, totalMsgs int) {
	// Get current file info
	currentFileNum := qw.fileNum.Load()
	currentPath := qw.currentFile.Name()

	// Close current file
	_ = qw.currentFile.Close()

	// Move to old files map
	qw.oldFilesMutex.Lock()
	qw.oldFiles[currentFileNum] = &walFileInfo{
		path:      currentPath,
		minOffset: minOffset,
		maxOffset: maxOffset,
		totalMsgs: totalMsgs,
		ackedMsgs: 0,
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

	qw.currentFile = file
	qw.fileOffset.Store(0)
	return nil
}

// cleanupLoop processes ACKs and deletes old WAL files
func (qw *QueueWAL) cleanupLoop() {
	defer qw.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case ack := <-qw.ackChan:
			qw.bitmapMutex.Lock()
			qw.ackBitmap.Add(uint32(ack.offset))
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

// tryDeleteOldFiles deletes WAL files where all messages are ACKed
func (qw *QueueWAL) tryDeleteOldFiles() {
	qw.oldFilesMutex.Lock()
	defer qw.oldFilesMutex.Unlock()

	qw.bitmapMutex.RLock()
	defer qw.bitmapMutex.RUnlock()

	for fileNum, info := range qw.oldFiles {
		// Check if all messages in this file are ACKed
		allAcked := true
		for offset := info.minOffset; offset <= info.maxOffset; offset++ {
			if !qw.ackBitmap.Contains(uint32(offset)) {
				allAcked = false
				break
			}
		}

		if allAcked {
			// Delete the WAL file
			_ = os.Remove(info.path)
			delete(qw.oldFiles, fileNum)
		}
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

// readMessageAtPosition reads a message at a specific file position (O(1))
func (qw *QueueWAL) readMessageAtPosition(queueName string, fileNum uint64, filePosition int64, expectedOffset uint64) (*protocol.Message, error) {
	// Get file path
	var filePath string
	currentFileNum := qw.fileNum.Load()

	if fileNum == currentFileNum {
		// Reading from current file
		qw.fileMutex.Lock()
		if qw.currentFile != nil {
			filePath = qw.currentFile.Name()
		}
		qw.fileMutex.Unlock()
	} else {
		// Reading from old file
		qw.oldFilesMutex.RLock()
		if info, ok := qw.oldFiles[fileNum]; ok {
			filePath = info.path
		}
		qw.oldFilesMutex.RUnlock()
	}

	if filePath == "" {
		return nil, fmt.Errorf("WAL file not found for fileNum %d", fileNum)
	}

	// Open file and seek to position
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(filePosition, 0)
	if err != nil {
		return nil, err
	}

	// Read CRC field (4 bytes)
	crcBytes := make([]byte, 4)
	_, err = file.Read(crcBytes)
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
	// This recreates buf[4:] from serialization
	crcCheckData := make([]byte, 4+dataLen)
	copy(crcCheckData[0:4], lengthBytes)
	copy(crcCheckData[4:], data)
	calculatedCRC := crc32.ChecksumIEEE(crcCheckData)

	if crc != calculatedCRC {
		return nil, fmt.Errorf("CRC mismatch for offset %d (expected %d, got %d)", expectedOffset, crc, calculatedCRC)
	}

	// Deserialize SHARED WAL format: [queue_len][queue][offset][exchange_len][exchange][routing_key_len][routing_key][body_len][body]
	pos := 0

	// Read queue name
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid WAL data: too short for queue length")
	}
	queueLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(queueLen) {
		return nil, fmt.Errorf("invalid WAL data: too short for queue name")
	}
	entryQueueName := string(data[pos : pos+int(queueLen)])
	pos += int(queueLen)

	// Validate queue name matches (for multi-queue isolation)
	if entryQueueName != queueName {
		return nil, fmt.Errorf("queue name mismatch: expected %s, got %s", queueName, entryQueueName)
	}

	// Read offset
	if len(data) < pos+8 {
		return nil, fmt.Errorf("invalid WAL data: too short for offset")
	}
	msgOffset := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	// Verify offset matches
	if msgOffset != expectedOffset {
		return nil, fmt.Errorf("offset mismatch: expected %d, got %d", expectedOffset, msgOffset)
	}

	// Read exchange
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid WAL data: too short for exchange length")
	}
	exchangeLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(exchangeLen) {
		return nil, fmt.Errorf("invalid WAL data: too short for exchange")
	}
	exchange := string(data[pos : pos+int(exchangeLen)])
	pos += int(exchangeLen)

	// Read routing key
	if len(data) < pos+4 {
		return nil, fmt.Errorf("invalid WAL data: too short for routing key length")
	}
	routingKeyLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(routingKeyLen) {
		return nil, fmt.Errorf("invalid WAL data: too short for routing key")
	}
	routingKey := string(data[pos : pos+int(routingKeyLen)])
	pos += int(routingKeyLen)

	// Read delivery mode
	if len(data) < pos+1 {
		return nil, fmt.Errorf("invalid WAL data: too short for delivery mode")
	}
	deliveryMode := data[pos]
	pos += 1

	// Read body
	if len(data) < pos+4 {
		return nil, fmt.Errorf("invalid WAL data: too short for body length")
	}
	bodyLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(bodyLen) {
		return nil, fmt.Errorf("invalid WAL data: too short for body")
	}
	body := data[pos : pos+int(bodyLen)]

	message := &protocol.Message{
		Body:         body,
		Exchange:     exchange,
		RoutingKey:   routingKey,
		DeliveryMode: deliveryMode,
		DeliveryTag:  msgOffset,
	}

	return message, nil
}

// readMessageSequential does sequential scan fallback (slow, only for unindexed messages)
func (qw *QueueWAL) readMessageSequential(queueName string, offset uint64) (*protocol.Message, error) {
	// Try to read from old files first
	qw.oldFilesMutex.RLock()
	defer qw.oldFilesMutex.RUnlock()

	for _, fileInfo := range qw.oldFiles {
		if offset >= fileInfo.minOffset && offset <= fileInfo.maxOffset {
			// Found the file containing this offset
			return qw.readMessageFromFile(queueName, fileInfo.path, offset)
		}
	}

	// Not in old files, try current file
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

		// Deserialize: [queue_len][queue][offset][exchange_len][exchange]...
		pos := 0

		// Read queue name
		if pos+4 > len(data) {
			return nil, fmt.Errorf("data too short for queue length")
		}
		queueLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		if pos+int(queueLen) > len(data) {
			return nil, fmt.Errorf("data too short for queue name")
		}
		entryQueueName := string(data[pos : pos+int(queueLen)])
		pos += int(queueLen)

		// Read offset
		if pos+8 > len(data) {
			return nil, fmt.Errorf("data too short for offset")
		}
		msgOffset := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8

		// Check if this is the message we're looking for
		if msgOffset == offset && entryQueueName == queueName {
			// Found it! Now deserialize the full message
			// Read exchange
			if pos+4 > len(data) {
				return nil, fmt.Errorf("data too short for exchange length")
			}
			exchangeLen := binary.BigEndian.Uint32(data[pos : pos+4])
			pos += 4

			if pos+int(exchangeLen) > len(data) {
				return nil, fmt.Errorf("data too short for exchange")
			}
			exchange := string(data[pos : pos+int(exchangeLen)])
			pos += int(exchangeLen)

			// Read routing key
			if pos+4 > len(data) {
				return nil, fmt.Errorf("data too short for routing key length")
			}
			routingKeyLen := binary.BigEndian.Uint32(data[pos : pos+4])
			pos += 4

			if pos+int(routingKeyLen) > len(data) {
				return nil, fmt.Errorf("data too short for routing key")
			}
			routingKey := string(data[pos : pos+int(routingKeyLen)])
			pos += int(routingKeyLen)

			// Read delivery mode
			if pos+1 > len(data) {
				return nil, fmt.Errorf("data too short for delivery mode")
			}
			deliveryMode := data[pos]
			pos += 1

			// Rest is body
			body := data[pos:]

			return &protocol.Message{
				Exchange:     exchange,
				RoutingKey:   routingKey,
				DeliveryMode: deliveryMode,
				Body:         body,
			}, nil
		}

		// Not the right message, continue scanning
	}
}

// checkpointLoop periodically checkpoints old WAL files to segments
func (qw *QueueWAL) checkpointLoop() {
	defer qw.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
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

// performCheckpoint moves messages from old WAL files to segments
// TODO(Phase 5): Reimplement checkpointing for shared WAL format
// The old per-queue checkpointing code has been removed because it used
// the old WAL format. Phase 5 will implement proper WAL compaction that:
// 1. Scans shared WAL files using the new format (with queue names)
// 2. Removes fully-ACKed messages
// 3. Compacts remaining messages into new WAL files
func (qw *QueueWAL) performCheckpoint() {
	// Disabled for now - will be reimplemented in Phase 5
	// Current implementation only deletes fully-ACKed WAL files via tryDeleteOldFiles()
	return
}

// close stops background goroutines and closes files
func (qw *QueueWAL) close() {
	close(qw.stopChan)
	qw.wg.Wait()

	qw.fileMutex.Lock()
	defer qw.fileMutex.Unlock()

	if qw.currentFile != nil {
		_ = qw.currentFile.Sync()
		_ = qw.currentFile.Close()
	}
}
