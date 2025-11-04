package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	OffsetCheckpointDir       = "offsets"
	DefaultCheckpointInterval = 5 * time.Second
	OffsetFileExtension       = ".json"
)

// ConsumerOffset tracks the last acknowledged delivery tag for a consumer
type ConsumerOffset struct {
	QueueName   string    `json:"queue_name"`
	ConsumerTag string    `json:"consumer_tag"`
	LastAckTag  uint64    `json:"last_ack_tag"`
	LastUpdated time.Time `json:"last_updated"`
}

// OffsetCheckpointStore manages periodic checkpointing of consumer offsets
type OffsetCheckpointStore struct {
	baseDir string
	mutex   sync.RWMutex

	// In-memory tracking of consumer offsets
	offsets map[string]map[string]*ConsumerOffset // queueName -> consumerTag -> offset

	// Background checkpointing
	ticker   *time.Ticker
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewOffsetCheckpointStore creates a new offset checkpoint store with default interval
func NewOffsetCheckpointStore(dataDir string) (*OffsetCheckpointStore, error) {
	return NewOffsetCheckpointStoreWithInterval(dataDir, DefaultCheckpointInterval)
}

// GetLastOffset returns the last checkpointed offset for a consumer
// Used for crash recovery
func (ocs *OffsetCheckpointStore) GetLastOffset(queueName, consumerTag string) (uint64, error) {
	ocs.mutex.RLock()
	defer ocs.mutex.RUnlock()

	queueOffsets, ok := ocs.offsets[queueName]
	if !ok {
		return 0, nil // No checkpoint exists
	}

	consumerOffset, ok := queueOffsets[consumerTag]
	if !ok {
		return 0, nil // No checkpoint for this consumer
	}

	return consumerOffset.LastAckTag, nil
}

// NewOffsetCheckpointStoreWithInterval creates a new offset checkpoint store with custom interval
// Set interval to 0 to disable background checkpointing (manual checkpoint only)
func NewOffsetCheckpointStoreWithInterval(dataDir string, interval time.Duration) (*OffsetCheckpointStore, error) {
	baseDir := filepath.Join(dataDir, OffsetCheckpointDir)

	// Create directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create offset directory: %w", err)
	}

	store := &OffsetCheckpointStore{
		baseDir:  baseDir,
		offsets:  make(map[string]map[string]*ConsumerOffset),
		stopChan: make(chan struct{}),
	}

	// Start background checkpointing only if interval > 0
	if interval > 0 {
		store.ticker = time.NewTicker(interval)
		store.wg.Add(1)
		go store.backgroundCheckpointer()
	}

	return store, nil
}

// UpdateOffset updates a consumer's offset in memory (zero disk writes)
func (ocs *OffsetCheckpointStore) UpdateOffset(queueName, consumerTag string, deliveryTag uint64) {
	ocs.mutex.Lock()
	defer ocs.mutex.Unlock()

	if ocs.offsets[queueName] == nil {
		ocs.offsets[queueName] = make(map[string]*ConsumerOffset)
	}

	if offset, exists := ocs.offsets[queueName][consumerTag]; exists {
		// Update existing offset (only if newer)
		if deliveryTag > offset.LastAckTag {
			offset.LastAckTag = deliveryTag
			offset.LastUpdated = time.Now()
		}
	} else {
		// Create new offset
		ocs.offsets[queueName][consumerTag] = &ConsumerOffset{
			QueueName:   queueName,
			ConsumerTag: consumerTag,
			LastAckTag:  deliveryTag,
			LastUpdated: time.Now(),
		}
	}
}

// GetOffset returns the last acknowledged delivery tag for a consumer
func (ocs *OffsetCheckpointStore) GetOffset(queueName, consumerTag string) (uint64, bool) {
	ocs.mutex.RLock()
	defer ocs.mutex.RUnlock()

	if queueOffsets, exists := ocs.offsets[queueName]; exists {
		if offset, exists := queueOffsets[consumerTag]; exists {
			return offset.LastAckTag, true
		}
	}

	return 0, false
}

// GetQueueOffsets returns all consumer offsets for a queue
func (ocs *OffsetCheckpointStore) GetQueueOffsets(queueName string) map[string]uint64 {
	ocs.mutex.RLock()
	defer ocs.mutex.RUnlock()

	result := make(map[string]uint64)
	if queueOffsets, exists := ocs.offsets[queueName]; exists {
		for consumerTag, offset := range queueOffsets {
			result[consumerTag] = offset.LastAckTag
		}
	}

	return result
}

// RemoveConsumer removes a consumer's offset tracking
func (ocs *OffsetCheckpointStore) RemoveConsumer(queueName, consumerTag string) error {
	ocs.mutex.Lock()
	defer ocs.mutex.Unlock()

	if queueOffsets, exists := ocs.offsets[queueName]; exists {
		delete(queueOffsets, consumerTag)
	}

	// Delete checkpoint file
	filename := makeOffsetFilename(queueName, consumerTag)
	path := filepath.Join(ocs.baseDir, filename)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete offset file: %w", err)
	}

	return nil
}

// backgroundCheckpointer periodically writes offsets to disk
func (ocs *OffsetCheckpointStore) backgroundCheckpointer() {
	defer ocs.wg.Done()

	for {
		select {
		case <-ocs.ticker.C:
			_ = ocs.CheckpointAll()
		case <-ocs.stopChan:
			// Final checkpoint before exit
			_ = ocs.CheckpointAll()
			return
		}
	}
}

// CheckpointAll writes all in-memory offsets to disk
func (ocs *OffsetCheckpointStore) CheckpointAll() error {
	ocs.mutex.RLock()
	defer ocs.mutex.RUnlock()

	for queueName, queueOffsets := range ocs.offsets {
		for consumerTag, offset := range queueOffsets {
			if err := ocs.checkpointOffsetUnlocked(queueName, consumerTag, offset); err != nil {
				// Log error but continue with other offsets
				// In production, we'd want better error handling
				continue
			}
		}
	}

	return nil
}

// checkpointOffsetUnlocked writes a single offset to disk (must hold read lock)
func (ocs *OffsetCheckpointStore) checkpointOffsetUnlocked(queueName, consumerTag string, offset *ConsumerOffset) error {
	data, err := json.MarshalIndent(offset, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal offset: %w", err)
	}

	filename := makeOffsetFilename(queueName, consumerTag)
	path := filepath.Join(ocs.baseDir, filename)

	// Atomic write via temp file
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// LoadAllOffsets loads all offset checkpoints from disk (for recovery)
func (ocs *OffsetCheckpointStore) LoadAllOffsets() error {
	files, err := os.ReadDir(ocs.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No checkpoints yet
		}
		return fmt.Errorf("failed to read offset directory: %w", err)
	}

	ocs.mutex.Lock()
	defer ocs.mutex.Unlock()

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != OffsetFileExtension {
			continue
		}

		path := filepath.Join(ocs.baseDir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip unreadable files
		}

		var offset ConsumerOffset
		if err := json.Unmarshal(data, &offset); err != nil {
			continue // Skip corrupted files
		}

		// Load into memory
		if ocs.offsets[offset.QueueName] == nil {
			ocs.offsets[offset.QueueName] = make(map[string]*ConsumerOffset)
		}
		ocs.offsets[offset.QueueName][offset.ConsumerTag] = &offset
	}

	return nil
}

// makeOffsetFilename creates a unique filename for a consumer offset
func makeOffsetFilename(queueName, consumerTag string) string {
	// Sanitize for filesystem
	safe := func(s string) string {
		s = filepath.Base(s) // Remove path separators
		return s
	}

	return fmt.Sprintf("%s_%s%s",
		safe(queueName),
		safe(consumerTag),
		OffsetFileExtension)
}

// Close stops the background checkpointer and performs final checkpoint
func (ocs *OffsetCheckpointStore) Close() error {
	close(ocs.stopChan)
	if ocs.ticker != nil {
		ocs.ticker.Stop()
	}
	ocs.wg.Wait()
	return nil
}

// GetAllOffsets returns all offsets for debugging/monitoring
func (ocs *OffsetCheckpointStore) GetAllOffsets() map[string]map[string]*ConsumerOffset {
	ocs.mutex.RLock()
	defer ocs.mutex.RUnlock()

	// Return a copy
	result := make(map[string]map[string]*ConsumerOffset)
	for queueName, queueOffsets := range ocs.offsets {
		result[queueName] = make(map[string]*ConsumerOffset)
		for consumerTag, offset := range queueOffsets {
			offsetCopy := *offset
			result[queueName][consumerTag] = &offsetCopy
		}
	}

	return result
}
