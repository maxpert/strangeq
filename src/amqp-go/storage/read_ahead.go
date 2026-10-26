package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	readAheadChunkSize  = 256 * 1024
	readAheadMaxEntries = 64
)

type readAheadBuffer struct {
	mu         sync.Mutex
	messages   map[uint64]*protocol.Message
	batchReads int
}

func newReadAheadBuffer() *readAheadBuffer {
	return &readAheadBuffer{
		messages: make(map[uint64]*protocol.Message),
	}
}

func (b *readAheadBuffer) get(tag uint64) (*protocol.Message, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	msg, ok := b.messages[tag]
	return msg, ok
}

// put takes OWNERSHIP of messages. Once handed over, the caller must not read or
// write that map again except through this type's methods — remove() mutates it
// in place on every acknowledgement, so an unsynchronised access by the caller is
// a data race.
func (b *readAheadBuffer) put(messages map[uint64]*protocol.Message) {
	b.mu.Lock()
	b.messages = messages
	b.batchReads++
	b.mu.Unlock()
}

// remove drops a single tag from the buffer.
//
// This is the cache's ack invalidation and it is load-bearing.
// DisruptorStorage.GetMessage consults this buffer BEFORE the WAL, and a
// readMessageBatch fills it with up to readAheadMaxEntries tags at once — so
// without this an acknowledged message keeps being served from here, bypassing
// the WAL's own ack gate entirely. broker/queue_reaper.go reapTTLSweep probes
// acked tags on every sweep and relies on getting nothing back; a cache hit
// there re-expires and re-dead-letters the same record without bound.
func (b *readAheadBuffer) remove(tag uint64) {
	b.mu.Lock()
	delete(b.messages, tag)
	b.mu.Unlock()
}

func (b *readAheadBuffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}

func (b *readAheadBuffer) has(tag uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.messages[tag]
	return ok
}

func (b *readAheadBuffer) batchReadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.batchReads
}

func (wm *WALManager) ReadBatch(queueName string, startOffset uint64) (map[uint64]*protocol.Message, error) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return nil, fmt.Errorf("shared WAL not initialized")
	}
	return sw.readMessageBatch(queueName, startOffset)
}

func (qw *QueueWAL) readMessageBatch(queueName string, startOffset uint64) (map[uint64]*protocol.Message, error) {
	key := recordKey{queueName: queueName, deliveryTag: startOffset}
	var messages map[uint64]*protocol.Message
	for {
		qw.offsetIndexMutex.RLock()
		location, ok := qw.offsetIndex[key]
		if !ok {
			qw.offsetIndexMutex.RUnlock()
			return nil, fmt.Errorf("offset %d not in WAL index", startOffset)
		}

		var limit int64
		collect := func(file *os.File) error {
			var cerr error
			messages, cerr = qw.collectBatchFrom(file, queueName, location.pos.position, limit)
			return cerr
		}

		if location.pos.segmentID == qw.fileNum.Load() {
			// Active reads never acquire fileMutex while holding the index lock.
			qw.offsetIndexMutex.RUnlock()
			qw.fileMutex.Lock()
			if location.pos.segmentID != qw.fileNum.Load() {
				qw.fileMutex.Unlock()
				continue
			}
			file := qw.currentReadFile
			limit = qw.fileOffset.Load()
			qw.fileMutex.Unlock()
			if file == nil {
				return nil, fmt.Errorf("WAL current read file not open for fileNum %d", location.pos.segmentID)
			}
			if err := collect(file); err != nil {
				return nil, err
			}
		} else {
			// Sealed reads retain the index read lock through ReadAt and body reads.
			qw.oldFilesMutex.RLock()
			info, registered := qw.oldFiles[location.pos.segmentID]
			limit = -1
			if registered {
				limit = info.durableSize
			}
			qw.oldFilesMutex.RUnlock()
			err := qw.withOldFileHandle(location.pos.segmentID, collect)
			qw.offsetIndexMutex.RUnlock()
			if err != nil {
				return nil, err
			}
		}
		break
	}

	// The queue-scoped volatile bitmap is a read gate only, and is touched with
	// no index/file lock held.
	for offset := range messages {
		if qw.isAcknowledged(queueName, offset) {
			delete(messages, offset)
		}
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages found in read-ahead for offset %d", startOffset)
	}
	return messages, nil
}

// collectBatchFrom reads one read-ahead chunk from file at filePosition,
// bounded at limit (-1 = no bound), and decodes every whole record in it
// belonging to queueName. It is split out of readMessageBatch so the retrying
// old-file arm and the current-file arm share one decoder, and so the retry
// has a single unit to re-run: everything that touches the file handle is
// inside here, and everything that does not — the acknowledgement filter, the
// empty check — is outside it.
//
// A short read at end of file is not an error: io.EOF simply bounds the chunk.
//
// WS-C C7:
//   - a stored CRC of 0 means CRC was disabled at write time — skip
//     verification, exactly the rule the four scanners apply
//     (wal_manager.go). The old unconditional check broke read-ahead on every
//     first record whenever crccheck was off.
//   - an all-zero record header is the start of a zero region, not a run of
//     zero-length records (walBlankRecordHeader): stop, like the scanners.
//   - limit is the durable end (fileOffset for the current file, durableSize
//     for a rolled one). Bytes past it are written but not fsynced, or belong
//     to a nacked batch; caching them would let GetMessage serve a message
//     whose publisher was never confirmed (or was nacked).
func (qw *QueueWAL) collectBatchFrom(file *os.File, queueName string, filePosition, limit int64) (map[uint64]*protocol.Message, error) {
	want := int64(readAheadChunkSize)
	if limit >= 0 {
		remaining := limit - filePosition
		if remaining <= 0 {
			return map[uint64]*protocol.Message{}, nil
		}
		if remaining < want {
			want = remaining
		}
	}
	chunk := make([]byte, want)
	n, err := walReadAt(file, chunk, filePosition)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	chunk = chunk[:n]

	messages := make(map[uint64]*protocol.Message)
	pos := 0
	for pos < len(chunk) {
		if pos+8 > len(chunk) {
			break
		}
		crc := binary.BigEndian.Uint32(chunk[pos : pos+4])
		dataLen := binary.BigEndian.Uint32(chunk[pos+4 : pos+8])

		// A zero region starting on a record boundary is the end of the data
		// (the write path cannot produce a zero-length record).
		if walBlankRecordHeader(crc, dataLen) {
			break
		}

		recordEnd := pos + 8 + int(dataLen)
		if recordEnd > len(chunk) {
			break
		}

		// A stored CRC of 0 means CRC was disabled at write time — skip
		// verification (the scanner's rule).
		if crc != 0 && crc != crc32.ChecksumIEEE(chunk[pos+4:recordEnd]) {
			break
		}

		data := chunk[pos+8 : recordEnd]
		recType, payload := recordTypeAndPayload(data)
		if recType == WALRecordTypeMessage {
			rm, ok := deserializeMessagePayload(payload)
			if ok && rm.QueueName == queueName {
				if len(rm.Message.BodyRef) == 8 {
					blockOffset := int64(binary.BigEndian.Uint64(rm.Message.BodyRef))
					body, berr := readBodyBlockAt(file, blockOffset)
					if berr != nil {
						// A body block that cannot be resolved because the
						// handle was closed under us is not a corrupt record —
						// it is the retryable case, and it must reach
						// withOldFileHandle rather than being skipped here.
						if errors.Is(berr, os.ErrClosed) {
							return nil, berr
						}
						pos = recordEnd
						continue
					}
					rm.Message.Body = body
					rm.Message.BodyRef = nil
				}
				rm.Message.Redelivered = false
				messages[rm.Offset] = rm.Message
				if len(messages) >= readAheadMaxEntries {
					break
				}
			}
		}

		pos = recordEnd
	}

	return messages, nil
}
