package storage

// TEST-ONLY READ-AHEAD INTERNALS.
//
// WI3 retired the delivery use of the read-ahead buffer and the per-queue
// readAheadBuffer: the hot cache replaced it, and the queue's hot cache is
// where a published message lives now. The machinery that fed it stayed
// behind in read_ahead.go with NO production caller:
//
//	readAheadBuffer            -> no reference anywhere in the module
//	WALManager.ReadBatch        -> no production caller
//	QueueWAL.readMessageBatch   -> reachable only through WALManager.ReadBatch
//	QueueWAL.collectBatchFrom   -> reachable only through readMessageBatch
//
// The production file was DELETED (that is the point of this move): the hot
// cache is the only read cache the broker has, and shipping a second,
// unreferenced read path in the production binary is exactly the orphaned
// machinery the review flagged.
//
// The three functions below survive ONLY because four storage test files
// still drive them as INSTRUMENTS:
//
//	wal_ack_read_branch_test.go      -> WALManager.ReadBatch  (probes whether
//	                                     an offsetIndex entry exists)
//	wal_ack_read_deadlock_test.go     -> WALManager.ReadBatch  (a concurrent
//	                                     reader storm)
//	wal_old_file_handle_test.go       -> QueueWAL.readMessageBatch (recovery
//	                                     from a closed-under-us old file handle)
//	read_ahead_fault_test.go          -> QueueWAL.collectBatchFrom (the durable
//	                                     end bound and the blank-header stop)
//
// Those test files belong to other workstreams, so their code is preserved
// byte-for-byte HERE — as test-only code — instead of being edited or
// deleted from under them. A method may be declared in a _test.go file, so
// these three keep their exact signatures and every existing test keeps
// compiling and running unchanged.
//
// When the owners of those four test files retire their use of the read-ahead
// probe, this file and the four call sites can be deleted together.
//
// The readAheadChunkSize / readAheadMaxEntries constants live here for the
// same reason: they were only ever used by the code below.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	readAheadChunkSize  = 256 * 1024
	readAheadMaxEntries = 64
)

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
