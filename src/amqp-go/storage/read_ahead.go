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

func (b *readAheadBuffer) put(messages map[uint64]*protocol.Message) {
	b.mu.Lock()
	b.messages = messages
	b.batchReads++
	b.mu.Unlock()
}

func (b *readAheadBuffer) clear() {
	b.mu.Lock()
	b.messages = make(map[uint64]*protocol.Message)
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
	qw.offsetIndexMutex.RLock()
	location, ok := qw.offsetIndex[startOffset]
	qw.offsetIndexMutex.RUnlock()
	if !ok {
		return nil, fmt.Errorf("offset %d not in WAL index", startOffset)
	}

	var file *os.File
	qw.fileMutex.Lock()
	currentFileNum := qw.fileNum.Load()
	if location.fileNum == currentFileNum {
		file = qw.currentReadFile
		qw.fileMutex.Unlock()
		if file == nil {
			return nil, fmt.Errorf("WAL current read file not open for fileNum %d", location.fileNum)
		}
	} else {
		qw.fileMutex.Unlock()
		var unlock func()
		var cacheErr error
		file, unlock, cacheErr = qw.getOldFileHandle(location.fileNum)
		if cacheErr != nil {
			return nil, cacheErr
		}
		defer unlock()
	}

	chunk := make([]byte, readAheadChunkSize)
	n, err := file.ReadAt(chunk, location.filePosition)
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

		recordEnd := pos + 8 + int(dataLen)
		if recordEnd > len(chunk) {
			break
		}

		if crc != crc32.ChecksumIEEE(chunk[pos+4:recordEnd]) {
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

	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages found in read-ahead for offset %d", startOffset)
	}

	return messages, nil
}
