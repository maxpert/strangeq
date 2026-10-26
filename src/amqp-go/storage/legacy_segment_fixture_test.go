package storage

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
)

// writeLegacyBatchForTest creates pre-WI8 legacy fixtures without retaining a
// production segment append API. Tests that exercise legacy reads, recovery,
// acknowledgement, or compaction use this helper; new messages use the WAL.
func writeLegacyBatchForTest(sm *SegmentManager, queueName string, messages []*RecoveryMessage) error {
	if len(messages) == 0 {
		return nil
	}
	qs, err := sm.getOrCreateQueueSegments(queueName)
	if err != nil {
		return err
	}
	qs.mutex.Lock()
	defer qs.mutex.Unlock()
	seg := qs.currentSegment
	if seg == nil {
		return fmt.Errorf("legacy fixture has no active segment")
	}
	position := seg.fileSize.Load()
	for _, rm := range messages {
		record, err := serializeSegmentMessage(rm.Message, rm.Offset, qs.cfg.CRCDisabled)
		if err != nil {
			return err
		}
		n, err := seg.file.Write(record)
		if err != nil {
			return err
		}
		if n != len(record) {
			return fmt.Errorf("short legacy fixture write")
		}
		qs.currentIndex.mutex.Lock()
		qs.currentIndex.entries[rm.Offset] = position
		qs.currentIndex.mutex.Unlock()
		seg.index[rm.Offset] = position
		seg.inv.addRecord(&segmentRecord{
			pos:         recordPos{segmentID: seg.segmentNum, position: position},
			queueName:   queueName,
			deliveryTag: rm.Offset,
			bytes:       int64(len(record)),
			bodyRef:     -1,
			committed:   true,
		})
		seg.inv.durableBytes.Add(int64(len(record)))
		seg.fileSize.Add(int64(n))
		seg.messageCount.Add(1)
		if seg.minOffset == 0 || rm.Offset < seg.minOffset {
			seg.minOffset = rm.Offset
		}
		if rm.Offset > seg.maxOffset {
			seg.maxOffset = rm.Offset
		}
		position += int64(n)
	}
	if err := seg.file.Sync(); err != nil {
		return err
	}
	if seg.fileSize.Load() >= qs.cfg.SegmentSize {
		return qs.rollSegmentLocked()
	}
	return nil
}

func writeLegacyForTest(sm *SegmentManager, queueName string, message *protocol.Message, offset uint64) error {
	return writeLegacyBatchForTest(sm, queueName, []*RecoveryMessage{{
		QueueName: queueName, Message: message, Offset: offset,
	}})
}
