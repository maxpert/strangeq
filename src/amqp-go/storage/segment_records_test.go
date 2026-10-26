package storage

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func inventoryTestWALConfig() WALConfig {
	cfg := DefaultWALConfig()
	cfg.SyncDisabled = true
	cfg.FileSize = 1 << 20
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	return cfg
}

func TestSegmentInventoryWriteAndRoll(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), inventoryTestWALConfig())
	require.NoError(t, err)
	defer wm.Close()
	const queue = "inventory-write"
	msg := &protocol.Message{Body: []byte("record-one"), DeliveryMode: 2}
	encoded, err := wm.sharedWAL.serializeMessageVersioned(queue, msg, 11)
	require.NoError(t, err)
	require.NoError(t, wm.Write(queue, msg, 11))
	qw := wm.sharedWAL
	oldNum := qw.fileNum.Load()
	inv := qw.currentInv
	require.NotNil(t, inv)
	require.True(t, inv.valid)
	require.Equal(t, int64(WALHeaderSize+len(encoded)), inv.durableBytes)
	require.Equal(t, int64(len(encoded)), inv.unacked.Load())
	require.Zero(t, inv.deadBytes.Load())
	r := inv.records[int64(WALHeaderSize)]
	require.NotNil(t, r)
	require.Equal(t, recordPos{segmentID: oldNum, position: int64(WALHeaderSize)}, r.pos)
	require.Equal(t, queue, r.queueName)
	require.Equal(t, uint64(11), r.deliveryTag)
	require.Equal(t, int64(len(encoded)), r.bytes)
	require.Equal(t, int64(-1), r.bodyRef)
	require.True(t, r.committed)
	require.Same(t, r, qw.offsetIndex[recordKey{queueName: queue, deliveryTag: 11}])
	qw.fileMutex.Lock()
	require.NoError(t, qw.rollFile())
	qw.fileMutex.Unlock()
	require.Empty(t, qw.currentInv.records)
	require.Equal(t, int64(WALHeaderSize), qw.currentInv.durableBytes)
	sealed := qw.oldFiles[oldNum]
	require.NotNil(t, sealed)
	require.Same(t, inv, sealed.inv)
	require.Same(t, r, inv.records[int64(WALHeaderSize)])
}

func TestSegmentInventoryBootCommittedOnly(t *testing.T) {
	dir := t.TempDir()
	cfg := inventoryTestWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	const queue = "inventory-boot"
	committed := &RecoveryMessage{QueueName: queue, Offset: 21, Message: &protocol.Message{Body: []byte("committed"), DeliveryMode: 2}}
	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{committed}))
	qw := wm.sharedWAL
	fileNum := qw.fileNum.Load()
	committedBytes := qw.fileOffset.Load()
	qw.fileMutex.Lock()
	epoch := qw.txEpoch.Add(1)
	torn := serializeTxMarker(WALTxMarkerBegin, epoch, cfg.CRCDisabled)
	uncommittedBytes, err := qw.serializeMessageVersioned(queue, &protocol.Message{Body: []byte("uncommitted"), DeliveryMode: 2}, 22)
	require.NoError(t, err)
	torn = append(torn, uncommittedBytes...)
	_, err = qw.currentFile.Write(torn)
	require.NoError(t, err)
	qw.fileMutex.Unlock()
	require.NoError(t, wm.Close())
	reopened, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer reopened.Close()
	inv := reopened.sharedWAL.oldFiles[fileNum].inv
	require.NotNil(t, inv)
	require.True(t, inv.valid)
	require.Len(t, inv.records, 1)
	require.Equal(t, committedBytes+int64(len(torn)), inv.durableBytes)
	require.Equal(t, int64(len(torn)), inv.deadBytes.Load())
	var live *segmentRecord
	for _, r := range inv.records {
		live = r
	}
	require.Equal(t, uint64(21), live.deliveryTag)
	require.Equal(t, live.bytes, inv.unacked.Load())
	_, ok := reopened.sharedWAL.offsetIndex[recordKey{queueName: queue, deliveryTag: 22}]
	require.False(t, ok)
}

func TestSegmentInventoryBodyRefcounts(t *testing.T) {
	dir := t.TempDir()
	cfg := inventoryTestWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	body := []byte("shared-body")
	done1, done2 := make(chan error, 1), make(chan error, 1)
	subs := []sharedSub{
		{queueName: "inventory-body-a", offset: 31, message: &protocol.Message{DeliveryMode: 2}, done: done1},
		{queueName: "inventory-body-b", offset: 32, message: &protocol.Message{DeliveryMode: 2}, done: done2},
	}
	require.NoError(t, wm.WriteSharedAsync(subs, body, 2))
	require.NoError(t, <-done1)
	require.NoError(t, <-done2)
	fileNum := wm.sharedWAL.fileNum.Load()
	require.NoError(t, wm.Close())
	reopened, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer reopened.Close()
	inv := reopened.sharedWAL.oldFiles[fileNum].inv
	require.NotNil(t, inv)
	require.Len(t, inv.records, 2)
	require.Len(t, inv.blocks, 1)
	var blockPos int64
	for pos, block := range inv.blocks {
		blockPos = pos
		require.Equal(t, int64(2), block.refs.Load())
	}
	var refs []*segmentRecord
	for _, r := range inv.records {
		require.Equal(t, blockPos, r.bodyRef)
		refs = append(refs, r)
	}
	blockBytes := inv.blocks[blockPos].bytes
	beforeDead := inv.deadBytes.Load()
	require.True(t, refs[0].markAck())
	require.Equal(t, beforeDead+refs[0].bytes, inv.deadBytes.Load())
	require.Equal(t, int64(1), inv.blocks[blockPos].refs.Load())
	require.True(t, refs[1].markAck())
	require.Equal(t, beforeDead+refs[0].bytes+refs[1].bytes+blockBytes, inv.deadBytes.Load())
	require.Zero(t, inv.blocks[blockPos].refs.Load())
	require.Zero(t, inv.unacked.Load())
	require.False(t, refs[1].markAck())
}

func TestSegmentInventoryLegacyLoad(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	const queue = "inventory-legacy"
	msg := &protocol.Message{Body: []byte("legacy"), DeliveryMode: 2}
	encoded, err := serializeSegmentMessage(msg, 41, false)
	require.NoError(t, err)
	require.NoError(t, writeLegacyForTest(sm, queue, msg, 41))
	val, ok := sm.queueSegments.Load(queue)
	require.True(t, ok)
	qs := val.(*QueueSegments)
	segPath := qs.currentSegment.path
	segNum := qs.currentSegment.segmentNum
	require.NoError(t, sm.Close())
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	torn := []byte{0, 0, 0}
	_, err = f.Write(torn)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	reopened, err := NewSegmentManager(dir)
	require.NoError(t, err)
	defer reopened.Close()
	_, err = reopened.RecoverFromSegments()
	require.NoError(t, err)
	val, ok = reopened.queueSegments.Load(queue)
	require.True(t, ok)
	loaded := val.(*QueueSegments).sealedSegments[segNum]
	require.NotNil(t, loaded)
	inv := loaded.inv
	require.NotNil(t, inv)
	require.True(t, inv.valid)
	require.Len(t, inv.records, 1)
	r := inv.records[0]
	require.NotNil(t, r)
	require.Equal(t, recordPos{segmentID: segNum, position: 0}, r.pos)
	require.Equal(t, queue, r.queueName)
	require.Equal(t, uint64(41), r.deliveryTag)
	require.Equal(t, int64(len(encoded)), r.bytes)
	require.Equal(t, int64(-1), r.bodyRef)
	require.True(t, r.committed)
	require.Equal(t, int64(len(encoded)+len(torn)), inv.durableBytes)
	require.Equal(t, int64(len(encoded)), inv.unacked.Load())
	require.Equal(t, int64(len(torn)), inv.deadBytes.Load())
	require.Equal(t, int64(8+binary.BigEndian.Uint32(encoded[4:8])), r.bytes)
}
