package broker

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

type deadLetterFaultStorage struct {
	*storage.StorageEngine
	failTarget atomic.Bool
	failures   atomic.Int64
	reads      atomic.Int64
}

func (s *deadLetterFaultStorage) StoreMessage(queue string, msg *protocol.Message) error {
	if queue == "fault-target" && s.failTarget.Load() {
		s.failures.Add(1)
		return errors.New("injected DLQ write failure")
	}
	return s.StorageEngine.StoreMessage(queue, msg)
}

func (s *deadLetterFaultStorage) ReadDeadLetter(id string) (*storage.PendingDeadLetter, error) {
	entry, err := s.StorageEngine.ReadDeadLetter(id)
	if err == nil && entry.Targets != nil {
		s.reads.Add(1)
	}
	return entry, err
}

func newDeadLetterFaultBroker(t *testing.T) (*StorageBroker, *deadLetterFaultStorage) {
	t.Helper()
	real, err := storage.NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, real.Close()) })
	fault := &deadLetterFaultStorage{StorageEngine: real}
	b := NewStorageBroker(fault, interfaces.EngineConfig{MaxQueueBacklog: interfaces.DefaultMaxQueueBacklog})
	t.Cleanup(b.Close)
	require.NoError(t, b.InitError())
	b.StartDeadLetterWorker()
	declareReliableSource(t, b)
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "fault-target", nil)

	fault.failTarget.Store(true)
	rejectReliableSource(t, b)
	require.Eventually(t, func() bool { return fault.failures.Load() > 0 }, 3*time.Second, 10*time.Millisecond,
		"the destination write failure must actually have occurred")
	require.Zero(t, b.GetQueueReadyCount("fault-target"))
	return b, fault
}

func TestReliableDeadLetter_RetriesDestinationWriteFailure(t *testing.T) {
	b, fault := newDeadLetterFaultBroker(t)

	fault.failTarget.Store(false)
	awaitReliableDelivery(t, b, "fault-target")
}

func TestReliableDeadLetter_RequiresCurrentBindingAfterTargetRecreation(t *testing.T) {
	b, fault := newDeadLetterFaultBroker(t)
	_, err := b.DeleteQueue("fault-target", false, false)
	require.NoError(t, err)
	_, err = b.DeclareQueue("fault-target", true, false, false, nil)
	require.NoError(t, err)
	reads := fault.reads.Load()
	fault.failTarget.Store(false)

	// Two more reads prove a retry completed while the successor was unbound.
	// Allow the observed bad result to reach the exact assertion below.
	require.Eventually(t, func() bool {
		return fault.reads.Load() >= reads+2 || b.GetQueueReadyCount("fault-target") > 0
	}, 3*time.Second, 10*time.Millisecond)
	require.Zero(t, b.GetQueueReadyCount("fault-target"), "a pending transfer must not go to an unbound successor")

	require.NoError(t, b.BindQueue("fault-target", "reliable-dlx", "", nil))
	awaitReliableDelivery(t, b, "fault-target")
}

func TestReliableDeadLetter_RetriesGetExpiryWithCreditBlockedConsumer(t *testing.T) {
	s, err := storage.NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	fault := &stagingFaultStorage{StorageEngine: s}
	b := NewStorageBroker(fault, interfaces.EngineConfig{MaxQueueBacklog: interfaces.DefaultMaxQueueBacklog})
	t.Cleanup(b.Close)
	b.StartDeadLetterWorker()
	declareReliableSource(t, b)
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "expiry-target", nil)
	consumer := registerManualConsumer(t, b, "reliable-source", "credit-blocked", 1, 1)
	publishToSource(t, b, "reliable-source", "unacked")
	require.Equal(t, "unacked", string(recvDeliveries(t, consumer, 1)[0].Message.Body))
	require.NoError(t, b.PublishMessage("", "reliable-source", &protocol.Message{
		Body: []byte("expired payload"), Expiration: "0",
	}))
	fault.failStage.Store(true)
	msg, _, _, err := b.GetMessageForGet("reliable-source", true)
	require.ErrorContains(t, err, "injected outbox commit failure")
	require.Nil(t, msg)

	fault.failStage.Store(false)
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("expiry-target") > 0 }, 3*time.Second, 10*time.Millisecond)
	msg = drainOne(t, b, "expiry-target")
	require.NotNil(t, msg)
	require.Equal(t, "expired payload", string(msg.Body))
	assertEntry(t, xDeathEntries(t, msg.Headers)[0], "reliable-source", DeadLetterExpired, 1)
}

var errDeadLetterSourceRead = errors.New("injected source read failure")

type deadLetterSourceReadFault struct {
	*storage.StorageEngine
	failRead atomic.Bool
}

func (s *deadLetterSourceReadFault) GetMessageForDelivery(queue string, tag uint64) (*protocol.Message, error) {
	if queue == "reliable-source" && s.failRead.Load() {
		return nil, errDeadLetterSourceRead
	}
	return s.StorageEngine.GetMessageForDelivery(queue, tag)
}

func (s *deadLetterSourceReadFault) GetMessage(queue string, tag uint64) (*protocol.Message, error) {
	if queue == "reliable-source" && s.failRead.Load() {
		return nil, errDeadLetterSourceRead
	}
	return s.StorageEngine.GetMessage(queue, tag)
}

func TestReliableDeadLetter_RetriesInitialGetReadFailure(t *testing.T) {
	s, err := storage.NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	fault := &deadLetterSourceReadFault{StorageEngine: s}
	b := NewStorageBroker(fault, interfaces.EngineConfig{MaxQueueBacklog: interfaces.DefaultMaxQueueBacklog})
	t.Cleanup(b.Close)
	b.StartDeadLetterWorker()
	declareReliableSource(t, b)
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "expiry-target", nil)
	require.NoError(t, b.PublishMessage("", "reliable-source", &protocol.Message{
		Body: []byte("expired payload"), Expiration: "0",
	}))
	fault.failRead.Store(true)
	msg, _, _, err := b.GetMessageForGet("reliable-source", true)
	require.ErrorIs(t, err, errDeadLetterSourceRead)
	require.Nil(t, msg)
	require.Equal(t, uint32(1), b.GetQueueReadyCount("reliable-source"))

	fault.failRead.Store(false)
	msg, _, _, err = b.GetMessageForGet("reliable-source", true)
	require.NoError(t, err)
	require.Nil(t, msg)
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("expiry-target") > 0 }, 3*time.Second, 10*time.Millisecond)
	msg = drainOne(t, b, "expiry-target")
	require.NotNil(t, msg)
	require.Equal(t, "expired payload", string(msg.Body))
	assertEntry(t, xDeathEntries(t, msg.Headers)[0], "reliable-source", DeadLetterExpired, 1)
}

func TestReliableDeadLetter_ReaperKeepsUnreadableWALHead(t *testing.T) {
	config := interfaces.EngineConfig{
		HotCacheQueueBytes: 1,
		MaxQueueBacklog:    interfaces.DefaultMaxQueueBacklog,
	}
	s, err := storage.NewStorageEngineWithConfig(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	b := NewStorageBroker(s, config)
	t.Cleanup(b.Close)
	var now atomic.Int64
	now.Store(1_000_000)
	b.SetTTLClock(now.Load)
	b.StartDeadLetterWorker()
	const queue = "reliable-source"
	_, err = b.DeclareQueue(queue, true, false, false, map[string]any{
		"x-dead-letter-exchange": "reliable-dlx",
		"x-dead-letter-strategy": "at-least-once",
		"x-overflow":             "reject-publish",
		"x-message-ttl":          int32(time.Hour.Milliseconds()),
	})
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "expiry-target", nil)
	require.NoError(t, b.PublishMessage("", queue, &protocol.Message{
		Body: []byte("cold expired payload"), DeliveryMode: 2,
	}))

	var failRead atomic.Bool
	failRead.Store(true)
	restore := storage.SetWALReadAtForTest(func(f *os.File, buf []byte, offset int64) (int, error) {
		if failRead.Load() {
			return 0, errDeadLetterSourceRead
		}
		return f.ReadAt(buf, offset)
	})
	t.Cleanup(restore)
	qs := b.getOrCreateQueueState(queue)
	tail := qs.Tail()
	now.Add((2 * time.Hour).Milliseconds())
	b.reapSweep(queue, qs)
	require.Equal(t, tail, qs.Tail(), "a cold read failure must leave the head eligible for retry")
	require.Equal(t, uint32(1), b.GetQueueReadyCount(queue))

	failRead.Store(false)
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("expiry-target") > 0 }, 3*time.Second, 10*time.Millisecond)
	msg := drainOne(t, b, "expiry-target")
	require.NotNil(t, msg)
	require.Equal(t, "cold expired payload", string(msg.Body))
	assertEntry(t, xDeathEntries(t, msg.Headers)[0], queue, DeadLetterExpired, 1)
}
