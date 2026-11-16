package broker

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

func declareReliableSource(t *testing.T, b *StorageBroker) {
	t.Helper()
	_, err := b.DeclareQueue("reliable-source", true, false, false, map[string]any{
		"x-dead-letter-exchange": "reliable-dlx",
		"x-dead-letter-strategy": "at-least-once",
		"x-overflow":             "reject-publish",
	})
	require.NoError(t, err)
}

func rejectReliableSource(t *testing.T, b *StorageBroker) {
	t.Helper()
	// A transient source message cannot be repopulated by ordinary WAL
	// recovery, so delivery after reopening requires a pending transfer.
	require.NoError(t, b.PublishMessage("", "reliable-source", &protocol.Message{
		Body: []byte("dead letter"), Headers: map[string]any{"typed": int32(7)},
	}))
	msg, tag, _, err := b.GetMessageForGet("reliable-source", false)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.NoError(t, b.RejectGetDelivery(tag, false))
	require.Zero(t, b.GetQueueReadyCount("reliable-source"))
}

func declareReliableTarget(t *testing.T, b *StorageBroker, name string, args map[string]any) {
	t.Helper()
	_, err := b.DeclareQueue(name, true, false, false, args)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue(name, "reliable-dlx", "", nil))
}

func awaitReliableDelivery(t *testing.T, b *StorageBroker, queue string) *protocol.Message {
	t.Helper()
	require.Eventually(t, func() bool { return b.GetQueueReadyCount(queue) > 0 }, 3*time.Second, 10*time.Millisecond)
	msg := drainOne(t, b, queue)
	require.NotNil(t, msg)
	require.Equal(t, "dead letter", string(msg.Body))
	require.Equal(t, uint8(2), msg.DeliveryMode)
	require.Equal(t, int32(7), msg.Headers["typed"])
	assertEntry(t, xDeathEntries(t, msg.Headers)[0], "reliable-source", DeadLetterRejected, 1)
	return msg
}

type stagingFaultStorage struct {
	*storage.StorageEngine
	failStage atomic.Bool
	attempts  atomic.Int64
}

func (s *stagingFaultStorage) StageDeadLetter(source string, msg *protocol.Message) error {
	s.attempts.Add(1)
	if s.failStage.Load() {
		return errors.New("injected outbox commit failure")
	}
	return s.StorageEngine.StageDeadLetter(source, msg)
}

func TestReliableDeadLetter_RetriesGetExpiryWithoutAConsumer(t *testing.T) {
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
	require.NoError(t, b.PublishMessage("", "reliable-source", &protocol.Message{
		Body: []byte("expired payload"), Expiration: "0",
	}))
	fault.failStage.Store(true)
	msg, _, _, err := b.GetMessageForGet("reliable-source", true)
	require.ErrorContains(t, err, "injected outbox commit failure")
	require.Nil(t, msg)
	require.Equal(t, uint32(1), b.GetQueueReadyCount("reliable-source"))
	require.Eventually(t, func() bool { return fault.attempts.Load() > 1 }, 3*time.Second, 10*time.Millisecond,
		"the idle expiry reaper must retry the popped tag")

	fault.failStage.Store(false)
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("expiry-target") > 0 }, 3*time.Second, 10*time.Millisecond)
	msg = drainOne(t, b, "expiry-target")
	require.NotNil(t, msg)
	require.Equal(t, "expired payload", string(msg.Body))
	require.Empty(t, msg.Expiration)
	assertEntry(t, xDeathEntries(t, msg.Headers)[0], "reliable-source", DeadLetterExpired, 1)
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("reliable-source") == 0 }, 3*time.Second, 10*time.Millisecond)
}

func TestReliableDeadLetter_RetriesFullTargetWithoutRepeatingHealthyTarget(t *testing.T) {
	b, closeBroker := createTestBroker(t)
	defer closeBroker()
	b.StartDeadLetterWorker()
	declareReliableSource(t, b)
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "blocked", map[string]any{"x-overflow": "reject-publish", "x-max-length": int32(1)})
	declareReliableTarget(t, b, "healthy", nil)
	publishToSource(t, b, "blocked", "filler")
	rejectReliableSource(t, b)
	awaitReliableDelivery(t, b, "healthy")
	require.Equal(t, uint32(1), b.GetQueueReadyCount("blocked"))
	require.Equal(t, "filler", string(drainOne(t, b, "blocked").Body))
	awaitReliableDelivery(t, b, "blocked")
	require.Nil(t, drainOne(t, b, "healthy"))
}

func TestReliableDeadLetter_ReplaysPendingTransferAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	open := func() (*StorageBroker, func()) {
		s, err := storage.NewStorageEngineWithDataDir(dataDir)
		require.NoError(t, err)
		b := NewStorageBroker(s, interfaces.EngineConfig{MaxQueueBacklog: 52428})
		require.NoError(t, b.InitError())
		b.StartDeadLetterWorker()
		return b, func() {
			b.Close()
			require.NoError(t, s.Close())
		}
	}
	func() {
		b, closeBroker := open()
		defer closeBroker()
		declareReliableSource(t, b)
		rejectReliableSource(t, b)
	}()

	b, closeBroker := open()
	defer closeBroker()
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "recovered-target", nil)
	awaitReliableDelivery(t, b, "recovered-target")
}
