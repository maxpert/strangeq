package broker

// THE BROKER'S READ TRIAGE: A DELIVERY ADMITS, A PROBE DOES NOT.
//
// The hot cache's write-through admission is attributable to a CONSUMER
// DELIVERY. Every read of a message is one of two things:
//
//   - a DELIVERY: the message is being handed to a consumer, so it IS in the
//     consumer's working set, and the read admits it into the hot cache;
//   - a PEEK: a probe (the TTL reaper), an existence check (the purge, the
//     drop-head eviction, the recovery byte accounting) or a dead-letter
//     read. Nothing asked for that message, so the read must NOT admit it:
//     admitting would fill the cache with messages nobody is reading and
//     inflate the queue's REPORTED MESSAGE COUNT.
//
// The fixture below counts the two reads, so the triage is observable rather
// than asserted by inspection.

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
)

// readTriageStorage counts the two reads. It embeds the CONCRETE storage so
// the broker's optional-interface assertions (ringDeleterDetailed and
// friends) still resolve against it.
type readTriageStorage struct {
	*storage.DisruptorStorage

	mu         sync.Mutex
	deliveries int
	peeks      int
}

func (s *readTriageStorage) GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	s.mu.Lock()
	s.peeks++
	s.mu.Unlock()
	return s.DisruptorStorage.GetMessage(queueName, deliveryTag)
}

func (s *readTriageStorage) GetMessageForDelivery(queueName string, deliveryTag uint64) (*protocol.Message, error) {
	s.mu.Lock()
	s.deliveries++
	s.mu.Unlock()
	return s.DisruptorStorage.GetMessageForDelivery(queueName, deliveryTag)
}

func (s *readTriageStorage) counts() (deliveries, peeks int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deliveries, s.peeks
}

func newReadTriageBroker(t *testing.T) (*StorageBroker, *readTriageStorage, func()) {
	t.Helper()
	store, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	fake := &readTriageStorage{DisruptorStorage: store}
	engineConfig := interfaces.EngineConfig{
		DepthHighWM:             52428,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	b := NewStorageBroker(fake, engineConfig)
	cleanup := func() {
		b.Close()
		_ = store.Close()
	}
	return b, fake, cleanup
}

// TestReadTriage_ConsumerDeliveryUsesTheAdmittingRead: a consumer delivery
// reads through the ADMITTING read. The message it hands out is in the
// consumer's working set, so the read admits it.
func TestReadTriage_ConsumerDeliveryUsesTheAdmittingRead(t *testing.T) {
	b, fake, cleanup := newReadTriageBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("triage-q", false, false, false, nil)
	require.NoError(t, err)

	msg := &protocol.Message{RoutingKey: "triage-q", Body: []byte("payload")}
	require.NoError(t, b.PublishMessage("", "triage-q", msg))

	deliveries, peeks := fake.counts()
	require.Equal(t, 0, deliveries, "no delivery has happened yet")
	require.Equal(t, 0, peeks, "nothing has read the message yet")

	// A basic.get IS a delivery.
	got, _, _, err := b.GetMessageForGet("triage-q", true)
	require.NoError(t, err)
	require.NotNil(t, got)

	deliveries, peeks = fake.counts()
	require.Equal(t, 1, deliveries, "a consumer delivery must use the admitting read")
	require.Equal(t, 0, peeks, "a consumer delivery must not use the peek read")
}

// TestReadTriage_ReaperProbeUsesTheNonAdmittingRead: the TTL reaper's sweep
// probes every ready tag. It is a PROBE, so it must use the non-admitting
// read: it walks the whole ready set on every sweep, and an admitting probe
// would fill the hot cache with messages that are about to be expired and
// inflate the count the broker reports for the queue.
func TestReadTriage_ReaperProbeUsesTheNonAdmittingRead(t *testing.T) {
	b, fake, cleanup := newReadTriageBroker(t)
	defer cleanup()

	qs := declareTTLQueue(t, b, "reaper-probe-q", map[string]interface{}{"x-message-ttl": 60000})

	msg := &protocol.Message{RoutingKey: "reaper-probe-q", Body: []byte("payload")}
	require.NoError(t, b.PublishMessage("", "reaper-probe-q", msg))

	deliveriesBefore, peeksBefore := fake.counts()

	// ONE sweep of the reaper, called directly so the fixture is
	// deterministic (no timer).
	b.reapTTLSweep("reaper-probe-q", qs, qs.Policy(), b.ttlNowMillis())

	deliveriesAfter, peeksAfter := fake.counts()
	require.Greater(t, peeksAfter, peeksBefore,
		"the reaper's sweep must probe the ready tags")
	require.Equal(t, deliveriesBefore, deliveriesAfter,
		"the reaper's sweep is a PROBE: it must not use the admitting read")
}

// TestReadTriage_PurgeExistenceCheckUsesTheNonAdmittingRead: the purge reads
// a message only to learn its body length. It is an existence check, so it
// must not admit.
func TestReadTriage_PurgeExistenceCheckUsesTheNonAdmittingRead(t *testing.T) {
	b, fake, cleanup := newReadTriageBroker(t)
	defer cleanup()

	// The purge reads the body ONLY for the byte accounting, so the queue
	// must enforce x-max-length-bytes.
	_, err := b.DeclareQueue("purge-probe-q", false, false, false, map[string]interface{}{
		"x-max-length-bytes": int64(1 << 20),
	})
	require.NoError(t, err)

	msg := &protocol.Message{RoutingKey: "purge-probe-q", Body: []byte("payload")}
	require.NoError(t, b.PublishMessage("", "purge-probe-q", msg))

	deliveriesBefore, peeksBefore := fake.counts()

	_, err = b.PurgeQueue("purge-probe-q")
	require.NoError(t, err)

	deliveriesAfter, peeksAfter := fake.counts()
	require.Greater(t, peeksAfter, peeksBefore,
		"the purge's existence check must use the non-admitting read")
	require.Equal(t, deliveriesBefore, deliveriesAfter,
		"the purge's existence check must not use the admitting read")
}

// TestReadTriage_SingleDeadLetterUsesTheNonAdmittingRead: the SINGLE
// reject/nack dead-letter path reads the source message to republish it to
// the dead-letter exchange.
//
// It is a SETTLE, not a consumer delivery: the source message is removed
// immediately afterwards, so admitting it would only churn the hot cache and
// inflate the queue's reported message count with a message that is about to
// be gone. The BATCH path already peeks (storage_broker.go,
// discardNackedBatch); the single path must peek too.
//
// BOTH single arms are covered: basic.reject and basic.nack (multiple=false)
// share the same dead-letter hand-off.
func TestReadTriage_SingleDeadLetterUsesTheNonAdmittingRead(t *testing.T) {
	arms := []struct {
		name    string
		discard func(b *StorageBroker, tag uint64) error
	}{
		{
			name: "reject",
			discard: func(b *StorageBroker, tag uint64) error {
				return b.RejectMessage("c1", tag, false)
			},
		},
		{
			name: "nack",
			discard: func(b *StorageBroker, tag uint64) error {
				return b.NacknowledgeMessage("c1", tag, false, false)
			},
		},
	}

	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			b, fake, cleanup := newReadTriageBroker(t)
			defer cleanup()

			// The DLX topology: one dead-letter exchange, one target queue
			// bound to it, and a source queue whose policy points at the DLX.
			require.NoError(t, b.DeclareExchange("triage-dlx", "direct", false, false, false, nil))
			_, err := b.DeclareQueue("triage-dlq", false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, b.BindQueue("triage-dlq", "triage-dlx", "triage-src", nil))
			_, err = b.DeclareQueue("triage-src", false, false, false, map[string]interface{}{
				"x-dead-letter-exchange": "triage-dlx",
			})
			require.NoError(t, err)

			c := registerManualConsumer(t, b, "triage-src", "c1", 10, 10)
			msg := &protocol.Message{RoutingKey: "triage-src", Body: []byte("payload")}
			require.NoError(t, b.PublishMessage("", "triage-src", msg))
			d := recvDeliveries(t, c, 1)[0]

			// The delivery that reached the consumer is the ONE admitting read.
			deliveriesBefore, peeksBefore := fake.counts()
			require.Equal(t, 1, deliveriesBefore,
				"the consumer delivery must be the one admitting read")
			require.Equal(t, 0, peeksBefore,
				"the consumer delivery must not use the peek read")

			// THE SINGLE DISCARD (requeue=false): the dead-letter hand-off.
			require.NoError(t, arm.discard(b, d.DeliveryTag))

			deliveriesAfter, peeksAfter := fake.counts()
			require.Equal(t, 1, peeksAfter-peeksBefore,
				"the single dead-letter read must be exactly ONE peek")
			require.Equal(t, deliveriesBefore, deliveriesAfter,
				"the single dead-letter read must NOT use the admitting read")

			// The message really was dead-lettered.
			require.NotNil(t, drainOne(t, b, "triage-dlq"),
				"the discarded message must be dead-lettered")
		})
	}
}
