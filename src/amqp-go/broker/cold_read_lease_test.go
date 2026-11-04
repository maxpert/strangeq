package broker

// WS-COLDREAD-PATH broker tests (spec §5.16–§5.19).
//
// The release points in the broker are the sites that DROP or REQUEUE a
// delivery instead of writing it: the deliverMessage stopCh arm (W5), the
// expired-on-delivery/get drops (W6), and the UnregisterConsumer drain (W7).
// The DLX clone (cloneForDeadLetter) is the one retained value-copy and must
// deep-copy a leased body.

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
)

// leasedMessage builds a message carrying a release token that counts its
// invocations. It models a delivery that came from the cold-read pool.
func leasedMessage(body []byte) (*protocol.Message, *atomic.Int32) {
	var n atomic.Int32
	m := &protocol.Message{
		Exchange:     "",
		RoutingKey:   "q",
		Body:         append([]byte(nil), body...),
		DeliveryMode: 2,
	}
	m.AttachBodyRelease(func() { n.Add(1) })
	return m, &n
}

// createLeaseTestBroker builds a broker whose hot cache refuses every
// admission, so a durable delivery read is a REFUSED admission that keeps its
// lease.
func createLeaseTestBroker(t *testing.T) *StorageBroker {
	t.Helper()
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
		HotCacheQueueBytes:      1,
	}
	store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
	require.NoError(t, err)
	b := NewStorageBroker(store, ec)
	t.Cleanup(func() {
		b.Close()
		_ = store.Close()
	})
	return b
}

// --- 16. the stopCh arm releases the lease -----------------------------------

func TestDeliverMessage_StopChReleasesLease(t *testing.T) {
	storage.ResetColdReadPoolForTest()
	t.Cleanup(storage.ResetColdReadPoolForTest)
	b := createLeaseTestBroker(t)

	const queue = "lease-stopch-q"
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(queue)

	big := make([]byte, 64*1024)
	msg := &protocol.Message{Exchange: "", RoutingKey: queue, Body: big, DeliveryMode: 2}
	require.NoError(t, b.PublishMessage("", queue, msg))
	msgID := msg.DeliveryTag
	require.NotZero(t, msgID)

	state := &ConsumerState{
		consumer: &protocol.Consumer{
			ID:       "c1",
			Tag:      "c1",
			Queue:    queue,
			Messages: make(chan *protocol.Delivery, 1),
		},
		queueName: queue,
		gate:      newPrefetchGate(10),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	close(state.done)
	// Fill the delivery channel so the send is NOT ready: the select must take
	// the (closed) stopCh arm deterministically.
	state.consumer.Messages <- &protocol.Delivery{DeliveryTag: msgID + 1000}
	close(state.stopCh)

	b.deliverMessage(qs, state, msgID, false)

	require.Equal(t, int64(0), storage.ColdReadPoolLeaseCountForTest(),
		"the stopCh arm must release the cold-read lease")
	_, free := storage.ColdReadPoolStatsForTest()
	require.GreaterOrEqual(t, free, 1, "the pooled buffer must be back in the free-list")
}

// --- 17. the expired drops release the lease ---------------------------------

func TestDropExpiredOnDelivery_ReleasesLease(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const queue = "lease-drop-delivery-q"
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(queue)

	msg, released := leasedMessage([]byte("expired"))
	b.dropExpiredOnDelivery(qs, b.consumerStateForTest(queue), msg, 4242, qs.Policy())

	require.Equal(t, int32(1), released.Load(), "the expired delivery must release the lease")
}

func TestDropExpiredOnGet_ReleasesLease(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const queue = "lease-drop-get-q"
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(queue)

	msg, released := leasedMessage([]byte("expired"))
	b.dropExpiredOnGet(qs, queue, msg, 4343, qs.Policy())

	require.Equal(t, int32(1), released.Load(), "the expired get must release the lease")
}

// --- 18. the DLX clone deep-copies a leased body -----------------------------

func TestCloneForDeadLetter_DeepCopiesLeasedBody(t *testing.T) {
	body := []byte("dead-letter-body")
	msg, released := leasedMessage(body)

	clone := cloneForDeadLetter(msg)

	require.False(t, clone.HasBodyRelease(), "a clone must not carry the source's lease")
	require.NotSame(t, &msg.Body[0], &clone.Body[0],
		"the clone's body must be a distinct backing array")

	// Mutating the source buffer must not change the clone.
	for i := range msg.Body {
		msg.Body[i] = 0xEE
	}
	require.Equal(t, body, clone.Body, "the clone must own an independent copy")

	// The source's release must not free the clone's body.
	msg.ReleaseBody()
	require.Equal(t, int32(1), released.Load())
	require.Equal(t, body, clone.Body, "the clone's body must survive the source's release")
}

// --- 19. the UnregisterConsumer drain releases the lease ---------------------

func TestUnregisterDrain_ReleasesLeasedBodies(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const queue = "lease-drain-q"
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(queue)

	state := b.consumerStateForTest(queue)
	msg, released := leasedMessage([]byte("drained"))
	state.consumer.Messages <- &protocol.Delivery{Message: msg, DeliveryTag: 5151}

	b.drainConsumerMessages(qs, state)

	require.Equal(t, int32(1), released.Load(), "the drain must release each dropped delivery")
}

// consumerStateForTest builds a minimal ConsumerState with a buffered delivery
// channel, for direct calls to the drop/drain paths.
func (b *StorageBroker) consumerStateForTest(queue string) *ConsumerState {
	return &ConsumerState{
		consumer: &protocol.Consumer{
			ID:       "c1",
			Tag:      "c1",
			Queue:    queue,
			Messages: make(chan *protocol.Delivery, 4),
		},
		queueName: queue,
		gate:      newPrefetchGate(10),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}
