package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// newLinkedConsumer registers a consumer whose protocol.Consumer carries a live
// Channel (and Channel.Connection) so the BOB-8 linkage fields can be asserted.
func newLinkedConsumer(t *testing.T, b *StorageBroker, queue, id, tag string, noAck bool, prefetch uint16, ch *protocol.Channel) *protocol.Consumer {
	t.Helper()
	c := &protocol.Consumer{
		Tag:           tag,
		Queue:         queue,
		NoAck:         noAck,
		PrefetchCount: prefetch,
		Channel:       ch,
		Messages:      make(chan *protocol.Delivery, 16),
	}
	require.NoError(t, registerConsumer(b, queue, id, c))
	return c
}

func newLinkedChannel(id uint16) (*protocol.Connection, *protocol.Channel) {
	conn := protocol.NewConnection(nil)
	ch := protocol.NewChannel(id, conn)
	return conn, ch
}

// --- BOB-1: QueueMessageCounts ---------------------------------------------

func TestQueueMessageCounts_UnknownQueueIsZeroKnownFalse(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	got := b.QueueMessageCounts("does-not-exist")
	require.False(t, got.Known, "an unknown queue is not Known")
	require.EqualValues(t, 0, got.Ready)
	require.EqualValues(t, 0, got.Unacked)
	require.EqualValues(t, 0, got.Total)

	_, ok := b.queueStates.Load("does-not-exist")
	require.False(t, ok, "the accessor must never create queue state")
}

func TestQueueMessageCounts_ReadyOnlyAfterPublish(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		publishToSource(t, b, "q", "m")
	}

	got := b.QueueMessageCounts("q")
	require.True(t, got.Known)
	require.EqualValues(t, 3, got.Ready)
	require.EqualValues(t, 0, got.Unacked)
	require.EqualValues(t, 3, got.Total)
}

func TestQueueMessageCounts_InflightAfterManualAckGet(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	publishToSource(t, b, "q", "m0")
	publishToSource(t, b, "q", "m1")

	msg, _, _, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.NotNil(t, msg)

	got := b.QueueMessageCounts("q")
	require.True(t, got.Known)
	require.EqualValues(t, 1, got.Ready)
	require.EqualValues(t, 1, got.Unacked)
	require.EqualValues(t, 2, got.Total)
}

func TestQueueMessageCounts_AfterAck(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	publishToSource(t, b, "q", "m0")
	publishToSource(t, b, "q", "m1")

	_, tag, _, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.NoError(t, b.AcknowledgeGetDelivery(tag))

	got := b.QueueMessageCounts("q")
	require.True(t, got.Known)
	require.EqualValues(t, 1, got.Ready)
	require.EqualValues(t, 0, got.Unacked)
	require.EqualValues(t, 1, got.Total)
}

// --- BOB-8: ConsumerViews ---------------------------------------------------

func TestConsumerViews_Linkage(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	conn, ch := newLinkedChannel(7)
	args := map[string]interface{}{"x-priority": int32(5)}
	c := &protocol.Consumer{
		Tag:           "tag-1",
		Queue:         "q",
		NoAck:         false,
		Exclusive:     true,
		PrefetchCount: 3,
		Args:          args,
		Channel:       ch,
		Messages:      make(chan *protocol.Delivery, 16),
	}
	require.NoError(t, registerConsumer(b, "q", "id-1", c))

	views := b.ConsumerViews()
	require.Len(t, views, 1)
	v := views[0]
	require.Equal(t, "id-1", v.ID)
	require.Equal(t, "tag-1", v.Tag)
	require.Equal(t, "q", v.Queue)
	require.False(t, v.NoAck)
	require.True(t, v.Exclusive)
	require.EqualValues(t, 3, v.PrefetchCount)
	require.Equal(t, args, v.Args)
	require.True(t, v.HasChannel)
	require.EqualValues(t, 7, v.ChannelID)
	require.Equal(t, conn.ID, v.ConnectionID)
}

func TestConsumerViews_NoAckIsZeroAckKnown(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	_, ch := newLinkedChannel(1)
	newLinkedConsumer(t, b, "q", "noack-id", "noack-tag", true, 0, ch)

	views := b.ConsumerViews()
	require.Len(t, views, 1)
	require.True(t, views[0].NoAck)
	require.EqualValues(t, 0, views[0].Unacked)
	require.True(t, views[0].AckKnown, "a no-ack consumer's unacked set is exactly 0 and known")
}

func TestConsumerViews_PrefetchZeroIsUnknown(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	_, ch := newLinkedChannel(1)
	newLinkedConsumer(t, b, "q", "p0-id", "p0-tag", false, 0, ch)

	views := b.ConsumerViews()
	require.Len(t, views, 1)
	require.False(t, views[0].NoAck)
	require.False(t, views[0].AckKnown,
		"a manual-ack prefetch-0 consumer bypasses the gate, so its unacked set is unknown")
	require.EqualValues(t, 0, views[0].Unacked)
}

func TestConsumerViews_ManualAckInflight(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	_, ch := newLinkedChannel(1)
	c := newLinkedConsumer(t, b, "q", "ma-id", "ma-tag", false, 1, ch)

	publishToSource(t, b, "q", "m0")

	select {
	case <-c.Messages:
		// delivered, deliberately not acked
	case <-time.After(5 * time.Second):
		t.Fatal("manual-ack consumer did not receive the delivery within 5s")
	}

	views := b.ConsumerViews()
	require.Len(t, views, 1)
	require.True(t, views[0].AckKnown)
	require.EqualValues(t, 1, views[0].Unacked)
}

func TestConsumerViews_DuplicateTagDistinctEntries(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	conn, ch1 := newLinkedChannel(1)
	ch2 := protocol.NewChannel(2, conn)

	c1 := &protocol.Consumer{Tag: "dup", Queue: "q", Channel: ch1, Messages: make(chan *protocol.Delivery, 4)}
	c2 := &protocol.Consumer{Tag: "dup", Queue: "q", Channel: ch2, Messages: make(chan *protocol.Delivery, 4)}
	require.NoError(t, registerConsumer(b, "q", "id-1", c1))
	require.NoError(t, registerConsumer(b, "q", "id-2", c2))

	views := b.ConsumerViews()
	require.Len(t, views, 2, "same-tag consumers on different channels are distinct entries")

	byID := map[string]uint16{}
	for _, v := range views {
		require.Equal(t, "dup", v.Tag)
		byID[v.ID] = v.ChannelID
	}
	require.Len(t, byID, 2)
	require.NotEqual(t, byID["id-1"], byID["id-2"], "distinct channels must be reported")
}

// TestConsumerViews_IDNeverEqualsTagByAccident builds the management map keyed
// by ID (never Tag) and proves two same-tag views survive the mapping.
func TestConsumerViews_IDNeverEqualsTagByAccident(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	conn, ch1 := newLinkedChannel(1)
	ch2 := protocol.NewChannel(2, conn)

	require.NoError(t, registerConsumer(b, "q", "consumer-a", &protocol.Consumer{
		Tag: "shared", Queue: "q", Channel: ch1, Messages: make(chan *protocol.Delivery, 4),
	}))
	require.NoError(t, registerConsumer(b, "q", "consumer-b", &protocol.Consumer{
		Tag: "shared", Queue: "q", Channel: ch2, Messages: make(chan *protocol.Delivery, 4),
	}))

	byID := map[string]ConsumerView{}
	for _, v := range b.ConsumerViews() {
		byID[v.ID] = v
	}
	require.Len(t, byID, 2, "keying by ID preserves both same-tag consumers")
	_, hasA := byID["consumer-a"]
	_, hasB := byID["consumer-b"]
	require.True(t, hasA)
	require.True(t, hasB)

	byTag := map[string]int{}
	for _, v := range b.ConsumerViews() {
		byTag[v.Tag]++
	}
	require.Equal(t, 2, byTag["shared"], "tag-keying would have collapsed them to one")
}

// --- BOB-11: broker-side EntityObserver -------------------------------------

type entityEvent struct {
	kind  string
	delta int64
}

type recordingObserver struct {
	mu     sync.Mutex
	events []entityEvent
}

func (o *recordingObserver) OnBrokerEntityChange(kind string, delta int64) {
	o.mu.Lock()
	o.events = append(o.events, entityEvent{kind: kind, delta: delta})
	o.mu.Unlock()
}

func (o *recordingObserver) snapshot() []entityEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]entityEvent, len(o.events))
	copy(out, o.events)
	return out
}

func TestEntityObserver_QueueExchangeConsumerDeltas(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	obs := &recordingObserver{}
	b.SetEntityObserver(obs)

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	// Idempotent re-declare of an existing queue must emit no delta.
	_, err = b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeclareExchange("x1", "direct", false, false, false, nil))
	require.NoError(t, b.DeleteExchange("x1", false))

	_, ch := newLinkedChannel(1)
	newLinkedConsumer(t, b, "q", "cid", "tag", false, 1, ch)
	require.NoError(t, b.UnregisterConsumer("cid"))

	_, err = b.DeleteQueue("q", false, false)
	require.NoError(t, err)

	want := []entityEvent{
		{kind: "queue", delta: 1},
		{kind: "exchange", delta: 1},
		{kind: "exchange", delta: -1},
		{kind: "consumer", delta: 1},
		{kind: "consumer", delta: -1},
		{kind: "queue", delta: -1},
	}
	require.Equal(t, want, obs.snapshot())
}

// TestEntityObserver_NotFiredOnIdempotentRedeclare is covered inside
// TestEntityObserver_QueueExchangeConsumerDeltas (the second DeclareQueue
// before any exchange/consumer event must contribute nothing). Kept as a named
// test so the spec's item 12 has an explicit green marker.
func TestEntityObserver_NotFiredOnIdempotentRedeclare(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	obs := &recordingObserver{}
	b.SetEntityObserver(obs)

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	_, err = b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	require.Equal(t, []entityEvent{{kind: "queue", delta: 1}}, obs.snapshot())
}
