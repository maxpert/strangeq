package broker

import (
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutoDeleteQueueDeletedWhenLastConsumerLeaves(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("auto-del-q", false, true, false, nil)
	broker.BindQueue("auto-del-q", "", "auto-del-q", nil)

	consumer1 := &protocol.Consumer{
		Tag:           "c1",
		Queue:         "auto-del-q",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err := broker.RegisterConsumer("auto-del-q", "c1", consumer1)
	require.NoError(t, err)

	_, err = broker.storage.GetQueue("auto-del-q")
	require.NoError(t, err)

	err = broker.UnregisterConsumer("c1")
	require.NoError(t, err)

	_, err = broker.storage.GetQueue("auto-del-q")
	assert.Error(t, err, "auto-delete queue should be deleted when last consumer leaves")
}

func TestAutoDeleteExchangeDeletedWhenLastBindingRemoved(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareExchange("auto-del-ex", "direct", false, true, false, nil)
	broker.DeclareQueue("adex-q", false, false, false, nil)
	broker.BindQueue("adex-q", "auto-del-ex", "rk1", nil)

	_, err := broker.storage.GetExchange("auto-del-ex")
	require.NoError(t, err)

	err = broker.UnbindQueue("adex-q", "auto-del-ex", "rk1")
	require.NoError(t, err)

	_, err = broker.storage.GetExchange("auto-del-ex")
	assert.Error(t, err, "auto-delete exchange should be deleted when last binding removed")
}

func TestExclusiveConsumerRejectsSecondConsumer(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-q", false, false, false, nil)

	consumer1 := &protocol.Consumer{
		Tag:           "excl-c1",
		Queue:         "excl-q",
		Exclusive:     true,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err := broker.RegisterConsumer("excl-q", "excl-c1", consumer1)
	require.NoError(t, err)

	consumer2 := &protocol.Consumer{
		Tag:           "excl-c2",
		Queue:         "excl-q",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err = broker.RegisterConsumer("excl-q", "excl-c2", consumer2)
	assert.Error(t, err, "second consumer should be rejected on exclusive queue")

	broker.UnregisterConsumer("excl-c1")
}

func TestExclusiveConsumerRejectsNonExclusiveSecond(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-q2", false, false, false, nil)

	consumer1 := &protocol.Consumer{
		Tag:           "excl-c1",
		Queue:         "excl-q2",
		Exclusive:     true,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err := broker.RegisterConsumer("excl-q2", "excl-c1", consumer1)
	require.NoError(t, err)

	consumer2 := &protocol.Consumer{
		Tag:           "non-excl-c2",
		Queue:         "excl-q2",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err = broker.RegisterConsumer("excl-q2", "non-excl-c2", consumer2)
	assert.Error(t, err)

	broker.UnregisterConsumer("excl-c1")
}

func TestDeleteExchange_RemovesBindings_NoStaleMatchOnRecreate(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareExchange("te-ex", "direct", false, false, false, nil)
	broker.DeclareQueue("te-q", false, false, false, nil)
	broker.BindQueue("te-q", "te-ex", "key1", nil)

	bindings, err := broker.storage.GetExchangeBindings("te-ex")
	require.NoError(t, err)
	require.Len(t, bindings, 1)

	err = broker.DeleteExchange("te-ex", false)
	require.NoError(t, err)

	bindings, err = broker.storage.GetExchangeBindings("te-ex")
	require.NoError(t, err)
	assert.Empty(t, bindings, "bindings should be removed when exchange is deleted")

	broker.DeclareExchange("te-ex", "direct", false, false, false, nil)
	msg := &protocol.Message{Body: []byte("test"), RoutingKey: "key1", Exchange: "te-ex"}
	err = broker.PublishMessage("te-ex", "key1", msg)
	require.NoError(t, err)

	msgs, err := broker.storage.GetQueueMessages("te-q")
	require.NoError(t, err)
	assert.Empty(t, msgs, "recreated exchange should have no stale bindings")
}

func TestQueueDeleteOK_RealMessageCount(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("del-count-q", false, false, false, nil)

	for i := 0; i < 5; i++ {
		msg := &protocol.Message{
			Body:       []byte("msg"),
			Exchange:   "",
			RoutingKey: "del-count-q",
		}
		err := broker.PublishMessage("", "del-count-q", msg)
		require.NoError(t, err)
	}

	count, err := broker.DeleteQueue("del-count-q", false, false)
	require.NoError(t, err)
	assert.Equal(t, 5, count, "DeleteQueue should return purged message count")
}

func TestQueueDeclareOK_RealConsumerCount(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("cc-q", false, false, false, nil)

	for i := 0; i < 3; i++ {
		c := &protocol.Consumer{
			Tag:           "c" + string(rune('A'+i)),
			Queue:         "cc-q",
			PrefetchCount: 10,
			Messages:      make(chan *protocol.Delivery, 10),
			Cancel:        make(chan struct{}, 1),
		}
		err := broker.RegisterConsumer("cc-q", c.Tag, c)
		require.NoError(t, err)
	}

	count := broker.GetQueueConsumerCount("cc-q")
	assert.Equal(t, 3, count)

	broker.UnregisterConsumer("cA")
	broker.UnregisterConsumer("cB")
	broker.UnregisterConsumer("cC")
}

func TestFanout_StoredDeliveriesDoNotShareHeaders(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareExchange("hdr-fanout", "fanout", false, false, false, nil)
	broker.DeclareQueue("hdr-q1", false, false, false, nil)
	broker.DeclareQueue("hdr-q2", false, false, false, nil)
	broker.DeclareQueue("hdr-q3", false, false, false, nil)
	broker.BindQueue("hdr-q1", "hdr-fanout", "", nil)
	broker.BindQueue("hdr-q2", "hdr-fanout", "", nil)
	broker.BindQueue("hdr-q3", "hdr-fanout", "", nil)

	msg := &protocol.Message{
		Body:       []byte("fanout-headers"),
		Exchange:   "hdr-fanout",
		RoutingKey: "",
		Headers: map[string]interface{}{
			"key1": "val1",
			"key2": int32(42),
		},
	}
	err := broker.PublishMessage("hdr-fanout", "", msg)
	require.NoError(t, err)

	msgs1, err := broker.storage.GetQueueMessages("hdr-q1")
	require.NoError(t, err)
	require.Len(t, msgs1, 1)

	msgs2, err := broker.storage.GetQueueMessages("hdr-q2")
	require.NoError(t, err)
	require.Len(t, msgs2, 1)

	msgs1[0].Headers["key1"] = "MUTATED"

	assert.NotEqual(t, "MUTATED", msgs2[0].Headers["key1"],
		"mutating one queue's headers should not affect other queues")
	assert.Equal(t, "val1", msgs2[0].Headers["key1"])
}

func TestProcessCompleteMessage_StoredBodyHeadersSurvivePoolRecycle(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("pool-q", false, false, false, nil)

	pendingMsg := &protocol.PendingMessage{
		Body: []byte("survive-recycle"),
		Header: &protocol.ContentHeader{
			Headers: map[string]interface{}{
				"h": "v",
			},
		},
	}

	msg := &protocol.Message{
		Body:        pendingMsg.Body,
		Headers:     pendingMsg.Header.Headers,
		Exchange:    "",
		RoutingKey:  "pool-q",
		ContentType: "text/plain",
	}
	err := broker.PublishMessage("", "pool-q", msg)
	require.NoError(t, err)

	pendingMsg.Body = nil
	pendingMsg.Header.Headers = nil

	stored, err := broker.storage.GetQueueMessages("pool-q")
	require.NoError(t, err)
	require.Len(t, stored, 1)

	assert.Equal(t, "survive-recycle", string(stored[0].Body),
		"stored body should survive pool recycle")
	assert.Equal(t, "v", stored[0].Headers["h"],
		"stored headers should survive pool recycle")
}

func TestAutoDeleteExchangeE2E_UnbindDeletesExchange(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareExchange("e2e-src", "fanout", false, true, false, nil)
	broker.DeclareExchange("e2e-dst", "fanout", false, false, false, nil)
	broker.DeclareQueue("e2e-q", false, false, false, nil)
	broker.BindQueue("e2e-q", "e2e-dst", "", nil)
	broker.BindExchange("e2e-dst", "e2e-src", "", nil)

	_, err := broker.storage.GetExchange("e2e-src")
	require.NoError(t, err)

	err = broker.UnbindExchange("e2e-dst", "e2e-src", "")
	require.NoError(t, err)

	_, err = broker.storage.GetExchange("e2e-src")
	assert.Error(t, err, "auto-delete source exchange should be deleted when last e2e binding removed")
}

func TestConcurrentAutoDeleteNoDeadlock(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("deadlock-q", false, true, false, nil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tag := "dc-c" + string(rune('A'+idx))
			c := &protocol.Consumer{
				Tag:           tag,
				Queue:         "deadlock-q",
				PrefetchCount: 10,
				Messages:      make(chan *protocol.Delivery, 10),
				Cancel:        make(chan struct{}, 1),
			}
			if err := broker.RegisterConsumer("deadlock-q", tag, c); err != nil {
				return
			}
			broker.UnregisterConsumer(tag)
		}(i)
	}
	wg.Wait()
}
