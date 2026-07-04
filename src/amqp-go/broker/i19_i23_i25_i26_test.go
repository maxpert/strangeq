package broker

import (
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExclusiveQueueOwnerTracking_FirstClaimSucceeds(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-owner-q", false, false, true, nil)

	assert.True(t, broker.SetQueueOwnerIfFree("excl-owner-q", "conn-A"),
		"first claim should succeed")

	owner, ok := broker.GetQueueOwner("excl-owner-q")
	require.True(t, ok)
	assert.Equal(t, "conn-A", owner)
}

func TestExclusiveQueueOwnerTracking_SecondConnectionCannotHijack(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-hijack-q", false, false, true, nil)

	require.True(t, broker.SetQueueOwnerIfFree("excl-hijack-q", "conn-A"),
		"first claim should succeed")

	assert.False(t, broker.SetQueueOwnerIfFree("excl-hijack-q", "conn-B"),
		"second connection should not be able to hijack exclusive queue")
}

func TestExclusiveQueueOwnerTracking_SameConnectionCanReclaim(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-reclaim-q", false, false, true, nil)

	require.True(t, broker.SetQueueOwnerIfFree("excl-reclaim-q", "conn-A"))

	assert.True(t, broker.SetQueueOwnerIfFree("excl-reclaim-q", "conn-A"),
		"same connection should be able to re-declare its own exclusive queue")
}

func TestExclusiveQueueRegisterConsumer_RejectsOtherConnection(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-consume-q", false, false, true, nil)
	require.True(t, broker.SetQueueOwnerIfFree("excl-consume-q", "conn-A"))

	connB := protocol.NewConnection(nil)
	connB.ID = "conn-B"
	chB := protocol.NewChannel(1, connB)

	consumer := &protocol.Consumer{
		Tag:           "c-b",
		Queue:         "excl-consume-q",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
		Channel:       chB,
	}
	err := broker.RegisterConsumer("excl-consume-q", "c-b", consumer)
	assert.Error(t, err, "consumer from another connection should be rejected on exclusive queue")
}

func TestExclusiveQueueRegisterConsumer_AllowsOwningConnection(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-consume-ok-q", false, false, true, nil)
	require.True(t, broker.SetQueueOwnerIfFree("excl-consume-ok-q", "conn-A"))

	connA := protocol.NewConnection(nil)
	connA.ID = "conn-A"
	chA := protocol.NewChannel(1, connA)

	consumer := &protocol.Consumer{
		Tag:           "c-a",
		Queue:         "excl-consume-ok-q",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
		Channel:       chA,
	}
	err := broker.RegisterConsumer("excl-consume-ok-q", "c-a", consumer)
	assert.NoError(t, err, "owning connection should be able to consume from its exclusive queue")

	broker.UnregisterConsumer("c-a")
}

func TestGetQueuesOwnedByConnection_ReturnsExclusiveQueues(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-list-1", false, false, true, nil)
	broker.DeclareQueue("excl-list-2", false, false, true, nil)
	broker.DeclareQueue("non-excl-list", false, false, false, nil)

	require.True(t, broker.SetQueueOwnerIfFree("excl-list-1", "conn-A"))
	require.True(t, broker.SetQueueOwnerIfFree("excl-list-2", "conn-A"))

	owned := broker.GetQueuesOwnedByConnection("conn-A")
	assert.Len(t, owned, 2)
	assert.Contains(t, owned, "excl-list-1")
	assert.Contains(t, owned, "excl-list-2")

	none := broker.GetQueuesOwnedByConnection("conn-B")
	assert.Empty(t, none)
}

func TestDeleteQueueClearsOwnerTracking(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-del-owner-q", false, false, true, nil)
	require.True(t, broker.SetQueueOwnerIfFree("excl-del-owner-q", "conn-A"))

	broker.DeleteQueue("excl-del-owner-q", false, false)

	_, ok := broker.GetQueueOwner("excl-del-owner-q")
	assert.False(t, ok, "owner tracking should be cleared after DeleteQueue")

	assert.True(t, broker.SetQueueOwnerIfFree("excl-del-owner-q", "conn-B"),
		"after deletion, a new connection should be able to claim the queue name")
}

func TestExclusiveQueueConnectionCloseTeardown_DeletesOwnedQueues(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-teardown-1", false, false, true, nil)
	broker.DeclareQueue("excl-teardown-2", false, true, true, nil)
	broker.DeclareQueue("non-excl-teardown", false, false, false, nil)

	require.True(t, broker.SetQueueOwnerIfFree("excl-teardown-1", "conn-A"))
	require.True(t, broker.SetQueueOwnerIfFree("excl-teardown-2", "conn-A"))

	owned := broker.GetQueuesOwnedByConnection("conn-A")
	require.Len(t, owned, 2)

	for _, qName := range owned {
		broker.DeleteQueue(qName, false, false)
	}

	_, err := broker.storage.GetQueue("excl-teardown-1")
	assert.Error(t, err, "exclusive queue should be deleted on connection close teardown")

	_, err = broker.storage.GetQueue("excl-teardown-2")
	assert.Error(t, err, "exclusive auto-delete queue should be deleted on connection close teardown")

	_, err = broker.storage.GetQueue("non-excl-teardown")
	assert.NoError(t, err, "non-exclusive queue should survive connection close")
}

func TestConcurrentExclusiveQueueClaim_NoDoubleOwner(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("excl-race-q", false, false, true, nil)

	var wg sync.WaitGroup
	winners := make(chan string, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			connID := "conn-" + string(rune('A'+idx))
			if broker.SetQueueOwnerIfFree("excl-race-q", connID) {
				winners <- connID
			}
		}(i)
	}
	wg.Wait()
	close(winners)

	winnerCount := 0
	for range winners {
		winnerCount++
	}
	assert.Equal(t, 1, winnerCount, "exactly one connection should win the exclusive queue claim")
}

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
	broker.BindQueue("hdr-q1", "hdr-fanout", "", nil)
	broker.BindQueue("hdr-q2", "hdr-fanout", "", nil)

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

	// Retrieve through the REAL get path (GetMessageForGet), not direct
	// storage access. This exercises queueState cursor walk + storage.GetMessage.
	msg1, _, _, err := broker.GetMessageForGet("hdr-q1", true)
	require.NoError(t, err)
	require.NotNil(t, msg1)

	msg2, _, _, err := broker.GetMessageForGet("hdr-q2", true)
	require.NoError(t, err)
	require.NotNil(t, msg2)

	// Mutating one queue's retrieved headers must not affect the other.
	// This verifies the fanout deep-copy invariant in PublishMessage.
	msg1.Headers["key1"] = "MUTATED"

	assert.NotEqual(t, "MUTATED", msg2.Headers["key1"],
		"mutating one queue's headers should not affect other queues")
	assert.Equal(t, "val1", msg2.Headers["key1"])
}

// TestPoolRecycle_StoredBodyHeadersSurviveRealPoolRecycle verifies the
// load-bearing aliasing invariant: processCompleteMessage constructs a
// Message whose Body and Headers are zero-copy aliases to the PendingMessage's
// Body and ContentHeader.Headers. After PublishMessage stores the message,
// the caller recycles the pool objects via PutPendingMessage / PutContentHeader
// / PutBasicPublishMethod. Those functions MUST only nil references — never
// recycle backing []byte or map storage — or the stored delivery is corrupted.
//
// This test goes through the REAL pool Get/Put path (not manual struct
// construction) and retrieves via GetMessageForGet (not direct storage access).
func TestPoolRecycle_StoredBodyHeadersSurviveRealPoolRecycle(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("pool-q", false, false, false, nil)

	// 1. Acquire pool-managed objects exactly like handleBasicPublish +
	//    processHeaderFrame + processBodyFrame do.
	pendingMsg := protocol.GetPendingMessage()
	pendingMsg.Method = protocol.GetBasicPublishMethod()
	pendingMsg.Method.Exchange = ""
	pendingMsg.Method.RoutingKey = "pool-q"

	pendingMsg.Header = protocol.GetContentHeader()
	pendingMsg.Header.ContentType = "text/plain"
	pendingMsg.Header.Headers = map[string]interface{}{
		"h":      "v",
		"score":  int32(99),
		"flag":   true,
		"items":  []interface{}{"a", "b"},
		"nested": map[string]interface{}{"inner": "deep"},
	}
	pendingMsg.Body = []byte("survive-recycle")
	pendingMsg.Received = uint64(len(pendingMsg.Body))
	pendingMsg.BodySize = pendingMsg.Received

	// 2. Construct the aliased Message — this mirrors processCompleteMessage
	//    in server/basic_handlers.go exactly (zero-copy Body + Headers).
	message := &protocol.Message{
		Body:            pendingMsg.Body,
		Headers:         pendingMsg.Header.Headers,
		Exchange:        pendingMsg.Method.Exchange,
		RoutingKey:      pendingMsg.Method.RoutingKey,
		ContentType:     pendingMsg.Header.ContentType,
		ContentEncoding: pendingMsg.Header.ContentEncoding,
		DeliveryMode:    pendingMsg.Header.DeliveryMode,
		Priority:        pendingMsg.Header.Priority,
		CorrelationID:   pendingMsg.Header.CorrelationID,
		ReplyTo:         pendingMsg.Header.ReplyTo,
		Expiration:      pendingMsg.Header.Expiration,
		MessageID:       pendingMsg.Header.MessageID,
		Timestamp:       pendingMsg.Header.Timestamp,
		Type:            pendingMsg.Header.Type,
		UserID:          pendingMsg.Header.UserID,
		AppID:           pendingMsg.Header.AppID,
		ClusterID:       pendingMsg.Header.ClusterID,
		Mandatory:       pendingMsg.Method.Mandatory,
	}

	// 3. Publish through the REAL broker path (PublishMessage → StoreMessage).
	err := broker.PublishMessage("", "pool-q", message)
	require.NoError(t, err)

	// 4. Recycle pool objects — this is the critical step. The real server
	//    code calls these immediately after processCompleteMessage returns.
	//    PutPendingMessage/PutContentHeader MUST only nil references, never
	//    recycle the backing []byte or map. If they did, the next pool Get
	//    would overwrite the stored delivery's Body/Headers.
	protocol.PutBasicPublishMethod(pendingMsg.Method)
	protocol.PutContentHeader(pendingMsg.Header)
	protocol.PutPendingMessage(pendingMsg)

	// 5. Retrieve through the REAL get path (GetMessageForGet → queueState
	//    cursor → storage.GetMessage), not direct storage access.
	retrieved, _, _, err := broker.GetMessageForGet("pool-q", true)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// 6. Assert the stored delivery was NOT corrupted by pool recycling.
	assert.Equal(t, "survive-recycle", string(retrieved.Body),
		"stored body should survive pool recycle")
	assert.Equal(t, "v", retrieved.Headers["h"],
		"stored headers should survive pool recycle")
	assert.Equal(t, int32(99), retrieved.Headers["score"],
		"stored header int32 value should survive pool recycle")
	assert.Equal(t, true, retrieved.Headers["flag"],
		"stored header bool value should survive pool recycle")
	assert.Equal(t, "text/plain", retrieved.ContentType,
		"stored content type should survive pool recycle")
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

func TestBrokerIsSupportedExchangeType(t *testing.T) {
	assert.True(t, isSupportedExchangeType("direct"))
	assert.True(t, isSupportedExchangeType("fanout"))
	assert.True(t, isSupportedExchangeType("topic"))
	assert.True(t, isSupportedExchangeType("headers"))
	assert.False(t, isSupportedExchangeType(""))
	assert.False(t, isSupportedExchangeType("redis"))
}

func TestDeclareExchangeRejectsUnsupportedType(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	err := broker.DeclareExchange("bad-type-ex", "redis", false, false, false, nil)
	assert.Error(t, err, "broker should reject unsupported exchange type")

	_, err = broker.storage.GetExchange("bad-type-ex")
	assert.Error(t, err, "unsupported exchange should not be persisted")
}

func TestDeclareExchangeTypeMismatchReturnsSentinelError(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	err := broker.DeclareExchange("mismatch-ex", "direct", false, false, false, nil)
	require.NoError(t, err)

	err = broker.DeclareExchange("mismatch-ex", "fanout", false, false, false, nil)
	assert.ErrorIs(t, err, ErrExchangeTypeMismatch)
}

func TestGetQueueConsumerTags(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("tags-q", false, false, false, nil)

	tags := broker.GetQueueConsumerTags("tags-q")
	assert.Empty(t, tags)

	c1 := &protocol.Consumer{
		Tag:           "tag1",
		Queue:         "tags-q",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err := broker.RegisterConsumer("tags-q", "tag1", c1)
	require.NoError(t, err)

	c2 := &protocol.Consumer{
		Tag:           "tag2",
		Queue:         "tags-q",
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	err = broker.RegisterConsumer("tags-q", "tag2", c2)
	require.NoError(t, err)

	tags = broker.GetQueueConsumerTags("tags-q")
	assert.ElementsMatch(t, []string{"tag1", "tag2"}, tags)

	broker.UnregisterConsumer("tag1")
	broker.UnregisterConsumer("tag2")
}
