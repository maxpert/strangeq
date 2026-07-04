package server

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spyMetricsCollector struct {
	acks           atomic.Int64
	rejects        atomic.Int64
	redeliver      atomic.Int64
	txStart        atomic.Int64
	txCommit       atomic.Int64
	txRoll         atomic.Int64
	published      atomic.Int64
	delivered      atomic.Int64
	unroutable     atomic.Int64
	connErrors     atomic.Int64
	chanErrors     atomic.Int64
	consumersTotal atomic.Int64
}

func (s *spyMetricsCollector) RecordConnectionCreated() {}
func (s *spyMetricsCollector) RecordConnectionClosed()  {}
func (s *spyMetricsCollector) RecordChannelCreated()    {}
func (s *spyMetricsCollector) RecordChannelClosed()     {}
func (s *spyMetricsCollector) RecordQueueDeclared()     {}
func (s *spyMetricsCollector) RecordQueueDeleted()      {}
func (s *spyMetricsCollector) UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int) {
}
func (s *spyMetricsCollector) DeleteQueueMetrics(queueName, vhost string) {}
func (s *spyMetricsCollector) RecordExchangeDeclared()                    {}
func (s *spyMetricsCollector) RecordExchangeDeleted()                     {}
func (s *spyMetricsCollector) RecordMessagePublished(size int)            { s.published.Add(1) }
func (s *spyMetricsCollector) RecordMessageDelivered(size int)            { s.delivered.Add(1) }
func (s *spyMetricsCollector) RecordMessageAcknowledged()                 { s.acks.Add(1) }
func (s *spyMetricsCollector) RecordMessageRedelivered()                  { s.redeliver.Add(1) }
func (s *spyMetricsCollector) RecordMessageRejected()                     { s.rejects.Add(1) }
func (s *spyMetricsCollector) RecordMessageUnroutable()                   { s.unroutable.Add(1) }
func (s *spyMetricsCollector) SetConsumersTotal(count int)                { s.consumersTotal.Store(int64(count)) }
func (s *spyMetricsCollector) RecordTransactionStarted()                  { s.txStart.Add(1) }
func (s *spyMetricsCollector) RecordTransactionCommitted()                { s.txCommit.Add(1) }
func (s *spyMetricsCollector) RecordTransactionRolledback()               { s.txRoll.Add(1) }
func (s *spyMetricsCollector) UpdateServerUptime(seconds float64)         {}
func (s *spyMetricsCollector) RecordPublishLatency(duration float64)      {}
func (s *spyMetricsCollector) RecordDeliveryLatency(duration float64)     {}
func (s *spyMetricsCollector) UpdateMessageAge(queueName, vhost string, ageSeconds float64) {
}
func (s *spyMetricsCollector) DeleteMessageAge(queueName, vhost string)       {}
func (s *spyMetricsCollector) UpdateDiskMetrics(freeBytes, usedBytes float64) {}
func (s *spyMetricsCollector) UpdateWALSize(bytes float64)                    {}
func (s *spyMetricsCollector) RecordWALWrite()                                {}
func (s *spyMetricsCollector) RecordWALFsync(duration float64)                {}
func (s *spyMetricsCollector) RecordWALWriteError()                           {}
func (s *spyMetricsCollector) UpdateSegmentMetrics(queueName string, count, sizeBytes float64) {
}
func (s *spyMetricsCollector) DeleteSegmentMetrics(queueName string) {}
func (s *spyMetricsCollector) RecordSegmentCompaction()              {}
func (s *spyMetricsCollector) RecordSegmentReadError()               {}
func (s *spyMetricsCollector) UpdateRingBufferUtilization(queueName string, utilization float64) {
}
func (s *spyMetricsCollector) DeleteRingBufferUtilization(queueName string)     {}
func (s *spyMetricsCollector) UpdateMemoryMetrics(usedBytes, heapBytes float64) {}
func (s *spyMetricsCollector) RecordGCPause(duration float64)                   {}
func (s *spyMetricsCollector) UpdateGoroutines(count float64)                   {}
func (s *spyMetricsCollector) UpdateFileDescriptors(count float64)              {}
func (s *spyMetricsCollector) RecordConnectionError(errorType string)           { s.connErrors.Add(1) }
func (s *spyMetricsCollector) RecordChannelError(errorType string)              { s.chanErrors.Add(1) }
func (s *spyMetricsCollector) RecordMessageDropped(queueName, reason string)    {}

func newServerWithSpy(t *testing.T, spy *spyMetricsCollector) *Server {
	t.Helper()
	srv := newTransactionTestServer(t)
	srv.MetricsCollector = spy
	if adapter, ok := srv.Broker.(*StorageBrokerAdapter); ok {
		adapter.broker.SetMetricsCollector(broker.MetricsCollector(spy))
	}
	return srv
}

func encodeBasicConsume(t *testing.T, queue, tag string, noAck bool) []byte {
	t.Helper()
	m := &protocol.BasicConsumeMethod{
		Queue:       queue,
		ConsumerTag: tag,
		NoAck:       noAck,
		Arguments:   map[string]interface{}{},
	}
	data, err := m.Serialize()
	require.NoError(t, err)
	return data
}

func encodeBasicCancel(t *testing.T, tag string, noWait bool) []byte {
	t.Helper()
	m := &protocol.BasicCancelMethod{
		ConsumerTag: tag,
		NoWait:      noWait,
	}
	data, err := m.Serialize()
	require.NoError(t, err)
	return data
}

func TestIsSupportedExchangeType(t *testing.T) {
	assert.True(t, isSupportedExchangeType("direct"))
	assert.True(t, isSupportedExchangeType("fanout"))
	assert.True(t, isSupportedExchangeType("topic"))
	assert.True(t, isSupportedExchangeType("headers"))
	assert.False(t, isSupportedExchangeType(""))
	assert.False(t, isSupportedExchangeType("unknown"))
	assert.False(t, isSupportedExchangeType("redis"))
}

func TestBasicPublishImmediateFalseAccepted(t *testing.T) {
	method := protocol.GetBasicPublishMethod()
	method.Exchange = ""
	method.RoutingKey = "test-q"
	method.Immediate = false
	assert.False(t, method.Immediate)
	protocol.PutBasicPublishMethod(method)
}

func TestMetricsAckFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-ack-q")

	publishMessageToQueue(t, srv, "metrics-ack-q", "ack-me")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "metrics-ack-q")

	err := srv.handleBasicAck(conn, 1, makeAckPayload(deliveryTag, false))
	require.NoError(t, err)

	assert.Equal(t, int64(1), spy.acks.Load(), "RecordMessageAcknowledged must fire through handleBasicAck")
}

func TestMetricsRejectFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-reject-q")

	publishMessageToQueue(t, srv, "metrics-reject-q", "reject-me")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "metrics-reject-q")

	err := srv.handleBasicReject(conn, 1, makeRejectPayload(deliveryTag, false))
	require.NoError(t, err)

	assert.Equal(t, int64(1), spy.rejects.Load(), "RecordMessageRejected must fire through handleBasicReject")
}

func TestMetricsNackFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-nack-q")

	publishMessageToQueue(t, srv, "metrics-nack-q", "nack-me")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "metrics-nack-q")

	err := srv.handleBasicNack(conn, 1, makeNackPayload(deliveryTag, false, false))
	require.NoError(t, err)

	assert.Equal(t, int64(1), spy.rejects.Load(), "RecordMessageRejected must fire through handleBasicNack")
}

func TestMetricsPublishedFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-pub-q")

	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "metrics-pub-q", "published"))
	require.NoError(t, err)

	assert.Equal(t, int64(1), spy.published.Load(), "RecordMessagePublished must fire through processCompleteMessage")
}

func TestMetricsTxSelectCommitFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-tx-commit-q")

	txSelect(t, srv, conn, frameCh, 1)
	assert.Equal(t, int64(1), spy.txStart.Load(), "RecordTransactionStarted must fire through handleTxSelect")

	txCommit(t, srv, conn, frameCh, 1)
	assert.Equal(t, int64(1), spy.txCommit.Load(), "RecordTransactionCommitted must fire through handleTxCommit")
}

func TestMetricsTxRollbackFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-tx-rollback-q")

	txSelect(t, srv, conn, frameCh, 1)
	assert.Equal(t, int64(1), spy.txStart.Load(), "RecordTransactionStarted must fire through handleTxSelect")

	txRollback(t, srv, conn, frameCh, 1)
	assert.Equal(t, int64(1), spy.txRoll.Load(), "RecordTransactionRolledback must fire through handleTxRollback")
}

func TestMetricsSetConsumersTotalFiresThroughRealHandler(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-consumers-q")

	err := srv.handleBasicConsume(conn, 1, encodeBasicConsume(t, "metrics-consumers-q", "test-consumer", false))
	require.NoError(t, err)
	drainFrame(t, frameCh)

	assert.Equal(t, int64(1), spy.consumersTotal.Load(), "SetConsumersTotal must be called after RegisterConsumer")

	err = srv.handleBasicCancel(conn, 1, encodeBasicCancel(t, "test-consumer", false))
	require.NoError(t, err)
	drainFrame(t, frameCh)

	assert.Equal(t, int64(0), spy.consumersTotal.Load(), "SetConsumersTotal must be called after UnregisterConsumer")
}

func TestMetricsRejectedAndRedeliveredThroughRealPath(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "metrics-redeliver-q")

	err := srv.handleBasicConsume(conn, 1, encodeBasicConsume(t, "metrics-redeliver-q", "test-redeliver", false))
	require.NoError(t, err)
	drainFrame(t, frameCh)

	ch, ok := conn.Channels.Load(uint16(1))
	require.True(t, ok)
	channel := ch.(*protocol.Channel)
	channel.Mutex.RLock()
	consumer := channel.Consumers["test-redeliver"]
	channel.Mutex.RUnlock()
	require.NotNil(t, consumer)

	publishMessageToQueue(t, srv, "metrics-redeliver-q", "redeliver-me")

	select {
	case delivery := <-consumer.Messages:
		assert.False(t, delivery.Redelivered, "first delivery must not be redelivered")
		err := srv.handleBasicReject(conn, 1, makeRejectPayload(delivery.DeliveryTag, true))
		require.NoError(t, err)
		assert.Equal(t, int64(1), spy.rejects.Load(), "RecordMessageRejected must fire through handleBasicReject")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}

	select {
	case delivery := <-consumer.Messages:
		assert.True(t, delivery.Redelivered, "second delivery must be redelivered")
		assert.Equal(t, int64(1), spy.redeliver.Load(), "RecordMessageRedelivered must fire through broker deliverMessage")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for redelivered message")
	}

	_ = srv.Broker.UnregisterConsumer("test-redeliver")
}

func TestMetricsConnectionErrorFiresOnFrameProcessingError(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)

	conn, _ := newPipeConnWithFrames(t)
	conn.FrameQueue = make(chan *protocol.Frame, 10)
	conn.AckQueue = make(chan *protocol.Frame, 10)
	conn.Done = make(chan struct{})

	badFrame := &protocol.Frame{
		Type:    protocol.FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x00, 0x00, 0x00},
	}
	conn.FrameQueue <- badFrame
	close(conn.FrameQueue)

	done := make(chan struct{})
	go func() {
		srv.processFrames(conn, done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processFrames did not exit")
	}

	assert.True(t, spy.connErrors.Load() >= 1, "RecordConnectionError must fire on frame processing error")
}

func TestConsumerCancelNotifyCapabilityEnabled(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	err := srv.sendConnectionStart(conn)
	require.NoError(t, err)

	classID, methodID, payload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(10), classID)
	assert.Equal(t, uint16(10), methodID)

	startMethod, err := protocol.ParseConnectionStart(payload)
	require.NoError(t, err)

	caps, ok := startMethod.ServerProperties["capabilities"]
	require.True(t, ok, "capabilities should be present")
	capsMap, ok := caps.(map[string]interface{})
	require.True(t, ok, "capabilities should be a map")
	val, ok := capsMap["consumer_cancel_notify"]
	require.True(t, ok, "consumer_cancel_notify should be present")
	assert.True(t, val.(bool), "consumer_cancel_notify should be true")
}

func TestSendBasicCancelSendsMethodFrame60_30(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	err := srv.sendBasicCancel(conn, 1, "my-consumer")
	require.NoError(t, err)

	classID, methodID, payload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(30), methodID)

	cancelMethod := &protocol.BasicCancelMethod{}
	err = cancelMethod.Deserialize(payload)
	require.NoError(t, err)
	assert.Equal(t, "my-consumer", cancelMethod.ConsumerTag)
	assert.True(t, cancelMethod.NoWait)
}

func TestServerInitiatedBasicCancelOnQueueDelete(t *testing.T) {
	srv := newTransactionTestServer(t)

	connA, frameChA := newPipeConnWithFrames(t)
	connA.ID = "conn-a"
	connB, frameChB := newPipeConnWithFrames(t)
	connB.ID = "conn-b"

	srv.Mutex.Lock()
	srv.Connections[connA.ID] = connA
	srv.Connections[connB.ID] = connB
	srv.Mutex.Unlock()

	setupQueueAndChannel(t, srv, connA, frameChA, "cancel-notify-q")

	err := srv.handleBasicConsume(connA, 1, encodeBasicConsume(t, "cancel-notify-q", "test-consumer", false))
	require.NoError(t, err)
	drainFrame(t, frameChA)

	chB := protocol.NewChannel(1, connB)
	connB.Channels.Store(uint16(1), chB)

	deletePayload, err := (&protocol.QueueDeleteMethod{Queue: "cancel-notify-q"}).Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDelete(connB, 1, deletePayload)
	require.NoError(t, err)
	drainFrame(t, frameChB)

	classID, methodID, payload := nextMethodFrame(t, frameChA)
	assert.Equal(t, uint16(60), classID, "expected basic class")
	assert.Equal(t, uint16(30), methodID, "expected basic.cancel method")

	cancelMethod := &protocol.BasicCancelMethod{}
	err = cancelMethod.Deserialize(payload)
	require.NoError(t, err)
	assert.Equal(t, "test-consumer", cancelMethod.ConsumerTag)
	assert.True(t, cancelMethod.NoWait)
}

func TestExchangeRedeclareTypeMismatchSendsChannelClose406(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declarePayload, err := (&protocol.ExchangeDeclareMethod{
		Exchange:  "type-mismatch-ex",
		Type:      "direct",
		Arguments: map[string]interface{}{},
	}).Serialize()
	require.NoError(t, err)
	err = srv.handleExchangeDeclare(conn, 1, declarePayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	redeclarePayload, err := (&protocol.ExchangeDeclareMethod{
		Exchange:  "type-mismatch-ex",
		Type:      "fanout",
		Arguments: map[string]interface{}{},
	}).Serialize()
	require.NoError(t, err)
	err = srv.handleExchangeDeclare(conn, 1, redeclarePayload)
	require.NoError(t, err, "type mismatch should return nil (channel close, not connection close)")

	classID, methodID, payload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID, "expected channel class")
	assert.Equal(t, uint16(40), methodID, "expected channel.close method")
	require.GreaterOrEqual(t, len(payload), 2)
	replyCode := binary.BigEndian.Uint16(payload[:2])
	assert.Equal(t, uint16(406), replyCode, "expected 406 PreconditionFailed")
}
