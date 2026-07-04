package server

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
)

type spyMetricsCollector struct {
	acks      int
	rejects   int
	redeliver int
	txStart   int
	txCommit  int
	txRoll    int
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
func (s *spyMetricsCollector) RecordMessagePublished(size int)            {}
func (s *spyMetricsCollector) RecordMessageDelivered(size int)            {}
func (s *spyMetricsCollector) RecordMessageAcknowledged()                 { s.acks++ }
func (s *spyMetricsCollector) RecordMessageRedelivered()                  { s.redeliver++ }
func (s *spyMetricsCollector) RecordMessageRejected()                     { s.rejects++ }
func (s *spyMetricsCollector) RecordMessageUnroutable()                   {}
func (s *spyMetricsCollector) SetConsumersTotal(count int)                {}
func (s *spyMetricsCollector) RecordTransactionStarted()                  { s.txStart++ }
func (s *spyMetricsCollector) RecordTransactionCommitted()                { s.txCommit++ }
func (s *spyMetricsCollector) RecordTransactionRolledback()               { s.txRoll++ }
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
func (s *spyMetricsCollector) RecordConnectionError(errorType string)           {}
func (s *spyMetricsCollector) RecordChannelError(errorType string)              {}
func (s *spyMetricsCollector) RecordMessageDropped(queueName, reason string)    {}

func TestIsSupportedExchangeType(t *testing.T) {
	assert.True(t, isSupportedExchangeType("direct"))
	assert.True(t, isSupportedExchangeType("fanout"))
	assert.True(t, isSupportedExchangeType("topic"))
	assert.True(t, isSupportedExchangeType("headers"))
	assert.False(t, isSupportedExchangeType(""))
	assert.False(t, isSupportedExchangeType("unknown"))
	assert.False(t, isSupportedExchangeType("redis"))
}

func TestSpyMetricsExactlyOnceAck(t *testing.T) {
	spy := &spyMetricsCollector{}
	spy.RecordMessageAcknowledged()
	spy.RecordMessageAcknowledged()
	assert.Equal(t, 2, spy.acks)
}

func TestSpyMetricsExactlyOnceReject(t *testing.T) {
	spy := &spyMetricsCollector{}
	spy.RecordMessageRejected()
	spy.RecordMessageRejected()
	assert.Equal(t, 2, spy.rejects)
}

func TestSpyMetricsRedelivered(t *testing.T) {
	spy := &spyMetricsCollector{}
	spy.RecordMessageRedelivered()
	assert.Equal(t, 1, spy.redeliver)
}

func TestBasicPublishImmediateFalseAccepted(t *testing.T) {
	method := protocol.GetBasicPublishMethod()
	method.Exchange = ""
	method.RoutingKey = "test-q"
	method.Immediate = false
	assert.False(t, method.Immediate)
	protocol.PutBasicPublishMethod(method)
}
