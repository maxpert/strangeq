package server

// MetricsCollector defines the interface for metrics collection
type MetricsCollector interface {
	// Connection metrics
	RecordConnectionCreated()
	RecordConnectionClosed()

	// Channel metrics
	RecordChannelCreated()
	RecordChannelClosed()

	// Queue metrics
	RecordQueueDeclared()
	RecordQueueDeleted()
	UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int)
	DeleteQueueMetrics(queueName, vhost string)

	// Exchange metrics
	RecordExchangeDeclared()
	RecordExchangeDeleted()

	// Message metrics
	RecordMessagePublished(size int)
	RecordMessageDelivered(size int)
	RecordMessageAcknowledged()
	RecordMessageRedelivered()
	RecordMessageRejected()
	RecordMessageUnroutable()

	// Consumer metrics
	SetConsumersTotal(count int)

	// Transaction metrics
	RecordTransactionStarted()
	RecordTransactionCommitted()
	RecordTransactionRolledback()

	// Server metrics
	UpdateServerUptime(seconds float64)
}

// NoOpMetricsCollector is a metrics collector that does nothing
type NoOpMetricsCollector struct{}

func (n *NoOpMetricsCollector) RecordConnectionCreated() {}
func (n *NoOpMetricsCollector) RecordConnectionClosed()  {}
func (n *NoOpMetricsCollector) RecordChannelCreated()    {}
func (n *NoOpMetricsCollector) RecordChannelClosed()     {}
func (n *NoOpMetricsCollector) RecordQueueDeclared()     {}
func (n *NoOpMetricsCollector) RecordQueueDeleted()      {}
func (n *NoOpMetricsCollector) UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int) {
}
func (n *NoOpMetricsCollector) DeleteQueueMetrics(queueName, vhost string) {}
func (n *NoOpMetricsCollector) RecordExchangeDeclared()                    {}
func (n *NoOpMetricsCollector) RecordExchangeDeleted()                     {}
func (n *NoOpMetricsCollector) RecordMessagePublished(size int)            {}
func (n *NoOpMetricsCollector) RecordMessageDelivered(size int)            {}
func (n *NoOpMetricsCollector) RecordMessageAcknowledged()                 {}
func (n *NoOpMetricsCollector) RecordMessageRedelivered()                  {}
func (n *NoOpMetricsCollector) RecordMessageRejected()                     {}
func (n *NoOpMetricsCollector) RecordMessageUnroutable()                   {}
func (n *NoOpMetricsCollector) SetConsumersTotal(count int)                {}
func (n *NoOpMetricsCollector) RecordTransactionStarted()                  {}
func (n *NoOpMetricsCollector) RecordTransactionCommitted()                {}
func (n *NoOpMetricsCollector) RecordTransactionRolledback()               {}
func (n *NoOpMetricsCollector) UpdateServerUptime(seconds float64)         {}
