package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector holds all Prometheus metrics for the AMQP server
type Collector struct {
	// Connection metrics
	ConnectionsTotal   prometheus.Gauge
	ConnectionsCreated prometheus.Counter
	ConnectionsClosed  prometheus.Counter

	// Channel metrics
	ChannelsTotal   prometheus.Gauge
	ChannelsCreated prometheus.Counter
	ChannelsClosed  prometheus.Counter

	// Queue metrics
	QueuesTotal          prometheus.Gauge
	QueuesDeclared       prometheus.Counter
	QueuesDeleted        prometheus.Counter
	QueueMessagesReady   *prometheus.GaugeVec
	QueueMessagesUnacked *prometheus.GaugeVec
	QueueMessagesTotal   *prometheus.GaugeVec
	QueueConsumers       *prometheus.GaugeVec

	// Exchange metrics
	ExchangesTotal    prometheus.Gauge
	ExchangesDeclared prometheus.Counter
	ExchangesDeleted  prometheus.Counter

	// Message metrics
	MessagesPublished      prometheus.Counter
	MessagesPublishedBytes prometheus.Counter
	MessagesDelivered      prometheus.Counter
	MessagesDeliveredBytes prometheus.Counter
	MessagesAcknowledged   prometheus.Counter
	MessagesRedelivered    prometheus.Counter
	MessagesRejected       prometheus.Counter
	MessagesUnroutable     prometheus.Counter

	// Consumer metrics
	ConsumersTotal prometheus.Gauge

	// Transaction metrics
	TransactionsStarted    prometheus.Counter
	TransactionsCommitted  prometheus.Counter
	TransactionsRolledback prometheus.Counter

	// Server metrics
	ServerUptime prometheus.Gauge
}

// NewCollector creates a new metrics collector with all Prometheus metrics
func NewCollector(namespace string) *Collector {
	if namespace == "" {
		namespace = "amqp"
	}

	return &Collector{
		// Connection metrics
		ConnectionsTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connections_total",
			Help:      "Current number of active connections",
		}),
		ConnectionsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_created_total",
			Help:      "Total number of connections created since server start",
		}),
		ConnectionsClosed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_closed_total",
			Help:      "Total number of connections closed since server start",
		}),

		// Channel metrics
		ChannelsTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "channels_total",
			Help:      "Current number of active channels",
		}),
		ChannelsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "channels_created_total",
			Help:      "Total number of channels created since server start",
		}),
		ChannelsClosed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "channels_closed_total",
			Help:      "Total number of channels closed since server start",
		}),

		// Queue metrics
		QueuesTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queues_total",
			Help:      "Current number of queues",
		}),
		QueuesDeclared: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queues_declared_total",
			Help:      "Total number of queues declared since server start",
		}),
		QueuesDeleted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queues_deleted_total",
			Help:      "Total number of queues deleted since server start",
		}),
		QueueMessagesReady: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_messages_ready",
			Help:      "Number of messages ready to be delivered in queue",
		}, []string{"queue", "vhost"}),
		QueueMessagesUnacked: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_messages_unacknowledged",
			Help:      "Number of messages delivered but not yet acknowledged in queue",
		}, []string{"queue", "vhost"}),
		QueueMessagesTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_messages_total",
			Help:      "Total number of messages in queue (ready + unacknowledged)",
		}, []string{"queue", "vhost"}),
		QueueConsumers: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_consumers",
			Help:      "Number of consumers on queue",
		}, []string{"queue", "vhost"}),

		// Exchange metrics
		ExchangesTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exchanges_total",
			Help:      "Current number of exchanges",
		}),
		ExchangesDeclared: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exchanges_declared_total",
			Help:      "Total number of exchanges declared since server start",
		}),
		ExchangesDeleted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exchanges_deleted_total",
			Help:      "Total number of exchanges deleted since server start",
		}),

		// Message metrics
		MessagesPublished: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_total",
			Help:      "Total number of messages published since server start",
		}),
		MessagesPublishedBytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_bytes_total",
			Help:      "Total bytes of messages published since server start",
		}),
		MessagesDelivered: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_delivered_total",
			Help:      "Total number of messages delivered to consumers since server start",
		}),
		MessagesDeliveredBytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_delivered_bytes_total",
			Help:      "Total bytes of messages delivered to consumers since server start",
		}),
		MessagesAcknowledged: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_acknowledged_total",
			Help:      "Total number of messages acknowledged since server start",
		}),
		MessagesRedelivered: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_redelivered_total",
			Help:      "Total number of messages redelivered since server start",
		}),
		MessagesRejected: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_rejected_total",
			Help:      "Total number of messages rejected since server start",
		}),
		MessagesUnroutable: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_unroutable_total",
			Help:      "Total number of unroutable messages since server start",
		}),

		// Consumer metrics
		ConsumersTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumers_total",
			Help:      "Current number of active consumers",
		}),

		// Transaction metrics
		TransactionsStarted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transactions_started_total",
			Help:      "Total number of transactions started since server start",
		}),
		TransactionsCommitted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transactions_committed_total",
			Help:      "Total number of transactions committed since server start",
		}),
		TransactionsRolledback: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "transactions_rolledback_total",
			Help:      "Total number of transactions rolled back since server start",
		}),

		// Server metrics
		ServerUptime: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "server_uptime_seconds",
			Help:      "Server uptime in seconds",
		}),
	}
}

// RecordConnectionCreated increments connection creation counter and total
func (c *Collector) RecordConnectionCreated() {
	c.ConnectionsCreated.Inc()
	c.ConnectionsTotal.Inc()
}

// RecordConnectionClosed increments connection close counter and decrements total
func (c *Collector) RecordConnectionClosed() {
	c.ConnectionsClosed.Inc()
	c.ConnectionsTotal.Dec()
}

// RecordChannelCreated increments channel creation counter and total
func (c *Collector) RecordChannelCreated() {
	c.ChannelsCreated.Inc()
	c.ChannelsTotal.Inc()
}

// RecordChannelClosed increments channel close counter and decrements total
func (c *Collector) RecordChannelClosed() {
	c.ChannelsClosed.Inc()
	c.ChannelsTotal.Dec()
}

// RecordQueueDeclared increments queue declaration counter and total
func (c *Collector) RecordQueueDeclared() {
	c.QueuesDeclared.Inc()
	c.QueuesTotal.Inc()
}

// RecordQueueDeleted increments queue deletion counter and decrements total
func (c *Collector) RecordQueueDeleted() {
	c.QueuesDeleted.Inc()
	c.QueuesTotal.Dec()
}

// RecordExchangeDeclared increments exchange declaration counter and total
func (c *Collector) RecordExchangeDeclared() {
	c.ExchangesDeclared.Inc()
	c.ExchangesTotal.Inc()
}

// RecordExchangeDeleted increments exchange deletion counter and decrements total
func (c *Collector) RecordExchangeDeleted() {
	c.ExchangesDeleted.Inc()
	c.ExchangesTotal.Dec()
}

// RecordMessagePublished records a published message
func (c *Collector) RecordMessagePublished(size int) {
	c.MessagesPublished.Inc()
	c.MessagesPublishedBytes.Add(float64(size))
}

// RecordMessageDelivered records a delivered message
func (c *Collector) RecordMessageDelivered(size int) {
	c.MessagesDelivered.Inc()
	c.MessagesDeliveredBytes.Add(float64(size))
}

// RecordMessageAcknowledged records an acknowledged message
func (c *Collector) RecordMessageAcknowledged() {
	c.MessagesAcknowledged.Inc()
}

// RecordMessageRedelivered records a redelivered message
func (c *Collector) RecordMessageRedelivered() {
	c.MessagesRedelivered.Inc()
}

// RecordMessageRejected records a rejected message
func (c *Collector) RecordMessageRejected() {
	c.MessagesRejected.Inc()
}

// RecordMessageUnroutable records an unroutable message
func (c *Collector) RecordMessageUnroutable() {
	c.MessagesUnroutable.Inc()
}

// UpdateQueueMetrics updates all metrics for a specific queue
func (c *Collector) UpdateQueueMetrics(queueName, vhost string, ready, unacked, consumers int) {
	labels := prometheus.Labels{"queue": queueName, "vhost": vhost}
	c.QueueMessagesReady.With(labels).Set(float64(ready))
	c.QueueMessagesUnacked.With(labels).Set(float64(unacked))
	c.QueueMessagesTotal.With(labels).Set(float64(ready + unacked))
	c.QueueConsumers.With(labels).Set(float64(consumers))
}

// DeleteQueueMetrics removes metrics for a deleted queue
func (c *Collector) DeleteQueueMetrics(queueName, vhost string) {
	labels := prometheus.Labels{"queue": queueName, "vhost": vhost}
	c.QueueMessagesReady.Delete(labels)
	c.QueueMessagesUnacked.Delete(labels)
	c.QueueMessagesTotal.Delete(labels)
	c.QueueConsumers.Delete(labels)
}

// SetConsumersTotal sets the total number of consumers
func (c *Collector) SetConsumersTotal(count int) {
	c.ConsumersTotal.Set(float64(count))
}

// RecordTransactionStarted increments transaction started counter
func (c *Collector) RecordTransactionStarted() {
	c.TransactionsStarted.Inc()
}

// RecordTransactionCommitted increments transaction committed counter
func (c *Collector) RecordTransactionCommitted() {
	c.TransactionsCommitted.Inc()
}

// RecordTransactionRolledback increments transaction rolled back counter
func (c *Collector) RecordTransactionRolledback() {
	c.TransactionsRolledback.Inc()
}

// UpdateServerUptime updates the server uptime metric
func (c *Collector) UpdateServerUptime(seconds float64) {
	c.ServerUptime.Set(seconds)
}
