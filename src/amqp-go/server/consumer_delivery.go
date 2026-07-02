package server

import (
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// consumerInfo holds cached references for a consumer
type consumerInfo struct {
	channel *protocol.Channel
}

// forwardConsumerMessages runs in a per-consumer goroutine, forwarding
// deliveries from the consumer's Messages channel to the shared fan-in
// channel. Exits when the Messages channel is closed or stop is signaled.
// Sends *protocol.Delivery directly (no wrapper) to avoid per-message
// heap allocations — Delivery.ConsumerTag is used for routing.
func forwardConsumerMessages(msgChan chan *protocol.Delivery, fanIn chan<- *protocol.Delivery, stop <-chan struct{}) {
	for {
		select {
		case delivery, ok := <-msgChan:
			if !ok {
				return
			}
			select {
			case fanIn <- delivery:
			case <-stop:
				return
			}
		case <-stop:
			return
		}
	}
}

// discoverConsumers scans all channels on the connection for consumers,
// starting forwarder goroutines for new ones and stopping forwarders for
// consumers that have been removed. The activeTags map is reused across
// calls (cleared in-place) to avoid per-iteration heap allocations.
func (s *Server) discoverConsumers(
	conn *protocol.Connection,
	consumerInfos map[string]*consumerInfo,
	forwarders map[string]chan struct{},
	activeTags map[string]struct{},
	fanIn chan<- *protocol.Delivery,
) {
	for k := range activeTags {
		delete(activeTags, k)
	}

	conn.Channels.Range(func(key, value interface{}) bool {
		channel := value.(*protocol.Channel)
		channel.Mutex.RLock()
		for _, consumer := range channel.Consumers {
			activeTags[consumer.Tag] = struct{}{}
			if _, exists := consumerInfos[consumer.Tag]; !exists {
				consumerInfos[consumer.Tag] = &consumerInfo{channel: channel}
				stop := make(chan struct{})
				forwarders[consumer.Tag] = stop
				go forwardConsumerMessages(consumer.Messages, fanIn, stop)
				s.Log.Debug("Started forwarder for consumer",
					zap.String("consumer_tag", consumer.Tag),
					zap.String("queue", consumer.Queue))
			}
		}
		channel.Mutex.RUnlock()
		return true
	})

	for tag := range consumerInfos {
		if _, active := activeTags[tag]; !active {
			close(forwarders[tag])
			delete(forwarders, tag)
			delete(consumerInfos, tag)
			s.Log.Debug("Removed stale consumer from delivery loop",
				zap.String("consumer_tag", tag))
		}
	}
}

// consumerDeliveryLoop continuously reads from consumer channels and sends
// messages to clients. Uses a fan-in channel pattern: each consumer has a
// forwarding goroutine that sends deliveries to a shared channel, replacing
// the O(N) reflect.Select approach with a simple O(1) channel select.
//
// Consumer discovery is gated by an atomic dirty flag on the connection
// (ConsumersDirty). The loop only re-scans channels when the flag is set,
// avoiding O(channels × consumers) work every iteration. A periodic timeout
// acts as a safety net to self-heal if a flag-set is missed.
func (s *Server) consumerDeliveryLoop(conn *protocol.Connection, done chan struct{}) {
	defer close(done)
	s.Log.Debug("Starting consumer delivery loop", zap.String("connection_id", conn.ID))

	consumerInfos := make(map[string]*consumerInfo)
	forwarders := make(map[string]chan struct{})
	activeTags := make(map[string]struct{})

	const fanInBufferSize = 1000
	fanIn := make(chan *protocol.Delivery, fanInBufferSize)

	selectTimeout := time.Duration(s.Config.Engine.ConsumerSelectTimeoutMS) * time.Millisecond
	if selectTimeout <= 0 {
		selectTimeout = 500 * time.Microsecond
	}
	maxBatchSize := s.Config.Engine.ConsumerMaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 100
	}

	timeout := time.NewTimer(selectTimeout)
	defer timeout.Stop()

	stopAllForwarders := func() {
		for tag, stop := range forwarders {
			close(stop)
			delete(forwarders, tag)
		}
	}
	defer stopAllForwarders()

	for {
		if conn.Closed.Load() {
			s.Log.Debug("Connection closed, stopping consumer delivery loop", zap.String("connection_id", conn.ID))
			return
		}

		if conn.ConsumersDirty.Swap(false) {
			s.discoverConsumers(conn, consumerInfos, forwarders, activeTags, fanIn)
		}

		if len(consumerInfos) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if !timeout.Stop() {
			select {
			case <-timeout.C:
			default:
			}
		}
		timeout.Reset(selectTimeout)

		select {
		case <-timeout.C:
			conn.ConsumersDirty.Store(true)
			continue

		case delivery, ok := <-fanIn:
			if !ok {
				// Fan-in channel closed (shouldn't happen, but handle gracefully)
				return
			}

			info, exists := consumerInfos[delivery.ConsumerTag]
			if !exists {
				// Consumer was removed after forwarding — drop the delivery
				continue
			}

			// Try to collect more messages from the fan-in channel (non-blocking)
			batch := []*protocol.Delivery{delivery}
			extras := make([]*protocol.Delivery, 0, maxBatchSize-1)

		draining:
			for len(batch)+len(extras) < maxBatchSize {
				select {
				case extra, ok := <-fanIn:
					if !ok {
						break draining
					}
					if extra.ConsumerTag == delivery.ConsumerTag {
						batch = append(batch, extra)
					} else {
						extras = append(extras, extra)
					}
				default:
					break draining
				}
			}

			// Send the batch
			err := s.sendBatchedDeliveries(conn, info.channel.ID, delivery.ConsumerTag, batch)
			if err != nil {
				s.Log.Error("Failed to send batched deliveries",
					zap.Error(err),
					zap.String("consumer_tag", delivery.ConsumerTag),
					zap.Int("batch_size", len(batch)),
					zap.Int("extras", len(extras)))
				conn.Closed.Store(true)
				s.requeueFailedDeliveries(batch, extras)
				return
			}

			// Process any extras from other consumers
			for i, extra := range extras {
				extraInfo, exists := consumerInfos[extra.ConsumerTag]
				if !exists {
					continue
				}
				err := s.sendBatchedDeliveries(conn, extraInfo.channel.ID, extra.ConsumerTag, []*protocol.Delivery{extra})
				if err != nil {
					s.Log.Error("Failed to send batched deliveries",
						zap.Error(err),
						zap.String("consumer_tag", extra.ConsumerTag),
						zap.Int("remaining_extras", len(extras)-i))
					conn.Closed.Store(true)
					// Requeue only the failed extra and remaining unprocessed extras.
					// Already-sent extras (0..i-1) were written to TCP successfully.
					// Under at-least-once semantics, requeuing them would cause
					// duplicate delivery for data the client may have already received.
					s.requeueFailedDeliveries(nil, extras[i:])
					return
				}
			}
		}
	}
}

// requeueFailedDeliveries requeues batch and extra deliveries after a TCP
// write error. Each delivery is requeued via RejectMessage(consumerTag,
// deliveryTag, true). The deliveryIndex.LoadAndDelete guard in RejectMessage
// ensures no double-requeue with the connection teardown's UnregisterConsumer
// cleanup (step 7), which also uses LoadAndDelete on the same index.
func (s *Server) requeueFailedDeliveries(batch []*protocol.Delivery, extras []*protocol.Delivery) {
	for _, d := range batch {
		s.requeueSingleDelivery(d)
	}
	for _, d := range extras {
		s.requeueSingleDelivery(d)
	}
}

func (s *Server) requeueSingleDelivery(d *protocol.Delivery) {
	if d == nil {
		return
	}
	consumerTag, ok := s.Broker.GetConsumerForDelivery(d.DeliveryTag)
	if !ok {
		return
	}
	if err := s.Broker.RejectMessage(consumerTag, d.DeliveryTag, true); err != nil {
		s.Log.Warn("Failed to requeue delivery on write error",
			zap.Uint64("delivery_tag", d.DeliveryTag),
			zap.String("consumer_tag", consumerTag),
			zap.Error(err))
	}
}
