package server

import (
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// consumerInfo holds cached references for a consumer, resolved from the
// broker-internal consumer identity that every Delivery carries.
type consumerInfo struct {
	channel *protocol.Channel
	// tag is the CLIENT-VISIBLE consumer tag to stamp on basic.deliver frames
	// for this consumer. It is cached here, on the identity-keyed routing
	// entry, rather than carried on every Delivery: Delivery already carries
	// the identity it is routed by, so keeping the wire tag here costs one
	// string per consumer instead of one per message, and the per-delivery
	// struct is unchanged in size.
	tag string
}

// forwardConsumerMessages runs in a per-consumer goroutine, forwarding
// deliveries from the consumer's Messages channel to the shared fan-in
// channel. Exits when the Messages channel is closed or stop is signaled.
// Sends *protocol.Delivery directly (no wrapper) to avoid per-message
// heap allocations — Delivery.ConsumerID is what the fan-in loop routes by,
// and the client-visible wire tag is read once per consumer off consumerInfo.tag.
//
// Flow control: before forwarding, a single atomic load of channel.FlowActive
// gates the send. When flow is inactive the goroutine parks on FlowWake until
// flow resumes or stop closes (zero overhead in the common active case).
func forwardConsumerMessages(channel *protocol.Channel, msgChan chan *protocol.Delivery, fanIn chan<- *protocol.Delivery, stop <-chan struct{}) {
	for {
		select {
		case delivery, ok := <-msgChan:
			if !ok {
				return
			}
			for !channel.FlowActive.Load() {
				select {
				case <-channel.FlowWake:
				case <-stop:
					return
				}
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
// consumers that have been removed. The activeIDs map is reused across
// calls (cleared in-place) to avoid per-iteration heap allocations.
//
// Keyed by the broker-internal consumer identity, NOT the wire tag: a
// connection may hold two channels whose consumers legitimately present the
// same tag (AMQP 0-9-1 scopes the tag per channel), and a tag-keyed table
// collapses them into one entry — one forwarder never started, one consumer's
// deliveries written to the other's channel.
func (s *Server) discoverConsumers(
	rt *connRuntime,
	consumerInfos map[string]*consumerInfo,
	forwarders map[string]chan struct{},
	activeIDs map[string]struct{},
	fanIn chan<- *protocol.Delivery,
) {
	conn := rt.conn
	for k := range activeIDs {
		delete(activeIDs, k)
	}

	conn.Channels.Range(func(key, value interface{}) bool {
		channel := value.(*protocol.Channel)
		channel.Mutex.RLock()
		for _, consumer := range channel.Consumers {
			activeIDs[consumer.ID] = struct{}{}
			if _, exists := consumerInfos[consumer.ID]; !exists {
				consumerInfos[consumer.ID] = &consumerInfo{channel: channel, tag: consumer.Tag}
				stop := make(chan struct{})
				forwarders[consumer.ID] = stop
				s.goConn(rt, "forwarder", func() {
					forwardConsumerMessages(channel, consumer.Messages, fanIn, stop)
				})
				s.Log.Debug("Started forwarder for consumer",
					zap.String("consumer_tag", consumer.Tag),
					zap.String("consumer_id", consumer.ID),
					zap.String("queue", consumer.Queue))
			}
		}
		channel.Mutex.RUnlock()
		return true
	})

	for id := range consumerInfos {
		if _, active := activeIDs[id]; !active {
			close(forwarders[id])
			delete(forwarders, id)
			delete(consumerInfos, id)
			s.Log.Debug("Removed stale consumer from delivery loop",
				zap.String("consumer_id", id))
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
func (s *Server) consumerDeliveryLoop(rt *connRuntime) {
	conn := rt.conn
	s.Log.Debug("Starting consumer delivery loop", zap.String("connection_id", conn.ID))

	consumerInfos := make(map[string]*consumerInfo)
	forwarders := make(map[string]chan struct{})
	activeIDs := make(map[string]struct{})

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
		for id, stop := range forwarders {
			close(stop)
			delete(forwarders, id)
		}
	}
	defer stopAllForwarders()

	batch := make([]*protocol.Delivery, 0, maxBatchSize)
	extras := make([]*protocol.Delivery, 0, maxBatchSize)

	for {
		if conn.Closed.Load() {
			s.Log.Debug("Connection closed, stopping consumer delivery loop", zap.String("connection_id", conn.ID))
			return
		}
		select {
		case <-conn.Done:
			return
		default:
		}

		if conn.ConsumersDirty.Swap(false) {
			s.discoverConsumers(rt, consumerInfos, forwarders, activeIDs, fanIn)
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

			info, exists := consumerInfos[delivery.ConsumerID]
			if !exists {
				// Consumer was removed after forwarding — drop the delivery
				continue
			}

			// Try to collect more messages from the fan-in channel (non-blocking)
			batch = batch[:0]
			batch = append(batch, delivery)
			extras = extras[:0]

		draining:
			for len(batch)+len(extras) < maxBatchSize {
				select {
				case extra, ok := <-fanIn:
					if !ok {
						break draining
					}
					if extra.ConsumerID == delivery.ConsumerID {
						batch = append(batch, extra)
					} else {
						extras = append(extras, extra)
					}
				default:
					break draining
				}
			}

			// Send the batch. The wire tag comes from the routing entry, not
			// from the delivery: the delivery carries the internal identity.
			err := s.sendBatchedDeliveries(conn, info.channel.ID, info.tag, batch)
			if err != nil {
				s.Log.Error("Failed to send batched deliveries",
					zap.Error(err),
					zap.String("consumer_tag", info.tag),
					zap.Int("batch_size", len(batch)),
					zap.Int("extras", len(extras)))
				conn.Closed.Store(true)
				s.requeueFailedDeliveries(batch, extras)
				return
			}

			// Process any extras from other consumers
			for i, extra := range extras {
				extraInfo, exists := consumerInfos[extra.ConsumerID]
				if !exists {
					continue
				}
				err := s.sendBatchedDeliveries(conn, extraInfo.channel.ID, extraInfo.tag, []*protocol.Delivery{extra})
				if err != nil {
					s.Log.Error("Failed to send batched deliveries",
						zap.Error(err),
						zap.String("consumer_tag", extraInfo.tag),
						zap.Int("remaining_extras", len(extras)-i))
					conn.Closed.Store(true)
					s.requeueFailedDeliveries(nil, extras[i:])
					return
				}
			}

			// Clear references to avoid retaining Delivery pointers
			for i := range batch {
				batch[i] = nil
			}
			for i := range extras {
				extras[i] = nil
			}
		}
	}
}

// requeueFailedDeliveries requeues batch and extra deliveries after a TCP
// write error. Each delivery is requeued via RejectMessage(consumerID,
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
	consumerID, ok := s.Broker.GetConsumerForDelivery(d.DeliveryTag)
	if !ok {
		return
	}
	if err := s.Broker.RejectMessage(consumerID, d.DeliveryTag, true); err != nil {
		s.Log.Warn("Failed to requeue delivery on write error",
			zap.Uint64("delivery_tag", d.DeliveryTag),
			zap.String("consumer_id", consumerID),
			zap.Error(err))
	}
}
