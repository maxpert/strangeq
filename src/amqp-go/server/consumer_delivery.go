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

// consumerDeliveryLoop continuously reads from consumer channels and sends
// messages to clients. Uses a fan-in channel pattern: each consumer has a
// forwarding goroutine that sends deliveries to a shared channel, replacing
// the O(N) reflect.Select approach with a simple O(1) channel select.
func (s *Server) consumerDeliveryLoop(conn *protocol.Connection, done chan struct{}) {
	defer close(done)
	s.Log.Debug("Starting consumer delivery loop", zap.String("connection_id", conn.ID))

	// Track consumer info and forwarding goroutine stop channels.
	// These maps are only accessed by this goroutine — no locks needed.
	consumerInfos := make(map[string]*consumerInfo)
	forwarders := make(map[string]chan struct{})

	// Fan-in channel: all forwarding goroutines send deliveries here.
	// Buffered to avoid blocking forwarders under burst load.
	const fanInBufferSize = 1000
	fanIn := make(chan *protocol.Delivery, fanInBufferSize)

	// Configurable timeout and batch size from EngineConfig (wired, not hardcoded)
	selectTimeout := time.Duration(s.Config.Engine.ConsumerSelectTimeoutMS) * time.Millisecond
	if selectTimeout <= 0 {
		selectTimeout = 500 * time.Microsecond
	}
	maxBatchSize := s.Config.Engine.ConsumerMaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 100
	}

	// TIMER REUSE: Create timer once and reuse to avoid allocations
	timeout := time.NewTimer(selectTimeout)
	defer timeout.Stop()

	// stopAllForwarders stops all active forwarding goroutines.
	// Called on loop exit via defer to prevent goroutine leaks.
	stopAllForwarders := func() {
		for tag, stop := range forwarders {
			close(stop)
			delete(forwarders, tag)
		}
	}
	defer stopAllForwarders()

	for {
		// Check if connection is closed (atomic, lock-free)
		if conn.Closed.Load() {
			s.Log.Debug("Connection closed, stopping consumer delivery loop", zap.String("connection_id", conn.ID))
			return
		}

		// Discover consumers using sync.Map (concurrent-safe, lock-free reads).
		// Track active consumer tags to detect removals (basic.cancel, channel close).
		activeTags := make(map[string]struct{})
		conn.Channels.Range(func(key, value interface{}) bool {
			channel := value.(*protocol.Channel)
			channel.Mutex.RLock()
			for _, consumer := range channel.Consumers {
				activeTags[consumer.Tag] = struct{}{}
				if _, exists := consumerInfos[consumer.Tag]; !exists {
					consumerInfos[consumer.Tag] = &consumerInfo{channel: channel}
					// Start a forwarding goroutine for this consumer
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

		// Remove stale consumers (cancelled via basic.cancel, channel close, etc.)
		for tag := range consumerInfos {
			if _, active := activeTags[tag]; !active {
				close(forwarders[tag])
				delete(forwarders, tag)
				delete(consumerInfos, tag)
				s.Log.Debug("Removed stale consumer from delivery loop",
					zap.String("consumer_tag", tag))
			}
		}

		// If we have no consumers, wait a bit and continue
		if len(consumerInfos) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Reset timer for reuse (must drain if Stop() returned false)
		if !timeout.Stop() {
			select {
			case <-timeout.C:
			default:
			}
		}
		timeout.Reset(selectTimeout)

		// O(1) select: wait on the single fan-in channel or timeout
		select {
		case <-timeout.C:
			// Timeout — no messages available, continue to next iteration
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
					zap.Int("dropped_extras", len(extras)))
				return
			}

			// Process any extras from other consumers
			for _, extra := range extras {
				extraInfo, exists := consumerInfos[extra.ConsumerTag]
				if !exists {
					continue
				}
				err := s.sendBatchedDeliveries(conn, extraInfo.channel.ID, extra.ConsumerTag, []*protocol.Delivery{extra})
				if err != nil {
					s.Log.Error("Failed to send batched deliveries",
						zap.Error(err),
						zap.String("consumer_tag", extra.ConsumerTag),
						zap.Int("remaining_extras", len(extras)-1))
					return
				}
			}
		}
	}
}
