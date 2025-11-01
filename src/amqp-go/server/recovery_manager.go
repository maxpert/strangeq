package server

import (
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// RecoveryManager handles server startup recovery of durable entities
type RecoveryManager struct {
	storage interfaces.Storage
	broker  UnifiedBroker
	logger  *zap.Logger
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(storage interfaces.Storage, broker UnifiedBroker, logger *zap.Logger) *RecoveryManager {
	return &RecoveryManager{
		storage: storage,
		broker:  broker,
		logger:  logger,
	}
}

// PerformRecovery performs complete server startup recovery
func (r *RecoveryManager) PerformRecovery() (*protocol.RecoveryStats, error) {
	startTime := time.Now()

	r.logger.Info("Starting server recovery process...")

	// Step 1: Validate storage integrity and repair if needed
	stats, err := r.validateAndRepairStorage()
	if err != nil {
		return stats, fmt.Errorf("storage validation failed: %w", err)
	}

	// Step 2: Recover durable exchanges
	if err := r.recoverDurableExchanges(stats); err != nil {
		return stats, fmt.Errorf("exchange recovery failed: %w", err)
	}

	// Step 3: Recover durable queues
	if err := r.recoverDurableQueues(stats); err != nil {
		return stats, fmt.Errorf("queue recovery failed: %w", err)
	}

	// Step 4: Recover bindings
	if err := r.recoverBindings(stats); err != nil {
		return stats, fmt.Errorf("binding recovery failed: %w", err)
	}

	// Step 5: Recover persistent messages
	if err := r.recoverPersistentMessages(stats); err != nil {
		return stats, fmt.Errorf("message recovery failed: %w", err)
	}

	// Step 6: Recover pending acknowledgments
	if err := r.recoverPendingAcknowledgments(stats); err != nil {
		return stats, fmt.Errorf("acknowledgment recovery failed: %w", err)
	}

	stats.RecoveryDuration = time.Since(startTime)

	// Step 7: Mark recovery as complete
	if err := r.storage.MarkRecoveryComplete(stats); err != nil {
		r.logger.Warn("Failed to mark recovery complete", zap.Error(err))
	}

	r.logger.Info("Server recovery completed",
		zap.Duration("duration", stats.RecoveryDuration),
		zap.Int("exchanges_recovered", stats.DurableExchangesRecovered),
		zap.Int("queues_recovered", stats.DurableQueuesRecovered),
		zap.Int("bindings_recovered", stats.BindingsRecovered),
		zap.Int("messages_recovered", stats.PersistentMessagesRecovered),
		zap.Int("pending_acks_recovered", stats.PendingAcksRecovered),
		zap.Int("corrupted_entries_repaired", stats.CorruptedEntriesRepaired),
		zap.Strings("validation_errors", stats.ValidationErrors),
	)

	return stats, nil
}

// validateAndRepairStorage validates storage integrity and repairs corruption
func (r *RecoveryManager) validateAndRepairStorage() (*protocol.RecoveryStats, error) {
	r.logger.Info("Validating storage integrity...")

	// First validate storage integrity
	stats, err := r.storage.ValidateStorageIntegrity()
	if err != nil {
		return stats, err
	}

	// If validation errors were found, attempt auto-repair
	if len(stats.ValidationErrors) > 0 {
		r.logger.Warn("Storage validation errors found, attempting auto-repair",
			zap.Int("error_count", len(stats.ValidationErrors)))

		repairStats, err := r.storage.RepairCorruption(true) // auto-repair enabled
		if err != nil {
			return stats, err
		}

		// Merge repair stats
		stats.CorruptedEntriesRepaired = repairStats.CorruptedEntriesRepaired
		if len(repairStats.ValidationErrors) > 0 {
			stats.ValidationErrors = append(stats.ValidationErrors, repairStats.ValidationErrors...)
		}
	}

	return stats, nil
}

// recoverDurableExchanges recovers all durable exchanges from storage
func (r *RecoveryManager) recoverDurableExchanges(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering durable exchanges...")

	metadata, err := r.storage.GetDurableEntityMetadata()
	if err != nil {
		return err
	}

	for i := range metadata.Exchanges {
		exchange := &metadata.Exchanges[i]
		if exchange.Durable {
			err := r.broker.DeclareExchange(
				exchange.Name,
				exchange.Kind,
				exchange.Durable,
				exchange.AutoDelete,
				exchange.Internal,
				exchange.Arguments,
			)
			if err != nil {
				r.logger.Error("Failed to recover durable exchange",
					zap.String("exchange", exchange.Name),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to recover exchange %s: %v", exchange.Name, err))
			} else {
				stats.DurableExchangesRecovered++
				r.logger.Debug("Recovered durable exchange",
					zap.String("exchange", exchange.Name),
					zap.String("type", exchange.Kind))
			}
		}
	}

	return nil
}

// recoverDurableQueues recovers all durable queues from storage
func (r *RecoveryManager) recoverDurableQueues(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering durable queues...")

	metadata, err := r.storage.GetDurableEntityMetadata()
	if err != nil {
		return err
	}

	for i := range metadata.Queues {
		queue := &metadata.Queues[i]
		if queue.Durable {
			_, err := r.broker.DeclareQueue(
				queue.Name,
				queue.Durable,
				queue.AutoDelete,
				queue.Exclusive,
				queue.Arguments,
			)
			if err != nil {
				r.logger.Error("Failed to recover durable queue",
					zap.String("queue", queue.Name),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to recover queue %s: %v", queue.Name, err))
			} else {
				stats.DurableQueuesRecovered++
				r.logger.Debug("Recovered durable queue",
					zap.String("queue", queue.Name))
			}
		}
	}

	return nil
}

// recoverBindings recovers all bindings from storage
func (r *RecoveryManager) recoverBindings(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering bindings...")

	metadata, err := r.storage.GetDurableEntityMetadata()
	if err != nil {
		return err
	}

	for _, binding := range metadata.Bindings {
		err := r.broker.BindQueue(
			binding.Queue,
			binding.Exchange,
			binding.RoutingKey,
			binding.Arguments,
		)
		if err != nil {
			r.logger.Error("Failed to recover binding",
				zap.String("queue", binding.Queue),
				zap.String("exchange", binding.Exchange),
				zap.String("routing_key", binding.RoutingKey),
				zap.Error(err))
			stats.ValidationErrors = append(stats.ValidationErrors,
				fmt.Sprintf("Failed to recover binding %s->%s: %v", binding.Exchange, binding.Queue, err))
		} else {
			stats.BindingsRecovered++
			r.logger.Debug("Recovered binding",
				zap.String("queue", binding.Queue),
				zap.String("exchange", binding.Exchange),
				zap.String("routing_key", binding.RoutingKey))
		}
	}

	return nil
}

// recoverPersistentMessages recovers all persistent messages from storage
func (r *RecoveryManager) recoverPersistentMessages(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering persistent messages...")

	recoverableMessages, err := r.storage.GetRecoverableMessages()
	if err != nil {
		return err
	}

	for queueName, messages := range recoverableMessages {
		for _, message := range messages {
			// Only recover messages with DeliveryMode=2 (persistent)
			if message.DeliveryMode == 2 {
				// For recovery, we need to republish the message to ensure proper routing
				err := r.broker.PublishMessage(message.Exchange, message.RoutingKey, message)
				if err != nil {
					r.logger.Error("Failed to recover persistent message",
						zap.String("queue", queueName),
						zap.Uint64("delivery_tag", message.DeliveryTag),
						zap.Error(err))
					stats.ValidationErrors = append(stats.ValidationErrors,
						fmt.Sprintf("Failed to recover message in queue %s: %v", queueName, err))
				} else {
					stats.PersistentMessagesRecovered++
					r.logger.Debug("Recovered persistent message",
						zap.String("queue", queueName),
						zap.Uint64("delivery_tag", message.DeliveryTag))
				}
			}
		}
	}

	return nil
}

// recoverPendingAcknowledgments recovers all pending acknowledgments from storage
func (r *RecoveryManager) recoverPendingAcknowledgments(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering pending acknowledgments...")

	pendingAcks, err := r.storage.GetAllPendingAcks()
	if err != nil {
		return err
	}

	// Clean up expired acknowledgments (older than 1 hour)
	expiredCutoff := 1 * time.Hour
	err = r.storage.CleanupExpiredAcks(expiredCutoff)
	if err != nil {
		r.logger.Warn("Failed to cleanup expired acknowledgments", zap.Error(err))
	}

	// For each pending acknowledgment, we need to decide what to do
	for _, pendingAck := range pendingAcks {
		// Check if the acknowledgment is too old (potential zombie)
		age := time.Since(pendingAck.DeliveredAt)
		if age > expiredCutoff {
			// Remove expired acknowledgment
			err := r.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
			if err != nil {
				r.logger.Warn("Failed to delete expired pending ack",
					zap.String("queue", pendingAck.QueueName),
					zap.Uint64("delivery_tag", pendingAck.DeliveryTag),
					zap.Error(err))
			}
			continue
		}

		stats.PendingAcksRecovered++
		r.logger.Debug("Recovered pending acknowledgment",
			zap.String("queue", pendingAck.QueueName),
			zap.String("consumer", pendingAck.ConsumerTag),
			zap.Uint64("delivery_tag", pendingAck.DeliveryTag),
			zap.Duration("age", age))
	}

	return nil
}

// UpdateDurableEntityMetadata updates the durable entity metadata in storage
func (r *RecoveryManager) UpdateDurableEntityMetadata(exchanges map[string]*protocol.Exchange, queues map[string]*protocol.Queue) error {
	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []protocol.Exchange{},
		Queues:      []protocol.Queue{},
		Bindings:    []protocol.Binding{},
		LastUpdated: time.Now(),
	}

	// Collect durable exchanges
	for _, exchange := range exchanges {
		if exchange.Durable {
			metadata.Exchanges = append(metadata.Exchanges, exchange.Copy())

			// Collect bindings for this exchange
			for _, binding := range exchange.Bindings {
				metadata.Bindings = append(metadata.Bindings, *binding)
			}
		}
	}

	// Collect durable queues
	for _, queue := range queues {
		if queue.Durable {
			metadata.Queues = append(metadata.Queues, *queue)
		}
	}

	return r.storage.StoreDurableEntityMetadata(metadata)
}
