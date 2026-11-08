package server

import "github.com/maxpert/amqp-go/broker"

// QueueReadyCount reports a queue's READY depth (messages available for
// delivery, excluding delivered-but-unacked). Unknown queues report 0. It backs
// the telemetry queue poller without widening UnifiedBroker, and requires no
// changes outside this file.
//
// Seam S-6 (WS-B): the implementation delegates to
// broker.(*StorageBroker).GetQueueReadyCount, the same accessor queue.declare-ok
// already uses.
func (a *StorageBrokerAdapter) QueueReadyCount(name string) uint32 {
	if a == nil || a.broker == nil {
		return 0
	}
	return a.broker.GetQueueReadyCount(name)
}

// QueueDepthHighWater reports a queue's maximum observed depth (waiting +
// inflight) since it was created. Diagnostic only (Task 2); unknown queues
// report 0. It backs the telemetry poller's per-queue high-water gauge through
// the same optional-seam pattern as QueueReadyCount.
func (a *StorageBrokerAdapter) QueueDepthHighWater(name string) uint64 {
	if a == nil || a.broker == nil {
		return 0
	}
	return a.broker.GetQueueDepthHighWater(name)
}

// QueueMessageCounts reports a queue's ready/unacked/total depth (BOB-1). It is
// the optional-interface seam (queueMessageTotals) the management layer
// discovers on the injected UnifiedBroker; nil-safe like QueueReadyCount.
func (a *StorageBrokerAdapter) QueueMessageCounts(name string) broker.QueueMessageCounts {
	if a == nil || a.broker == nil {
		return broker.QueueMessageCounts{}
	}
	return a.broker.QueueMessageCounts(name)
}

// InflightCount reports the broker-wide total of delivered-but-unsettled
// messages. It is the optional seam the builder discovers on the injected
// UnifiedBroker to bind amqp_messages_inflight (spec 06 §3.5); nil-safe.
func (a *StorageBrokerAdapter) InflightCount() int64 {
	if a == nil || a.broker == nil {
		return 0
	}
	return a.broker.InflightCount()
}

// ConsumerViews returns one management view per live consumer (BOB-8), the
// optional-interface seam (consumerViews) the management layer discovers on the
// injected UnifiedBroker.
func (a *StorageBrokerAdapter) ConsumerViews() []broker.ConsumerView {
	if a == nil || a.broker == nil {
		return nil
	}
	return a.broker.ConsumerViews()
}

// ListBindings returns every binding in the vhost (BOB-4). It is the optional
// binding-lister seam the management layer discovers on the injected
// UnifiedBroker; nil-safe like the other accessors.
func (a *StorageBrokerAdapter) ListBindings() ([]broker.BindingView, error) {
	if a == nil || a.broker == nil {
		return nil, nil
	}
	return a.broker.ListBindings()
}

// SetEntityObserver installs the BOB-11 entity observer on the wrapped broker.
// It is the optional-interface seam the server's entity-counter wiring
// discovers; a broker without it leaves snapshot_coherent false.
func (a *StorageBrokerAdapter) SetEntityObserver(o broker.EntityObserver) {
	if a == nil || a.broker == nil {
		return
	}
	a.broker.SetEntityObserver(o)
}
