package server

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
