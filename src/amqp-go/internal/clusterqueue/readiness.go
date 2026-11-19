package clusterqueue

// QueueReady inspects applied state without reserving a delivery. The caller
// serializes it with application and snapshot restore.
func (m *Machine) QueueReady(queue string, incarnation QueueID) (bool, error) {
	if !validQueueName(queue) || incarnation == 0 {
		return false, ErrInvalidCommand
	}
	q := m.state.Queues[queue]
	if q == nil {
		return false, ErrQueueNotFound
	}
	if q.ID != incarnation {
		return false, ErrQueueIncarnation
	}
	return len(q.Ready) != 0, nil
}
