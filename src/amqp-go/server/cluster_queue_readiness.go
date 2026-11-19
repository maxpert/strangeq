package server

import "github.com/maxpert/amqp-go/internal/clusterqueue"

// QueueReady exposes an advisory read for the applied-state wait.
func (m *ClusterMachine) QueueReady(queue string, incarnation clusterqueue.QueueID) (bool, error) {
	return m.queues.QueueReady(queue, incarnation)
}

func (m *ClusterMachine) AssignmentReady(request clusterqueue.AssignmentRequest) (bool, error) {
	return m.queues.AssignmentReady(request)
}

func (m *ClusterMachine) CaptureAssignmentPage(request clusterqueue.AssignmentRequest) (func() ([]byte, error), int, error) {
	return m.queues.CaptureAssignmentPage(request)
}

func (m *ClusterMachine) CaptureAssignmentReplica(request clusterqueue.AssignmentRequest, coverage clusterqueue.AssignmentCoverage) (func() ([]byte, error), int, error) {
	return m.queues.CaptureAssignmentReplica(request, coverage)
}
