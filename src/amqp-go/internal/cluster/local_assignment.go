package cluster

import (
	"errors"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
)

// localAssignmentResult stays inside one route operation. Actual peer replies
// remain the unchanged portable byte frame; it is never encoded or forwarded.
type localAssignmentResult struct {
	page      *clusterqueue.BorrowedAssignmentPage
	domainErr error
}

func (r *localAssignmentResult) release() {
	if r != nil && r.page != nil {
		r.page.Release()
		r.page = nil
	}
	if r != nil {
		r.domainErr = nil
	}
}

// These are the same queue-result errors carried in the portable page header.
// Capture/encoder storage failures must stay on ReadCaptured's terminal path.
func assignmentResultError(err error) bool {
	return errors.Is(err, clusterqueue.ErrInvalidCommand) || errors.Is(err, clusterqueue.ErrQueueNotFound) ||
		errors.Is(err, clusterqueue.ErrQueueIncarnation) || errors.Is(err, clusterqueue.ErrSessionClosed) ||
		errors.Is(err, clusterqueue.ErrUnknownSession) || errors.Is(err, clusterqueue.ErrSessionOwner) ||
		errors.Is(err, clusterqueue.ErrUnknownAttempt) || errors.Is(err, clusterqueue.ErrIDsExhausted) ||
		errors.Is(err, clusterqueue.ErrCapacity)
}
