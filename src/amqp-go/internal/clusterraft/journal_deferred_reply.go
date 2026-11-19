package clusterraft

import (
	"errors"
	"fmt"
	"io"
)

const journalReplySlots = 4

// A deferred journal reply captures the result of one committed application.
// Its encoder may read immutable bytes through the Group's journal lifetime.
type deferredJournalReplyMachine interface {
	ApplyJournaledReply(data []byte, wantReply bool, source io.ReaderAt) ([]byte, func() ([]byte, error), error)
}

// completeCapturedReply runs under the Group lock. Once the waiter is removed,
// only its worker sends the committed reply, even if the caller cancels.
func (g *Group) completeCapturedReply(id key, store *journalGroupStore, encode func() ([]byte, error)) error {
	slots := store.node.replySlots
	select {
	case slots <- struct{}{}:
		unpin, err := g.pinJournalCapture()
		if err != nil {
			<-slots
			return err
		}
		waiter := g.pending[id]
		delete(g.pending, id)
		g.replyWorkers.Go(func() {
			defer func() { <-slots }()
			defer unpin()
			body, err := encode()
			if err != nil {
				g.mu.Lock()
				failure := g.failReplyEncoding(err)
				g.mu.Unlock()
				waiter <- response{err: failure}
				return
			}
			waiter <- response{body: body}
		})
		return nil
	default:
		body, err := encode()
		if err == nil {
			g.complete(id[:], body)
		}
		return err
	}
}

// failReplyEncoding records a local payload read failure under Group.mu.
func (g *Group) failReplyEncoding(err error) error {
	failure := fmt.Errorf("encode committed raft reply: %w", err)
	if g.terminal != nil {
		g.terminalCause = errors.Join(g.terminalCause, failure)
	} else {
		g.stop(failure)
	}
	return errors.Join(ErrLeadershipLost, failure)
}
