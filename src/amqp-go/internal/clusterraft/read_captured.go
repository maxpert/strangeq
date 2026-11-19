package clusterraft

import "context"

const captureJournalReplyBytes = 32 << 10

// ReadCaptured captures a quorum-fenced result under the Group lock. Large
// encoders use the node's existing reply slots and can then read immutable
// journal bytes outside the lock while Close waits for the captured result.
func (g *Group) ReadCaptured(ctx context.Context, capture func(Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	return g.readCaptured(ctx, g.Read, capture)
}

// CaptureAppliedLeader reads committed local state for an internal worker.
// The worker must obtain a quorum authorization before acting on this view.
func (g *Group) CaptureAppliedLeader(ctx context.Context, capture func(Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	return g.readCaptured(ctx, g.readAppliedLeader, capture)
}

func (g *Group) readAppliedLeader(ctx context.Context, query func(Machine) ([]byte, error)) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.stopped(); err != nil {
		return nil, err
	}
	if leader, _ := g.readQuorum(g.node.BasicStatus()); !leader {
		return nil, ErrNotLeader
	}
	return query(g.cfg.Machine)
}

func (g *Group) readCaptured(ctx context.Context,
	read func(context.Context, func(Machine) ([]byte, error)) ([]byte, error),
	capture func(Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	var encode func() ([]byte, error)
	var held chan struct{}
	var unpin func()
	inline, err := read(ctx, func(machine Machine) ([]byte, error) {
		reply, size, err := capture(machine)
		if err != nil {
			return nil, err
		}
		if store, ok := g.store.(*journalGroupStore); ok && size >= captureJournalReplyBytes {
			slots := store.node.replySlots
			select {
			case slots <- struct{}{}:
				unpin, err = g.pinJournalCapture()
				if err != nil {
					<-slots
					return nil, err
				}
				g.replyWorkers.Add(1)
				held, encode = slots, reply
				return nil, nil
			default:
			}
		}
		body, err := reply()
		if err != nil {
			return nil, g.failReplyEncoding(err)
		}
		return body, nil
	})
	if held != nil {
		defer func() {
			<-held
			unpin()
			g.replyWorkers.Done()
		}()
	}
	if err != nil || encode == nil {
		return inline, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := encode()
	if err != nil {
		g.mu.Lock()
		err = g.failReplyEncoding(err)
		g.mu.Unlock()
		return nil, err
	}
	return body, nil
}

// HasLocalWork evaluates a cheap, read-only scheduling predicate. Results are
// advisory and carry no quorum guarantee.
func (g *Group) HasLocalWork(pending func(Machine) bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopped() == nil && g.node.BasicStatus().Lead == g.cfg.ID && pending(g.cfg.Machine)
}
