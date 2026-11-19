package clusterraft

import "context"

const captureJournalReplyBytes = 32 << 10

// ReadCaptured captures a quorum-fenced result under the Group lock. Large
// encoders use the node's existing reply slots and can then read immutable
// journal bytes outside the lock while Close waits for the captured result.
func (g *Group) ReadCaptured(ctx context.Context, capture func(Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	var encode func() ([]byte, error)
	var held chan struct{}
	inline, err := g.Read(ctx, func(machine Machine) ([]byte, error) {
		reply, size, err := capture(machine)
		if err != nil {
			return nil, err
		}
		if store, ok := g.store.(*journalGroupStore); ok && size >= captureJournalReplyBytes {
			slots := store.node.replySlots
			select {
			case slots <- struct{}{}:
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
