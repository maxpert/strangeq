package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// handshakeSlot is one unit of the pre-auth pending-handshake budget (§2).
// Release is idempotent: the slot is returned exactly once, either when
// connection.open-ok has been sent or when handleConnection exits, whichever
// comes first.
type handshakeSlot struct {
	s    *Server
	once sync.Once
}

func (h *handshakeSlot) release() {
	if h == nil {
		return
	}
	h.once.Do(func() { h.s.pendingHandshakes.Add(-1) })
}

// connRuntime holds all server-side per-connection state that cannot live on
// protocol.Connection (which may only be extended in the FrameQueue region —
// WS-A owns the rest of protocol/). One connRuntime exists per accepted
// connection from handleConnection until finishConnection.
type connRuntime struct {
	conn *protocol.Connection
	slot *handshakeSlot

	wg       sync.WaitGroup // every per-connection goroutine (§13)
	torndown chan struct{}  // closed by finishConnection after cleanupConnection returns

	handshakeDone         atomic.Bool // connection.open-ok sent
	connClosing           atomic.Bool // server sent connection.close, awaiting close-ok
	closeHandshakeStarted atomic.Bool // either side started the connection close handshake
	closeTimer            atomic.Pointer[time.Timer]

	closingChannels sync.Map     // uint16 → struct{}: server sent channel.close, awaiting close-ok/close
	closingCount    atomic.Int32 // fast-path gate: 0 ⇒ no channel is closing

	liveGoroutines atomic.Int32 // test observability (§13)

	// leaseCtx bounds a reader's blocking publish-lease Reserve (WS-I Phase
	// B): leaseCancel is called by writeLocked's error path, by connection
	// teardown step 3, and by finishConnection, so a dead connection never
	// leaves its reader parked in the limiter.
	leaseCtx    context.Context
	leaseCancel context.CancelFunc
}

func (rt *connRuntime) releaseHandshakeSlot() {
	if rt.slot != nil {
		rt.slot.release()
	}
}

func (rt *connRuntime) isChannelClosing(id uint16) bool {
	if rt.closingCount.Load() == 0 {
		return false
	}
	_, ok := rt.closingChannels.Load(id)
	return ok
}

func (rt *connRuntime) markChannelClosing(id uint16) (first bool) {
	if _, loaded := rt.closingChannels.LoadOrStore(id, struct{}{}); loaded {
		return false
	}
	rt.closingCount.Add(1)
	return true
}

func (rt *connRuntime) clearChannelClosing(id uint16) {
	if _, ok := rt.closingChannels.LoadAndDelete(id); ok {
		rt.closingCount.Add(-1)
	}
}

// newConnRuntime creates and registers the runtime for c (handleConnection path).
func (s *Server) newConnRuntime(c *protocol.Connection, slot *handshakeSlot) *connRuntime {
	rt := &connRuntime{conn: c, slot: slot, torndown: make(chan struct{})}
	rt.leaseCtx, rt.leaseCancel = context.WithCancel(context.Background())
	s.runtimes.Store(c, rt)
	return rt
}

// runtimeFor returns the runtime for c, creating it lazily so existing unit
// tests that build a bare protocol.NewConnection(pipe) and call handlers
// directly keep working. Hot-path code fetches rt once at goroutine start and
// passes it down; only error paths call runtimeFor.
func (s *Server) runtimeFor(c *protocol.Connection) *connRuntime {
	if rt, ok := s.runtimes.Load(c); ok {
		return rt.(*connRuntime)
	}
	rt, _ := s.runtimes.LoadOrStore(c, newBareConnRuntime(c))
	return rt.(*connRuntime)
}

func newBareConnRuntime(c *protocol.Connection) *connRuntime {
	rt := &connRuntime{conn: c, torndown: make(chan struct{})}
	rt.leaseCtx, rt.leaseCancel = context.WithCancel(context.Background())
	return rt
}
