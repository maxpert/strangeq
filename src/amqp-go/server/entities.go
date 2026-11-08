package server

import (
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// EntityTotals is a linearisable point-in-time count of the five broker entity
// kinds plus bindings and a monotonic generation. Two reads that observe the
// same Generation observed identical counts. Bindings is nil only while the
// aggregate binding accessor (BOB-4) is absent from the injected broker.
type EntityTotals struct {
	Connections int64  `json:"connections"`
	Channels    int64  `json:"channels"`
	Consumers   int64  `json:"consumers"`
	Queues      int64  `json:"queues"`
	Exchanges   int64  `json:"exchanges"`
	Bindings    *int64 `json:"bindings"` // nil => JSON null (binding seam absent)
	Generation  uint64 `json:"-"`
}

// SnapshotEntityTotals returns the entity counts from one gate-held read.
// Bounded cost: O(1); it takes only Server.entityGate (permitted).
func (s *Server) SnapshotEntityTotals() EntityTotals {
	s.entityGate.Lock()
	defer s.entityGate.Unlock()
	t := EntityTotals{
		Connections: s.entityConnections,
		Channels:    s.entityChannels,
		Consumers:   s.entityConsumers,
		Queues:      s.entityQueues,
		Exchanges:   s.entityExchanges,
		Generation:  s.entityGeneration,
	}
	if s.entityBindingsWired {
		n := s.entityBindings
		t.Bindings = &n
	}
	return t
}

// EntityObserverWired reports whether the broker observer seam is installed.
// The management layer reports snapshot_coherent only when this is true.
func (s *Server) EntityObserverWired() bool {
	s.entityGate.Lock()
	defer s.entityGate.Unlock()
	return s.entityObserverWired
}

// OnBrokerEntityChange applies a broker-side entity delta. It implements
// broker.EntityObserver and is called by the broker OUTSIDE any broker lock,
// after the registry changed.
func (s *Server) OnBrokerEntityChange(kind string, delta int64) {
	s.entityGate.Lock()
	switch kind {
	case "queue":
		s.entityQueues += delta
	case "exchange":
		s.entityExchanges += delta
	case "consumer":
		s.entityConsumers += delta
	case "binding":
		s.entityBindings += delta
	default:
		s.entityGate.Unlock()
		return
	}
	s.entityGeneration++
	s.entityGate.Unlock()
}

// withEntityDelta applies a delta to one of the server-owned counters
// (connections, channels) under entityGate and bumps the generation. It is
// called at the same statements that mutate Server.Connections / the per-
// connection channel count.
func (s *Server) withEntityDelta(kind string, delta int64) {
	s.entityGate.Lock()
	switch kind {
	case "connections":
		s.entityConnections += delta
	case "channels":
		s.entityChannels += delta
	default:
		s.entityGate.Unlock()
		return
	}
	s.entityGeneration++
	s.entityGate.Unlock()
}

// InitEntityCounters seeds the five entity counters from the live registries
// and wires the broker observer. It MUST be called exactly once, AFTER recovery
// (so recovered entities are seeded, not double-counted by later observer
// events) and BEFORE the management listener starts. Build() calls it at
// integration; tests call it directly.
func (s *Server) InitEntityCounters() {
	// Read the registries into locals first, then take entityGate alone. No
	// path holds entityGate while taking Server.Mutex, so there is no
	// lock-order inversion.
	s.Mutex.RLock()
	connections := int64(len(s.Connections))
	var channels int64
	for _, conn := range s.Connections {
		conn.Channels.Range(func(_, _ interface{}) bool {
			channels++
			return true
		})
	}
	s.Mutex.RUnlock()

	var queues, exchanges, consumers int64
	if s.Broker != nil {
		queues = int64(len(s.Broker.GetQueues()))
		exchanges = int64(len(s.Broker.GetExchanges()))
		consumers = int64(len(s.Broker.GetConsumers()))
	}

	// BOB-4: seed the binding count from the aggregate binding seam when the
	// injected broker has one. A custom UnifiedBroker without it keeps the
	// count unwired, so the overview renders object_totals.bindings: null.
	//
	// If the seam IS present but the seed read FAILS, capabilities.bindings
	// reports true while object_totals.bindings stays null. That is the honest
	// state — the boot count is genuinely unknown, and seeding a fabricated 0
	// (which the observer's later deltas would then compound) would be worse —
	// but it must not be SILENT: log the failure so an operator can see the two
	// disagree (WS4 review finding 8).
	var bindings int64
	bindingsWired := false
	if lister, ok := s.Broker.(interface {
		ListBindings() ([]broker.BindingView, error)
	}); ok {
		v, err := lister.ListBindings()
		if err == nil {
			bindings = int64(len(v))
			bindingsWired = true
		} else if s.Log != nil {
			s.Log.Warn("could not seed the binding entity count; object_totals.bindings will report null",
				zap.Error(err))
		}
	}

	s.entityGate.Lock()
	s.entityConnections = connections
	s.entityChannels = channels
	s.entityQueues = queues
	s.entityExchanges = exchanges
	s.entityConsumers = consumers
	s.entityBindings = bindings
	s.entityBindingsWired = bindingsWired
	s.entityGeneration++
	s.entityGate.Unlock()

	s.wireEntityObserver()
}

// wireEntityObserver installs the entity observer on the production broker,
// discovered through the optional SetEntityObserver(EntityObserver) seam. A
// custom UnifiedBroker that lacks the seam leaves entityObserverWired false.
func (s *Server) wireEntityObserver() {
	if s.Broker == nil {
		return
	}
	setter, ok := s.Broker.(interface {
		SetEntityObserver(broker.EntityObserver)
	})
	if !ok {
		return
	}
	setter.SetEntityObserver(s)
	s.entityGate.Lock()
	s.entityObserverWired = true
	s.entityGate.Unlock()
}

// ChannelView is the lock-free management view of one live channel. Fields
// whose backing value is guarded by protocol.Channel.Mutex are deliberately
// absent: the consumer count is derived from Broker.ConsumerViews(), and
// prefetch_count/transactional are read from lock-free atomics (BOB-3).
// `closing` is derived from the per-connection runtime, never from
// Channel.Closed (which is written under Channel.Mutex).
type ChannelView struct {
	Number       uint16
	ConnectionID string
	User         string
	Vhost        string
	Consumers    int
	Confirm      bool  // Channel.ConfirmMode (atomic.Bool) — safe
	FlowActive   bool  // Channel.FlowActive   (atomic.Bool) — safe
	Unacked      int64 // BOB-8 sum over consumers on this channel
	HasUnacked   bool  // false when any owning consumer has AckKnown==false

	// BOB-3 fields.
	PrefetchCount uint16 // Channel.PrefetchCount.Load() (0 = unlimited)
	Transactional bool   // Channel.Transactional.Load()
	Closing       bool   // connRuntime.isChannelClosing(ch.ID)
}

// ChannelViews enumerates live channels under Server.Mutex.RLock() only. It
// ranges Connection.Channels sync.Maps and reads atomics/immutable fields; it
// never takes Channel.Mutex and never reads Channel.Consumers / Closed /
// PrefetchCount. Consumers is derived from Broker.ConsumerViews() by matching
// ChannelID+ConnectionID.
func (s *Server) ChannelViews() []ChannelView {
	type chanKey struct {
		connID string
		number uint16
	}
	type consumerAgg struct {
		count      int
		unacked    int64
		hasUnacked bool
	}

	perChannel := make(map[chanKey]*consumerAgg)
	if cv, ok := s.Broker.(interface {
		ConsumerViews() []broker.ConsumerView
	}); ok {
		for _, v := range cv.ConsumerViews() {
			if !v.HasChannel {
				continue
			}
			k := chanKey{connID: v.ConnectionID, number: v.ChannelID}
			a := perChannel[k]
			if a == nil {
				a = &consumerAgg{hasUnacked: true}
				perChannel[k] = a
			}
			a.count++
			a.unacked += v.Unacked
			if !v.AckKnown {
				a.hasUnacked = false
			}
		}
	}

	var views []ChannelView
	s.Mutex.RLock()
	for _, conn := range s.Connections {
		connID := conn.ID
		rt, hasRT := s.runtimes.Load(conn)
		// PRE-EXISTING FOLLOW-UP (reviewer F6): Username and Vhost are written
		// by the handshake goroutine (handleConnectionStartOK / handleConnectionOpen
		// in connection_handlers.go) AFTER this connection was already published
		// into s.Connections (server.go), and that write is NOT held under
		// Server.Mutex. Reading them here under Mutex.RLock is therefore an
		// unsynchronised read. A correct fix publishes the connection only after
		// connection.open (or guards the two fields), which is a handshake change
		// deliberately left out of this work stream rather than risked here.
		user := conn.Username
		vhost := conn.Vhost
		conn.Channels.Range(func(_, value interface{}) bool {
			ch := value.(*protocol.Channel)
			view := ChannelView{
				Number:        ch.ID,
				ConnectionID:  connID,
				User:          user,
				Vhost:         vhost,
				Confirm:       ch.ConfirmMode.Load(),
				FlowActive:    ch.FlowActive.Load(),
				HasUnacked:    true,
				PrefetchCount: uint16(ch.PrefetchCount.Load()),
				Transactional: ch.Transactional.Load(),
			}
			// BOB-3: the closing signal comes from the per-connection runtime
			// (markChannelClosing fires on every server-initiated channel
			// close), never from Channel.Closed — which is written under
			// Channel.Mutex and whose channel is already out of the registry by
			// the time it is set.
			if hasRT {
				view.Closing = rt.(*connRuntime).isChannelClosing(ch.ID)
			}
			if a := perChannel[chanKey{connID: connID, number: ch.ID}]; a != nil {
				view.Consumers = a.count
				view.Unacked = a.unacked
				view.HasUnacked = a.hasUnacked
			}
			views = append(views, view)
			return true
		})
	}
	s.Mutex.RUnlock()
	return views
}
