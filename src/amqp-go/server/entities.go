package server

import (
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
)

// EntityTotals is a linearisable point-in-time count of the five broker entity
// kinds plus a monotonic generation. Two reads that observe the same Generation
// observed identical counts. Bindings is nil until the aggregate binding
// accessor (BOB-4) exists.
type EntityTotals struct {
	Connections int64  `json:"connections"`
	Channels    int64  `json:"channels"`
	Consumers   int64  `json:"consumers"`
	Queues      int64  `json:"queues"`
	Exchanges   int64  `json:"exchanges"`
	Bindings    *int64 `json:"bindings"` // nil => JSON null (BOB-4 unbuilt)
	Generation  uint64 `json:"-"`
}

// SnapshotEntityTotals returns the five counts from one gate-held read.
// Bounded cost: O(1); it takes only Server.entityGate (permitted).
func (s *Server) SnapshotEntityTotals() EntityTotals {
	s.entityGate.Lock()
	defer s.entityGate.Unlock()
	return EntityTotals{
		Connections: s.entityConnections,
		Channels:    s.entityChannels,
		Consumers:   s.entityConsumers,
		Queues:      s.entityQueues,
		Exchanges:   s.entityExchanges,
		Generation:  s.entityGeneration,
	}
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

	s.entityGate.Lock()
	s.entityConnections = connections
	s.entityChannels = channels
	s.entityQueues = queues
	s.entityExchanges = exchanges
	s.entityConsumers = consumers
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
// absent: prefetch_count and transactional are reported null by the API, and
// `closing` is not observable.
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
		user := conn.Username
		vhost := conn.Vhost
		conn.Channels.Range(func(_, value interface{}) bool {
			ch := value.(*protocol.Channel)
			view := ChannelView{
				Number:       ch.ID,
				ConnectionID: connID,
				User:         user,
				Vhost:        vhost,
				Confirm:      ch.ConfirmMode.Load(),
				FlowActive:   ch.FlowActive.Load(),
				HasUnacked:   true,
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
