package server

// This file declares ONLY the frozen management-side view types of
// `.notes/specs/mgmt-03-broker-accessors.md` §7 (BOB-11). The accessor METHODS
// that populate them (SnapshotEntityTotals, EntityObserverWired, ChannelViews,
// the entityGate counters) are WS3's broker-accessor work and are intentionally
// NOT implemented here. The management package discovers them through the
// optional interfaces `entitySnapshotter` and `channelViews` and falls back to
// independent reads with `snapshot_coherent: false` when absent.

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
	Bindings    *int64 `json:"bindings"` // nil => JSON null (BOB-4)
	Generation  uint64 `json:"-"`
}

// ChannelView is the lock-free management view of one live channel. Fields
// whose backing value is guarded by protocol.Channel.Mutex are deliberately
// absent: prefetch_count and transactional are reported null by the API.
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
