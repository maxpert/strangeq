package protocol

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultReadBufferSize is the default size of the per-connection buffered
// reader that coalesces frame reads (SQ-1). A single publish is three frames
// (method + content-header + body); without buffering every frame costs ~2 raw
// read syscalls straight off net.Conn, so a 3-frame publish burns ~6. A 64 KiB
// buffer lets io.ReadFull pull many frames' worth of bytes per syscall.
const DefaultReadBufferSize = 64 * 1024

var idFallbackCounter atomic.Uint64

func fallbackID(prefix string) string {
	n := idFallbackCounter.Add(1)
	return fmt.Sprintf("%s.%d.%d", prefix, time.Now().UnixNano(), n)
}

// Connection represents an AMQP connection
type Connection struct {
	ID   string
	Conn net.Conn
	// Reader is the buffered read side of Conn. ALL frame reads for the
	// connection MUST go through Reader (never Conn directly) from the handshake
	// onward; reading the raw Conn after buffering starts would discard bytes
	// already pulled into the buffer. SetReadDeadline stays on the raw Conn —
	// bufio.Reader delegates the underlying deadline transparently (SQ-1).
	Reader   io.Reader
	Channels sync.Map    // map[uint16]*Channel - concurrent-safe map
	Vhost    string      // Virtual host for this connection
	Username string      // Authenticated username
	User     interface{} // Authenticated *interfaces.User (interface{} to avoid import cycle; set once during handshake, immutable thereafter)
	// ClientProperties holds the properties/capabilities the client advertised
	// in connection.start-ok, captured during the handshake and immutable
	// thereafter. nil until start-ok is parsed. SQ-12 gates
	// connection.blocked/unblocked emission on the client's advertised
	// connection.blocked capability (see ClientSupportsBlocked).
	ClientProperties map[string]interface{}
	PendingMessages  map[uint16]*PendingMessage // Track messages being published on each channel.
	// SINGLE-WRITER: accessed only by the processFrames goroutine. No mutex needed.
	FrameQueue     chan *Frame   // Buffer frames between reader and processor goroutines
	AckQueue       chan *Frame   // Buffer ACK/NACK/Reject frames for the ack worker goroutine
	Done           chan struct{} // Closed during connection teardown to unblock goroutines waiting on AckQueue
	ConfirmWake    chan struct{} // SQ-5: capacity-1 poke channel for the publisher-confirm flusher goroutine
	WriteMutex     sync.Mutex    // Protects socket writes (heartbeat sender + frame processor both write)
	Closed         atomic.Bool   // Atomic flag for connection closure
	ConsumersDirty atomic.Bool   // Set when consumers are added/removed; delivery loop re-scans only when true

	// DepthBackpressure is a reserved per-connection queue-depth backpressure
	// flag (true when queue usage is high). It is currently written nowhere;
	// SQ-12 resource-alarm backpressure is driven by the server's global
	// alarmState, not this field. Renamed from Blocked for clarity — the
	// resource-alarm state a client is told about lives in AlarmNotified below.
	DepthBackpressure atomic.Bool

	// AlarmNotified tracks whether SQ-12 has told this (opted-in) client it is
	// blocked: true after connection.blocked was sent, false after
	// connection.unblocked. It makes edge emission exactly-once per connection
	// and resolves the race between the monitor's broadcast and a connection
	// that joins mid-alarm. Only meaningful for clients that advertised the
	// connection.blocked capability.
	AlarmNotified atomic.Bool

	// HasPublished is set the first time this connection completes a basic.publish
	// and never cleared. SQ-12 blocks only PUBLISHING connections (RabbitMQ
	// parity): a consumer-only connection (HasPublished false) is never
	// reader-paused, so its acks/consumes/control frames keep flowing and a
	// memory alarm can clear as queues drain. Read on the reader-pause path only
	// while an alarm is active (short-circuited behind alarmState), so it adds no
	// hot-path cost when no alarm is set.
	HasPublished atomic.Bool

	// AlarmWake is a capacity-1 poke channel that unparks this connection's
	// reader when a resource alarm clears (SQ-12). The monitor pokes it
	// (non-blocking) after storing alarmState=0; the parked reader re-checks the
	// authoritative alarmState after receiving the poke, so a coalesced or
	// racing poke cannot wedge it (lost-wakeup-free).
	AlarmWake chan struct{}

	// Negotiated connection parameters (set during handshake)
	MaxFrameSize uint32        // Maximum frame size negotiated with client (0 = unlimited)
	MaxChannels  uint16        // Maximum channels negotiated with client (0 = unlimited)
	HeartbeatSec atomic.Uint32 // Negotiated heartbeat interval in seconds (0 = disabled)
	ChannelCount atomic.Int32  // Current number of open channels on this connection

	// Connection metadata for stats/reporting
	ConnectedAt  time.Time    // When the connection was established
	LastActivity atomic.Int64 // UnixNano of last activity (updated on frame read/write)
}

// NewConnection creates a new AMQP connection with the default read-buffer size.
func NewConnection(conn net.Conn) *Connection {
	return NewConnectionWithReadBuffer(conn, DefaultReadBufferSize)
}

// NewConnectionWithReadBuffer creates a new AMQP connection whose frame reads are
// coalesced through a bufio.Reader of bufSize bytes (SQ-1). A non-positive
// bufSize falls back to DefaultReadBufferSize. The buffered reader wraps conn;
// callers must route every frame read through c.Reader, and keep SetReadDeadline
// on c.Conn.
func NewConnectionWithReadBuffer(conn net.Conn, bufSize int) *Connection {
	if bufSize <= 0 {
		bufSize = DefaultReadBufferSize
	}
	c := &Connection{
		ID:   generateID(),
		Conn: conn,
		// conn may be nil in unit tests that never read frames; bufio tolerates a
		// nil underlying reader until the first Read.
		Reader: bufio.NewReaderSize(conn, bufSize),
		// Channels: sync.Map needs no initialization
		PendingMessages: make(map[uint16]*PendingMessage),
		FrameQueue:      make(chan *Frame, 10000), // 10K frame buffer for reader/processor separation
		AckQueue:        make(chan *Frame, 4096),  // 4K ACK buffer for off-processor ACK handling
		ConfirmWake:     make(chan struct{}, 1),   // SQ-5: pokes coalesce; see WakeConfirmFlusher
		AlarmWake:       make(chan struct{}, 1),   // SQ-12: pokes coalesce; unparks the reader on alarm clear
		Done:            make(chan struct{}),
		ConnectedAt:     time.Now(),
	}
	c.TouchActivity()
	return c
}

// ClientSupportsBlocked reports whether the client advertised the
// connection.blocked capability in its connection.start-ok client-properties.
// Per the RabbitMQ resource-alarm extension, blocked/unblocked frames are
// emitted ONLY to clients that opt in via capabilities."connection.blocked" =
// true (SQ-12). Safe on a nil ClientProperties map and tolerant of malformed
// property types — anything other than an explicit true reads as unsupported.
func (c *Connection) ClientSupportsBlocked() bool {
	caps, ok := c.ClientProperties["capabilities"].(map[string]interface{})
	if !ok {
		return false
	}
	v, ok := caps["connection.blocked"].(bool)
	return ok && v
}

// TouchActivity updates the last-activity timestamp to now.
func (c *Connection) TouchActivity() {
	c.LastActivity.Store(time.Now().UnixNano())
}

// GetLastActivity returns the last-activity timestamp.
func (c *Connection) GetLastActivity() time.Time {
	return time.Unix(0, c.LastActivity.Load())
}

// WakeConfirmFlusher pokes the connection's publisher-confirm flusher goroutine
// (SQ-5). Non-blocking and lock-free: pokes coalesce in the capacity-1 channel,
// which is exactly the batching mechanism — while the flusher is busy writing
// one basic.ack, any number of newly durable publishes fold into the single
// pending poke, and the next flush covers them all with one multiple=true ack.
// Safe on a zero-value Connection (nil chan): the send can never proceed, so
// the default branch is taken.
func (c *Connection) WakeConfirmFlusher() {
	select {
	case c.ConfirmWake <- struct{}{}:
	default:
	}
}

// Channel represents an AMQP channel
type Channel struct {
	ID              uint16
	Connection      *Connection
	Closed          bool
	Mutex           sync.RWMutex
	Consumers       map[string]*Consumer // Consumer tag -> Consumer
	DeliveryTag     uint64               // Used for delivery tags in acknowledgements
	PrefetchCount   uint16               // Channel-level prefetch count
	PrefetchSize    uint32               // Channel-level prefetch size (0 = unlimited)
	GlobalPrefetch  bool                 // Apply prefetch settings globally
	CurrentQueue    string               // Last declared queue name (for empty-name resolution per AMQP spec)
	FlowActive      atomic.Bool          // channel.flow state: true = content frames may be sent
	FlowWake        chan struct{}        // signaled to wake parked forwarders when flow resumes/closes
	ConfirmMode     atomic.Bool          // confirm.select state: true = server sends basic.ack for each publish
	ConfirmSequence atomic.Uint64        // channel-scoped delivery tag sequence for publisher confirms

	// SQ-5: publisher-confirm batching state. The per-publish hot path touches
	// ONLY confirmDurable (a lock-free CAS-max watermark). confirmFlushMu is a
	// BATCH-granularity lock: it is taken once per flushed basic.ack (never per
	// message) and serializes ack emission so the client always observes a
	// strictly increasing confirm-tag stream. confirmAcked is mutated only under
	// confirmFlushMu but is an atomic so HasUnflushedConfirms can peek lock-free.
	confirmDurable atomic.Uint64 // highest contiguous tag past the durability barrier
	confirmAcked   atomic.Uint64 // highest tag already acked to the client
	confirmFlushMu sync.Mutex    // serializes confirm flushes and channel-close suppression
	confirmClosed  bool          // set on channel close; suppresses further confirm acks

	// SQ-18: per-channel wire delivery-tag remapping.
	//
	// A single channel may consume from multiple queues (and/or interleave
	// basic.get) whose broker-internal message IDs are drawn from one global,
	// per-message counter. Emitting those raw msgIDs as the wire delivery tag
	// makes the channel's tag stream non-monotonic (AMQP 0.9.1 requires a
	// per-channel strictly increasing sequence) and breaks cumulative acks
	// (basic.ack/nack with multiple=true), whose "all tags <= N" semantics span
	// every consumer on the channel — a client's single cumulative ack must be
	// able to settle earlier deliveries that came from a *different* queue.
	//
	// wireTagSeq mints the monotonic per-channel wire tags (1,2,3,...). The
	// table maps each still-unacked wire tag back to the underlying broker
	// (msgID, consumer) so ack/nack/reject can be translated to the msgID-keyed
	// broker ledger. No-ack deliveries are implicitly settled at send time and
	// are never acked by the client, so they consume a wire tag (to keep the
	// stream monotonic) but are deliberately NOT tracked here — keeping the
	// no-ack fast path allocation-free and map-free.
	wireTagSeq   atomic.Uint64
	wireTagMu    sync.Mutex
	wireTagTable map[uint64]WireDeliveryRef
}

// WireDeliveryRef records the broker-internal identity behind an outstanding
// wire delivery tag so acknowledgements addressed by wire tag can be routed to
// the correct msgID-keyed broker settle path.
type WireDeliveryRef struct {
	MsgID       uint64
	ConsumerTag string // "" for a basic.get delivery
	IsGet       bool
}

// NewChannel creates a new AMQP channel
func NewChannel(id uint16, conn *Connection) *Channel {
	ch := &Channel{
		ID:             id,
		Connection:     conn,
		Consumers:      make(map[string]*Consumer),
		DeliveryTag:    0,     // Will be incremented for each delivery
		PrefetchCount:  0,     // No limit by default
		PrefetchSize:   0,     // No limit by default
		GlobalPrefetch: false, // Per-consumer by default
		FlowWake:       make(chan struct{}, 1),
		wireTagTable:   make(map[uint64]WireDeliveryRef),
	}
	ch.FlowActive.Store(true) // Flow is active by default per AMQP spec
	return ch
}

// AdvanceConfirmDurable records that every publisher-confirm tag <= tag on this
// channel has crossed the durability barrier and is safe to ack (SQ-5).
// Lock-free CAS-max: safe from any goroutine and never regresses the watermark.
//
// CONTIGUITY CONTRACT: because a later flush acks the watermark with
// multiple=true (confirming ALL tags <= watermark), callers must deliver
// barrier crossings for a channel in non-decreasing tag order. Today this holds
// trivially: publishes on a channel are processed serially by the connection's
// frame-processor goroutine and the synchronous barrier completes each tag
// before the next publish starts. A future asynchronous barrier (e.g. WAL
// group-commit completion callbacks) must preserve per-channel completion order
// or fold out-of-order completions into a contiguous watermark before calling
// this.
func (c *Channel) AdvanceConfirmDurable(tag uint64) {
	for {
		cur := c.confirmDurable.Load()
		if tag <= cur || c.confirmDurable.CompareAndSwap(cur, tag) {
			return
		}
	}
}

// HasUnflushedConfirms reports whether this channel has durable publisher
// confirms not yet acked to the client. Lock-free; used by the confirm flusher
// to skip idle channels without taking the flush lock.
func (c *Channel) HasUnflushedConfirms() bool {
	return c.confirmDurable.Load() > c.confirmAcked.Load()
}

// FlushConfirms sends at most ONE basic.ack covering every durable-but-unacked
// publisher-confirm tag on this channel (SQ-5). send is invoked with
// (tag, multiple) — multiple=true iff the ack covers more than one tag — while
// the flush lock is held, so concurrent flushers (inline flush on the frame
// processor and the per-connection flusher goroutine) emit acks in strictly
// increasing tag order and never double-ack. A no-op if the channel's confirm
// state is closed or nothing is pending. Returns send's error without marking
// the tags acked, so a failed write is retryable (in practice a write error
// tears the connection down).
func (c *Channel) FlushConfirms(send func(tag uint64, multiple bool) error) error {
	c.confirmFlushMu.Lock()
	defer c.confirmFlushMu.Unlock()
	return c.flushConfirmsLocked(send)
}

// FlushAndCloseConfirms performs a final confirm flush and marks the channel
// closed for confirms, all under one critical section (SQ-5). After it returns,
// no goroutine can emit another confirm ack for this channel — required on
// channel.close so a straggling flusher pass cannot write a basic.ack after the
// channel.close-ok. The final flush error is returned but the closed mark is
// applied regardless.
func (c *Channel) FlushAndCloseConfirms(send func(tag uint64, multiple bool) error) error {
	c.confirmFlushMu.Lock()
	defer c.confirmFlushMu.Unlock()
	err := c.flushConfirmsLocked(send)
	c.confirmClosed = true
	return err
}

func (c *Channel) flushConfirmsLocked(send func(tag uint64, multiple bool) error) error {
	if c.confirmClosed {
		return nil
	}
	durable := c.confirmDurable.Load()
	acked := c.confirmAcked.Load()
	if durable <= acked {
		return nil
	}
	if err := send(durable, durable > acked+1); err != nil {
		return err
	}
	c.confirmAcked.Store(durable)
	return nil
}

// NextWireTag mints the next strictly-increasing per-channel wire delivery tag
// (1-based). Cheap and lock-free; safe to call from the delivery loop and the
// basic.get handler concurrently.
func (c *Channel) NextWireTag() uint64 {
	return c.wireTagSeq.Add(1)
}

// TrackDelivery records the broker identity behind a wire tag so a later
// acknowledgement addressed by that wire tag can be translated back to the
// msgID-keyed broker ledger. Only manual-ack deliveries are tracked.
func (c *Channel) TrackDelivery(wireTag, msgID uint64, consumerTag string, isGet bool) {
	c.wireTagMu.Lock()
	c.wireTagTable[wireTag] = WireDeliveryRef{MsgID: msgID, ConsumerTag: consumerTag, IsGet: isGet}
	c.wireTagMu.Unlock()
}

// ResolveWireTag returns the broker identity behind a wire tag WITHOUT removing
// it (used by transactional buffering, where settlement is deferred to commit).
func (c *Channel) ResolveWireTag(wireTag uint64) (WireDeliveryRef, bool) {
	c.wireTagMu.Lock()
	ref, ok := c.wireTagTable[wireTag]
	c.wireTagMu.Unlock()
	return ref, ok
}

// TakeWireTag atomically resolves and removes the entry for a single wire tag.
// A miss means the tag was already settled (duplicate ack, or a no-ack / stale
// tag) and the caller must treat it as a no-op.
func (c *Channel) TakeWireTag(wireTag uint64) (WireDeliveryRef, bool) {
	c.wireTagMu.Lock()
	ref, ok := c.wireTagTable[wireTag]
	if ok {
		delete(c.wireTagTable, wireTag)
	}
	c.wireTagMu.Unlock()
	return ref, ok
}

// TakeWireTagsUpTo removes and returns every still-outstanding wire tag <= upto
// on the channel — the cumulative-ack (multiple=true) working set. It spans all
// consumers on the channel, which is exactly why cumulative acks now settle
// deliveries that originated from different queues. Bounded by the channel's
// outstanding-unacked window (sum of per-consumer prefetch), so O(window).
func (c *Channel) TakeWireTagsUpTo(upto uint64) []WireDeliveryRef {
	c.wireTagMu.Lock()
	var refs []WireDeliveryRef
	for w, ref := range c.wireTagTable {
		if w <= upto {
			refs = append(refs, ref)
			delete(c.wireTagTable, w)
		}
	}
	c.wireTagMu.Unlock()
	return refs
}

// Exchange represents an AMQP exchange
type Exchange struct {
	Name       string
	Kind       string // direct, fanout, topic, headers
	Durable    bool
	AutoDelete bool
	Internal   bool
	Arguments  map[string]interface{}
	Bindings   []*Binding // List of bindings to queues
	Mutex      sync.RWMutex
}

// Copy returns a copy of the Exchange without the mutex.
// This is safe to use when you need to pass Exchange by value.
func (e *Exchange) Copy() Exchange {
	// Copy bindings
	bindingsCopy := make([]*Binding, len(e.Bindings))
	copy(bindingsCopy, e.Bindings)

	// Copy arguments
	argsCopy := make(map[string]interface{}, len(e.Arguments))
	for k, v := range e.Arguments {
		argsCopy[k] = v
	}

	return Exchange{
		Name:       e.Name,
		Kind:       e.Kind,
		Durable:    e.Durable,
		AutoDelete: e.AutoDelete,
		Internal:   e.Internal,
		Arguments:  argsCopy,
		Bindings:   bindingsCopy,
		// Mutex is intentionally not copied
	}
}

// Binding represents a binding between an exchange and a queue
type Binding struct {
	Exchange   string
	Queue      string
	RoutingKey string
	Arguments  map[string]interface{}
}

// Queue represents an AMQP queue using actor model (NO LOCKS!)
type Queue struct {
	Name         string
	Durable      bool
	AutoDelete   bool
	Exclusive    bool
	Arguments    map[string]interface{}
	Channel      *Channel      // Reference back to parent channel (runtime state, not persisted)
	MessageCount atomic.Uint64 // In-memory message count (runtime state, not persisted)
	OwnerConnID  string        // Connection ID that owns this queue (exclusive queues only)
}

// NewQueue creates a new queue
func NewQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) *Queue {
	q := &Queue{
		Name:       name,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		Arguments:  arguments,
	}
	return q
}

// Message represents an AMQP message
type Message struct {
	Body            []byte
	Headers         map[string]interface{}
	Exchange        string
	RoutingKey      string
	DeliveryTag     uint64
	Redelivered     bool
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8 // 1 = non-persistent, 2 = persistent
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       uint64
	Type            string
	UserID          string
	AppID           string
	ClusterID       string
	Mandatory       bool
	// EnqueueUnixMilli is the absolute publish/enqueue instant in Unix
	// milliseconds, used by SQ-9 (W4) as the anchor for per-message and
	// per-queue TTL deadline evaluation across a restart. W2 only makes this
	// field DURABLE (persisted in the WAL/segment message record and
	// reconstructed on recovery); it does NOT stamp it — W4 owns stamping it at
	// publish and deriving effective TTL. A zero value means "unset" (no TTL
	// anchor recorded), so the field costs nothing on the non-durable/no-TTL
	// hot path where it is never written.
	EnqueueUnixMilli int64
}

// Delivery represents a message delivery to a consumer
type Delivery struct {
	Message     *Message
	DeliveryTag uint64 // broker-internal msgID (NOT the wire delivery tag; see Channel wire-tag remapping)
	Redelivered bool
	Exchange    string
	RoutingKey  string
	ConsumerTag string
	NoAck       bool // consumer is no-ack: settled at send time, never acked by the client
}

// PendingMessage represents a message in the process of being published
// (method frame received, waiting for header and body frames)
type PendingMessage struct {
	Method   *BasicPublishMethod
	Header   *ContentHeader
	Body     []byte
	BodySize uint64
	Received uint64 // How much of the body has been received so far
	Channel  *Channel
}

// Consumer represents a message consumer
type Consumer struct {
	Tag            string
	Channel        *Channel
	Queue          string
	NoAck          bool
	Exclusive      bool
	Args           map[string]interface{}
	Messages       chan *Delivery
	Cancel         chan struct{}
	PrefetchCount  uint16        // Maximum number of unacknowledged messages
	CurrentUnacked atomic.Uint64 // Current count of unacknowledged messages (atomic for lock-free access)
}

// generateID generates a random ID string
func generateID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return fallbackID("conn")
	}

	return fmt.Sprintf("%x", b)
}

// GenerateQueueName generates a unique server-assigned queue name.
// Per AMQP 0.9.1 spec, when queue.declare is called with an empty name,
// the server MUST create a unique generated name. RabbitMQ uses the
// "amq.gen." prefix convention; we follow the same convention for
// client compatibility.
func GenerateQueueName() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return fallbackID("amq.gen")
	}
	return "amq.gen." + fmt.Sprintf("%x", b)
}
