package protocol

import (
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var idFallbackCounter atomic.Uint64

func fallbackID(prefix string) string {
	n := idFallbackCounter.Add(1)
	return fmt.Sprintf("%s.%d.%d", prefix, time.Now().UnixNano(), n)
}

// Connection represents an AMQP connection
type Connection struct {
	ID              string
	Conn            net.Conn
	Channels        sync.Map                   // map[uint16]*Channel - concurrent-safe map
	Vhost           string                     // Virtual host for this connection
	Username        string                     // Authenticated username
	User            interface{}                // Authenticated *interfaces.User (interface{} to avoid import cycle; set once during handshake, immutable thereafter)
	PendingMessages map[uint16]*PendingMessage // Track messages being published on each channel.
	// SINGLE-WRITER: accessed only by the processFrames goroutine. No mutex needed.
	FrameQueue     chan *Frame   // Buffer frames between reader and processor goroutines
	AckQueue       chan *Frame   // Buffer ACK/NACK/Reject frames for the ack worker goroutine
	Done           chan struct{} // Closed during connection teardown to unblock goroutines waiting on AckQueue
	WriteMutex     sync.Mutex    // Protects socket writes (heartbeat sender + frame processor both write)
	Closed         atomic.Bool   // Atomic flag for connection closure
	ConsumersDirty atomic.Bool   // Set when consumers are added/removed; delivery loop re-scans only when true
	Blocked        atomic.Bool   // Back-pressure flag: true when queue usage > 90%, false when < 80%

	// Negotiated connection parameters (set during handshake)
	MaxFrameSize uint32        // Maximum frame size negotiated with client (0 = unlimited)
	MaxChannels  uint16        // Maximum channels negotiated with client (0 = unlimited)
	HeartbeatSec atomic.Uint32 // Negotiated heartbeat interval in seconds (0 = disabled)
	ChannelCount atomic.Int32  // Current number of open channels on this connection

	// Connection metadata for stats/reporting
	ConnectedAt  time.Time    // When the connection was established
	LastActivity atomic.Int64 // UnixNano of last activity (updated on frame read/write)
}

// NewConnection creates a new AMQP connection
func NewConnection(conn net.Conn) *Connection {
	c := &Connection{
		ID:   generateID(),
		Conn: conn,
		// Channels: sync.Map needs no initialization
		PendingMessages: make(map[uint16]*PendingMessage),
		FrameQueue:      make(chan *Frame, 10000), // 10K frame buffer for reader/processor separation
		AckQueue:        make(chan *Frame, 4096),  // 4K ACK buffer for off-processor ACK handling
		Done:            make(chan struct{}),
		ConnectedAt:     time.Now(),
	}
	c.TouchActivity()
	return c
}

// TouchActivity updates the last-activity timestamp to now.
func (c *Connection) TouchActivity() {
	c.LastActivity.Store(time.Now().UnixNano())
}

// GetLastActivity returns the last-activity timestamp.
func (c *Connection) GetLastActivity() time.Time {
	return time.Unix(0, c.LastActivity.Load())
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
	}
	ch.FlowActive.Store(true) // Flow is active by default per AMQP spec
	return ch
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
}

// Delivery represents a message delivery to a consumer
type Delivery struct {
	Message     *Message
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
	ConsumerTag string
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
