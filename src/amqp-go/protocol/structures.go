package protocol

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"net"
	"sync"
	"time"
)

// Connection represents an AMQP connection
type Connection struct {
	ID              string
	Conn            net.Conn
	Channels        map[uint16]*Channel
	Vhost           string                     // Virtual host for this connection
	Username        string                     // Authenticated username
	PendingMessages map[uint16]*PendingMessage // Track messages being published on each channel
	Mailboxes       *ConnectionMailboxes       // Three separate mailboxes (RabbitMQ-style: Heartbeat, Channel, Connection)
	Mutex           sync.RWMutex               // Protects connection state
	WriteMutex      sync.Mutex                 // Protects socket writes (heartbeat sender + frame processor both write)
	Closed          bool
	Blocked         bool // RabbitMQ-style memory alarm: true when queue usage > 90%, false when < 80%
}

// NewConnection creates a new AMQP connection
func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		ID:              generateID(),
		Conn:            conn,
		Channels:        make(map[uint16]*Channel),
		PendingMessages: make(map[uint16]*PendingMessage),
		Mailboxes:       NewConnectionMailboxes(), // RabbitMQ-style: three separate unbounded mailboxes
	}
}

// Channel represents an AMQP channel
type Channel struct {
	ID             uint16
	Connection     *Connection
	Closed         bool
	Mutex          sync.RWMutex
	Consumers      map[string]*Consumer // Consumer tag -> Consumer
	DeliveryTag    uint64               // Used for delivery tags in acknowledgements
	PrefetchCount  uint16               // Channel-level prefetch count
	PrefetchSize   uint32               // Channel-level prefetch size (0 = unlimited)
	GlobalPrefetch bool                 // Apply prefetch settings globally
}

// NewChannel creates a new AMQP channel
func NewChannel(id uint16, conn *Connection) *Channel {
	return &Channel{
		ID:             id,
		Connection:     conn,
		Consumers:      make(map[string]*Consumer),
		DeliveryTag:    0,     // Will be incremented for each delivery
		PrefetchCount:  0,     // No limit by default
		PrefetchSize:   0,     // No limit by default
		GlobalPrefetch: false, // Per-consumer by default
	}
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

// Queue represents an AMQP queue
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Arguments  map[string]interface{}

	// Storage: Index+cache architecture (always enabled)
	IndexManager *MessageIndexManager // Lightweight metadata (~64 bytes per message)
	Cache        *MessageCache        // Bounded LRU cache for message bodies
	MemoryLimit  int64                // Max cache size (bytes)

	Mutex   sync.RWMutex
	Channel *Channel // Reference back to parent channel
}

// Copy returns a copy of the Queue without the mutex.
// This is safe to use when you need to pass Queue by value.
func (q *Queue) Copy() Queue {
	// Copy arguments
	argsCopy := make(map[string]interface{}, len(q.Arguments))
	for k, v := range q.Arguments {
		argsCopy[k] = v
	}

	return Queue{
		Name:       q.Name,
		Durable:    q.Durable,
		AutoDelete: q.AutoDelete,
		Exclusive:  q.Exclusive,
		Arguments:  argsCopy,
		Channel:    q.Channel,
		// Mutex, IndexManager, Cache not copied (reference types)
	}
}

// InitializeStorage initializes the index+cache storage architecture
func (q *Queue) InitializeStorage(maxCacheSize int64) {
	if maxCacheSize <= 0 {
		maxCacheSize = DefaultMaxCacheSize
	}

	q.IndexManager = NewMessageIndexManager()
	q.Cache = NewMessageCache(maxCacheSize)
	q.MemoryLimit = maxCacheSize
}

// AddMessage adds a message to the queue storage
func (q *Queue) AddMessage(deliveryTag uint64, message *Message) {
	idx := NewMessageIndex(deliveryTag, message, q.Name)
	q.IndexManager.Add(idx)
	q.Cache.Put(idx.CacheKey, deliveryTag, message)
}

// GetMessage retrieves a message by delivery tag
// Returns the message from cache if available, nil if not found
func (q *Queue) GetMessage(deliveryTag uint64) (*Message, *MessageIndex) {
	idx, found := q.IndexManager.Get(deliveryTag)
	if !found {
		return nil, nil
	}

	if message, ok := q.Cache.Get(idx.CacheKey); ok {
		return message, idx
	}

	// Cache miss - caller should load from disk
	return nil, idx
}

// RemoveMessage removes a message from the queue storage
func (q *Queue) RemoveMessage(deliveryTag uint64) bool {
	idx, found := q.IndexManager.Get(deliveryTag)
	if !found {
		return false
	}

	q.Cache.Remove(idx.CacheKey)
	return q.IndexManager.Remove(deliveryTag)
}

// MessageCount returns the number of messages in the queue
func (q *Queue) MessageCount() int {
	return q.IndexManager.Len()
}

// CacheStats returns cache statistics for this queue
func (q *Queue) CacheStats() CacheStats {
	return q.Cache.GetStats()
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
	PrefetchCount  uint16 // Maximum number of unacknowledged messages
	CurrentUnacked uint64 // Current count of unacknowledged messages
}

// ConsumerDelivery represents a message delivered to a consumer
type ConsumerDelivery struct {
	Tag          string
	Delivery     *Delivery
	Consumer     *Consumer
	NotifyCancel chan string
}

// generateID generates a random ID string
func generateID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// This shouldn't happen in practice, but if it does, we'll use a simple fallback
		// Seed the math/rand package to ensure it generates different values
		mrand.Seed(time.Now().UnixNano())
		fallbackID := mrand.Int63()
		return fmt.Sprintf("conn-%d", fallbackID)
	}

	return fmt.Sprintf("%x", b)
}
