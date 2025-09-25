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
	ID       string
	Conn     net.Conn
	Channels map[uint16]*Channel
	Mutex    sync.RWMutex
	Closed   bool
}

// NewConnection creates a new AMQP connection
func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		ID:       generateID(),
		Conn:     conn,
		Channels: make(map[uint16]*Channel),
	}
}

// Channel represents an AMQP channel
type Channel struct {
	ID         uint16
	Connection *Connection
	Closed     bool
	Mutex      sync.RWMutex
	// Add more channel-specific state here
}

// NewChannel creates a new AMQP channel
func NewChannel(id uint16, conn *Connection) *Channel {
	return &Channel{
		ID:         id,
		Connection: conn,
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
	Messages   []*Message
	Mutex      sync.RWMutex
	Channel    *Channel // Reference back to parent channel
}

// Message represents an AMQP message
type Message struct {
	Body       []byte
	Headers    map[string]interface{}
	Exchange   string
	RoutingKey string
	DeliveryTag uint64
	Redelivered bool
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