package interfaces

import (
	"context"
	"errors"
	"net"
	"time"
)

// Authentication errors
var (
	ErrUserNotFound       = errors.New("user not found")
	ErrInvalidCredentials = errors.New("invalid credentials")
)

// Logger defines the interface for logging operations
type Logger interface {
	Debug(msg string, fields ...LogField)
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
	Fatal(msg string, fields ...LogField)

	With(fields ...LogField) Logger
	Sync() error
}

// LogField represents a structured logging field
type LogField struct {
	Key   string
	Value interface{}
}

// ConnectionHandler defines the interface for custom connection handling
type ConnectionHandler interface {
	// HandleConnection processes a new client connection
	HandleConnection(conn net.Conn) error

	// OnConnect is called when a connection is established
	OnConnect(connectionID string) error

	// OnDisconnect is called when a connection is closed
	OnDisconnect(connectionID string) error

	// ValidateConnection performs connection validation
	ValidateConnection(connectionID string) error
}

// Authenticator defines the interface for authentication mechanisms
type Authenticator interface {
	// Authenticate validates user credentials
	Authenticate(username, password string) (*User, error)

	// Authorize checks if a user has permission for an operation
	Authorize(user *User, operation Operation) error

	// GetUser retrieves user information
	GetUser(username string) (*User, error)

	// RefreshUser updates user information from the backing store
	RefreshUser(user *User) error
}

// User represents an authenticated user
type User struct {
	Username    string
	Permissions []Permission
	Groups      []string
	Metadata    map[string]interface{}
}

// Permission represents a user permission
type Permission struct {
	Resource string // exchange, queue, etc.
	Action   string // read, write, configure, etc.
	Pattern  string // resource name pattern
}

// Operation represents an operation requiring authorization
type Operation struct {
	Type     string                 // "exchange.declare", "queue.bind", "basic.publish", etc.
	Resource string                 // resource name
	Action   string                 // specific action
	Context  map[string]interface{} // additional context
}

// Server defines the interface for AMQP server implementations
type Server interface {
	// Start starts the server with the given context
	Start(ctx context.Context) error

	// Stop gracefully stops the server
	Stop(ctx context.Context) error

	// Shutdown forcefully shuts down the server
	Shutdown() error

	// Health returns the server health status
	Health() HealthStatus

	// GetStats returns server statistics
	GetStats() *ServerStats

	// GetConnections returns active connections
	GetConnections() []ConnectionInfo
}

// HealthStatus represents server health information
type HealthStatus struct {
	Status    string
	Uptime    time.Duration
	Errors    []string
	Warnings  []string
	Timestamp time.Time
}

// ServerStats provides server statistics
type ServerStats struct {
	Uptime            time.Duration
	Connections       int
	Channels          int
	Exchanges         int
	Queues            int
	Consumers         int
	MessagesPublished int64
	MessagesDelivered int64
	BytesReceived     int64
	BytesSent         int64
}

// ConnectionInfo provides information about a connection
type ConnectionInfo struct {
	ID            string
	RemoteAddress string
	Username      string
	VirtualHost   string
	Channels      int
	ConnectedAt   time.Time
	LastActivity  time.Time
}
