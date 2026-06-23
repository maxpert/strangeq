package interfaces

import (
	"context"
	"errors"
	"net"
	"regexp"
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
	Username         string
	VHostPermissions []VHostPermission
	Tags             []string
	Groups           []string
	Metadata         map[string]interface{}
}

// VHostPermission ties a permission triple to a specific virtual host.
// A user must have a VHostPermission entry for a vhost to access it at all.
type VHostPermission struct {
	VHost      string     `json:"vhost"`
	Permission Permission `json:"permission"`
}

// Permission represents a per-vhost permission triple (RabbitMQ model).
// Each field is a regular expression matched against resource names.
// The empty string pattern (^$) matches nothing and denies all operations.
type Permission struct {
	Configure string `json:"configure"` // regex for configure ops (declare/delete resources)
	Write     string `json:"write"`     // regex for write ops (publish, bind queue to exchange)
	Read      string `json:"read"`      // regex for read ops (consume, get, bind exchange)
}

// Matches checks if the given resource name matches this permission's
// regex pattern for the specified action. Returns false on invalid regex
// or empty pattern.
func (p Permission) Matches(action OperationAction, resourceName string) bool {
	var pattern string
	switch action {
	case ActionConfigure:
		pattern = p.Configure
	case ActionWrite:
		pattern = p.Write
	case ActionRead:
		pattern = p.Read
	default:
		return false
	}
	if pattern == "" {
		return false
	}
	matched, err := regexp.MatchString(pattern, resourceName)
	if err != nil {
		return false
	}
	return matched
}

// OperationAction represents the type of authorization action.
// These map to RabbitMQ's configure/write/read permission model.
type OperationAction string

const (
	ActionConfigure OperationAction = "configure"
	ActionWrite     OperationAction = "write"
	ActionRead      OperationAction = "read"
)

// ResourceType represents the type of AMQP resource being authorized.
type ResourceType string

const (
	ResourceExchange ResourceType = "exchange"
	ResourceQueue    ResourceType = "queue"
	ResourceVHost    ResourceType = "vhost"
)

// Operation represents an authorization check request.
type Operation struct {
	Action       OperationAction // configure, write, or read
	ResourceType ResourceType    // exchange, queue, or vhost
	Resource     string          // resource name (exchange/queue name, or vhost name)
	VHost        string          // virtual host context for the operation
}

// DefaultExchangeName is the name used for permission checks on the AMQP
// default exchange (empty name). RabbitMQ maps the blank default exchange
// name to "amq.default" when performing permission checks.
const DefaultExchangeName = "amq.default"

// NormalizeExchangeName maps the empty default exchange name to the
// canonical name used for permission checks. Non-empty names are returned
// unchanged.
func NormalizeExchangeName(name string) string {
	if name == "" {
		return DefaultExchangeName
	}
	return name
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
