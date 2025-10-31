package interfaces

import (
	"time"
)

// Config defines the interface for server configuration
type Config interface {
	// GetNetwork returns network configuration
	GetNetwork() NetworkConfig

	// GetStorage returns storage configuration
	GetStorage() StorageConfig

	// GetSecurity returns security configuration
	GetSecurity() SecurityConfig

	// GetServer returns server information configuration
	GetServer() ServerConfig

	// Validate validates the configuration
	Validate() error

	// Load loads configuration from a source
	Load(source string) error

	// Save saves configuration to a destination
	Save(destination string) error
}

// NetworkConfig holds network-related configuration
type NetworkConfig struct {
	// Address to bind the server to
	Address string

	// Port to listen on
	Port int

	// Maximum number of connections
	MaxConnections int

	// Connection timeout
	ConnectionTimeout time.Duration

	// Heartbeat interval
	HeartbeatInterval time.Duration

	// TCP keepalive settings
	TCPKeepAlive         bool
	TCPKeepAliveInterval time.Duration

	// Buffer sizes
	ReadBufferSize  int
	WriteBufferSize int
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	// Backend type ("memory", "bbolt", "badger", etc.)
	Backend string

	// Connection string or file path
	Path string

	// Storage-specific options
	Options map[string]interface{}

	// Persistence settings
	Persistent bool
	SyncWrites bool

	// Message settings
	MessageTTL int64 // Message TTL in seconds (0 = no TTL)

	// Performance settings
	CacheSize     int64
	MaxOpenFiles  int
	CompactionAge time.Duration
}

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	// TLS settings
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string

	// Authentication settings
	AuthenticationEnabled  bool
	AuthenticationBackend  string // "file", "ldap", "database", etc.
	AuthenticationConfig   map[string]interface{}
	AuthenticationFilePath string   // Path to auth file for file backend
	AuthMechanisms         []string // Enabled SASL mechanisms (PLAIN, ANONYMOUS, etc.)

	// Authorization settings
	AuthorizationEnabled bool
	DefaultVHost         string

	// Access control
	AllowedUsers []string
	BlockedUsers []string
	AllowedHosts []string
	BlockedHosts []string
}

// ServerConfig holds server information configuration
type ServerConfig struct {
	// Server identification
	Name      string
	Version   string
	Product   string
	Platform  string
	Copyright string

	// Operational settings
	LogLevel  string
	LogFile   string
	PidFile   string
	Daemonize bool

	// Performance settings
	MaxChannelsPerConnection int
	MaxFrameSize             int
	MaxMessageSize           int64

	// Timeouts and intervals
	ChannelTimeout  time.Duration
	MessageTimeout  time.Duration
	CleanupInterval time.Duration

	// Memory management
	// MemoryLimitPercent sets memory threshold as percentage of system RAM (0-100)
	// Default: 60 (60% of RAM)
	// Set to 0 to use absolute limit instead
	MemoryLimitPercent int `json:"memory_limit_percent"`

	// MemoryLimitBytes sets absolute memory limit in bytes
	// Used when MemoryLimitPercent is 0
	// Default: 0 (use percentage instead)
	MemoryLimitBytes int64 `json:"memory_limit_bytes"`
}
