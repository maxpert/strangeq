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

	// GetEngine returns engine tuning configuration
	GetEngine() EngineConfig

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

	// Offset checkpoint interval (Phase 4)
	// Set to 0 to disable background checkpointing (manual checkpoint only)
	OffsetCheckpointInterval time.Duration
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

// EngineConfig holds internal engine tuning parameters
// Phase 6G: Configurable hot-path parameters for performance optimization
type EngineConfig struct {
	// ========================================
	// Queue State Management
	// ========================================

	// AvailableChannelBuffer is the size of the lock-free available message ID channel
	// Larger buffer = more memory, more headroom for bursts
	// Smaller buffer = less memory, must be sized for throughput
	// Phase 6E: Reduced from 100M to 10M (612 MB memory savings)
	// Default: 10,000,000 (10M entries = 80 MB per queue)
	// Memory: 8 bytes per uint64 entry
	AvailableChannelBuffer int `json:"available_channel_buffer"`

	// ========================================
	// Ring Buffer (Hot Path)
	// ========================================

	// RingBufferSize is the size of the in-memory ring buffer (must be power of 2)
	// Larger = more messages in fast memory before spilling to WAL
	// Smaller = less memory, more frequent WAL writes
	// Default: 65,536 (64K messages)
	// Memory: ~100 bytes per slot = ~6.5 MB per queue
	RingBufferSize int `json:"ring_buffer_size"`

	// SpillThresholdPercent is when to start writing to WAL (0-100)
	// Higher = more tolerance for bursts, but less safety margin
	// Lower = more conservative, earlier WAL writes
	// Default: 80 (start spilling at 80% full = 51,200 messages)
	SpillThresholdPercent int `json:"spill_threshold_percent"`

	// ========================================
	// Write-Ahead Log (WAL)
	// ========================================

	// WALBatchSize is how many messages to batch before flushing WAL
	// Larger = better throughput, higher latency, more data loss risk on crash
	// Smaller = lower latency, more fsync calls, lower throughput
	// Default: 1,000 messages
	WALBatchSize int `json:"wal_batch_size"`

	// WALBatchTimeout is max time to wait before flushing partial WAL batch
	// Longer = better batching, higher latency
	// Shorter = lower latency, less efficient batching
	// Default: 10ms
	WALBatchTimeout time.Duration `json:"wal_batch_timeout"`

	// WALFileSize is the max size of each WAL file before rotation
	// Larger = fewer files, longer replay on restart
	// Smaller = more files, faster replay, more file handles
	// Default: 536,870,912 (512 MB)
	WALFileSize int64 `json:"wal_file_size"`

	// WALChannelBuffer is the size of write/ack request channels to WAL
	// Larger = more buffering for bursts, more memory
	// Smaller = less memory, potential blocking under load
	// Default: 10,000
	WALChannelBuffer int `json:"wal_channel_buffer"`

	// ========================================
	// Segment Storage (Cold Path)
	// ========================================

	// SegmentSize is the max size of each segment file before creating new one
	// Larger = fewer files, longer compaction time
	// Smaller = more files, faster compaction, more file handles
	// Default: 1,073,741,824 (1 GB)
	SegmentSize int64 `json:"segment_size"`

	// SegmentCheckpointInterval is how often to checkpoint segment metadata
	// Longer = less I/O overhead, more replay on restart
	// Shorter = more I/O overhead, faster restart
	// Default: 5 minutes
	SegmentCheckpointInterval time.Duration `json:"segment_checkpoint_interval"`

	// CompactionThreshold is the fraction of deleted messages to trigger compaction (0.0-1.0)
	// Higher = less frequent compaction, more wasted space
	// Lower = more frequent compaction, less wasted space, more I/O
	// Default: 0.5 (50% deleted)
	CompactionThreshold float64 `json:"compaction_threshold"`

	// CompactionInterval is how often to check for segments needing compaction
	// Longer = less overhead checking, slower space reclamation
	// Shorter = more frequent checks, faster space reclamation
	// Default: 30 minutes
	CompactionInterval time.Duration `json:"compaction_interval"`

	// ========================================
	// Consumer Delivery
	// ========================================

	// ConsumerSelectTimeout is how long to wait when no messages available
	// Longer = less CPU spinning, higher latency when messages arrive
	// Shorter = lower latency, more CPU usage
	// Default: 500 microseconds
	ConsumerSelectTimeout time.Duration `json:"consumer_select_timeout"`

	// ConsumerMaxBatchSize is max messages to deliver per consumer per poll
	// Larger = better throughput, potential unfairness between consumers
	// Smaller = better fairness, lower throughput
	// Default: 100
	ConsumerMaxBatchSize int `json:"consumer_max_batch_size"`

	// ========================================
	// Background Maintenance
	// ========================================

	// ExpiredMessageCheckInterval is how often to scan for expired messages
	// Longer = less overhead, slower TTL enforcement
	// Shorter = more overhead, stricter TTL enforcement
	// Default: 60 seconds
	ExpiredMessageCheckInterval time.Duration `json:"expired_message_check_interval"`

	// WALCleanupCheckInterval is how often to check for old WAL files to delete
	// Longer = slower cleanup, more disk usage
	// Shorter = faster cleanup, more overhead
	// Default: 5 minutes
	WALCleanupCheckInterval time.Duration `json:"wal_cleanup_check_interval"`

	// OffsetCleanupBatchSize is how many old offsets to delete per cleanup cycle
	// Larger = faster cleanup, more I/O per cycle
	// Smaller = slower cleanup, less I/O impact
	// Default: 1,000
	OffsetCleanupBatchSize int `json:"offset_cleanup_batch_size"`

	// OffsetCleanupInterval is how often to cleanup acknowledged message offsets
	// Longer = less overhead, more stale offsets in memory
	// Shorter = more overhead, cleaner memory
	// Default: 30 seconds
	OffsetCleanupInterval time.Duration `json:"offset_cleanup_interval"`
}
