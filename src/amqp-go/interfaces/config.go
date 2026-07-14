package interfaces

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

	// Heartbeat interval in milliseconds
	HeartbeatIntervalMS int64

	// TCP keepalive settings
	TCPKeepAlive           bool
	TCPKeepAliveIntervalMS int64 // TCP keepalive interval in milliseconds

	// Socket buffer sizes (SQ-2). Applied as SO_RCVBUF / SO_SNDBUF on accepted
	// TCP connections (TLS is unwrapped to its underlying socket). Default 256
	// KiB each; operators may raise up to ~1 MiB for high-bandwidth links. <=0
	// leaves the OS default in place.
	ReadBufferSize  int
	WriteBufferSize int

	// ReadCoalesceBufferSize is the size (bytes) of the per-connection
	// bufio.Reader that coalesces frame reads (SQ-1). Larger buffers amortize
	// more frames per read syscall at the cost of per-connection memory.
	// Default: 65536 (64 KiB). <=0 falls back to the default.
	ReadCoalesceBufferSize int

	// ReaderOverflowFlowBytes is the per-connection reader-overflow backlog (in
	// bytes of buffered inbound frames) at which the server asserts
	// channel.flow(active=false) to the client, asking it to pause publishing.
	// The reader keeps draining the socket (so acks keep flowing) while the
	// backlog persists, and resumes with channel.flow(active=true) once it fully
	// drains. Best-effort: a client that ignores flow keeps growing the backlog
	// until the hard cap. Default: 8 MiB. 0 disables the flow signal.
	ReaderOverflowFlowBytes int64

	// ReaderOverflowHardCapBytes is the hard per-connection cap on the
	// reader-overflow backlog. When the buffered inbound frame bytes exceed it,
	// the connection is closed (DoS / runaway-publisher protection) rather than
	// growing memory without bound. Must exceed ReaderOverflowFlowBytes.
	// Default: 64 MiB. 0 disables the cap (unbounded — not recommended).
	ReaderOverflowHardCapBytes int64

	// ReaderBackpressureProbeMS bounds how long the reader blocks handing a
	// buffered frame to a full FrameQueue before it breaks out to read exactly
	// one more frame off the socket (SQ-13). While backlogged the reader blocks
	// on the FrameQueue hand-off rather than reading more publishes — not reading
	// IS the TCP backpressure that paces the producer to the consumer's drain
	// rate. The probe interval keeps acks and control frames trapped behind a
	// flood on a shared connection flowing (read within one interval), preventing
	// the ack-starvation / consume-staleness deadlocks. Smaller = more responsive
	// ack draining under sustained backpressure at the cost of more wakeups.
	// Default: 5 ms. <=0 falls back to the default.
	ReaderBackpressureProbeMS int64
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	// Path is the directory where all persistent data is stored
	Path string

	// Fsync controls the WAL group-commit durability barrier. It is a TRI-STATE
	// pointer so an omitted config key is distinguishable from an explicit false:
	//   nil   = default (fsync ON, durable) — the safe default
	//   true  = fsync after each WAL batch (safer but slower)
	//   false = skip the group-commit fsync (faster, relies on OS page cache;
	//           NOT crash-durable). Only an EXPLICIT false disables durability.
	// The tri-state matters because the --config load path seeds a zero-value
	// AMQPConfig{} before Load, so an omitted key MUST mean "default ON" rather
	// than Go's zero false — otherwise honoring the flag would silently make any
	// fsync-omitting config non-durable. Transactions (WriteTxAtomic) always
	// fdatasync regardless of this flag.
	Fsync *bool

	// CRCCheck controls whether WAL/segment records carry a CRC32 integrity
	// checksum. It is a TRI-STATE pointer mirroring Fsync:
	//   nil   = default (CRC ON, safe) — the safe default
	//   true  = compute and verify CRC32 per record (safe but costs ~8% CPU)
	//   false = skip CRC computation (write zero, skip verification on read;
	//           faster, no integrity check). Only an EXPLICIT false disables it.
	// The record format is unchanged: the CRC field is always present, just zero
	// when disabled. The read path skips verification when the stored CRC is
	// zero, so mixed files (some with CRC, some without) are handled gracefully.
	CRCCheck *bool

	// Message settings
	MessageTTL int64 // Message TTL in seconds (0 = no TTL)

	// Performance settings
	CacheMB  int64 // Metadata cache size in megabytes
	MaxFiles int   // Maximum open file handles

	// RetentionMS is how long to keep old segments before compaction (milliseconds)
	// Example: 86400000 = 24 hours
	RetentionMS int64

	// CheckpointIntervalMS is how often to save consumer offset positions (milliseconds)
	// Set to 0 to disable background checkpointing (manual checkpoint only)
	// Example: 5000 = 5 seconds
	CheckpointIntervalMS int64
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

// ResourceAlarmConfig configures the RabbitMQ-style resource alarms (SQ-12):
// when the broker process is low on memory or the storage disk is low on free
// space, publishers are throttled (connection.blocked is emitted to opted-in
// clients and their readers are paused for TCP backpressure) until the pressure
// clears. These fields are named distinctly from the Security.Blocked* auth
// allow/deny lists, which are unrelated.
//
// When Enabled is false — or both arms resolve to a zero threshold — the alarm
// monitor goroutine is never spawned and the broker does zero RSS/Statfs
// sampling (the unset zero-cost contract). Thresholds default to RabbitMQ's own
// defaults so flipping Enabled behaves like RabbitMQ out of the box.
type ResourceAlarmConfig struct {
	// Enabled turns the resource-alarm monitor on. Default false (dormant):
	// no monitor goroutine, no memory/disk sampling.
	Enabled bool `json:"enabled"`

	// MemoryHighWatermark is the fraction of detected total memory (cgroup limit
	// or machine RAM) at or above which process RSS trips the memory alarm —
	// the vm_memory_high_watermark equivalent. Default 0.4. A value <= 0
	// disables the memory arm.
	MemoryHighWatermark float64 `json:"memory_high_watermark"`

	// DiskFreeLimitBytes is the free-space floor (in bytes) on the storage disk
	// below which the disk alarm trips — the disk_free_limit equivalent.
	// Default 52428800 (50 MB). A value <= 0 disables the disk arm.
	DiskFreeLimitBytes int64 `json:"disk_free_limit_bytes"`

	// Hysteresis is the fractional margin between the set and clear thresholds
	// used to prevent alarm flapping. The memory alarm clears when RSS falls to
	// MemoryHighWatermark*(1-Hysteresis) of total; the disk alarm clears when
	// free space rises to DiskFreeLimitBytes*(1+Hysteresis). Default 0.05 (5%).
	Hysteresis float64 `json:"hysteresis"`

	// CheckIntervalMS is the alarm monitor's sampling cadence in milliseconds —
	// deliberately faster than the 60s system-metrics ticker so alarms react
	// promptly. Default 2000 (2s). A value <= 0 falls back to the default.
	CheckIntervalMS int64 `json:"check_interval_ms"`
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

	// Timeouts and intervals (in milliseconds)
	CleanupIntervalMS int64 // Background cleanup interval
}

// EngineConfig holds internal engine tuning parameters
// Phase 6G: Configurable hot-path parameters for performance optimization
type EngineConfig struct {
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

	// DepthHighWMOverride, when > 0, directly sets the per-queue backpressure
	// high-water mark instead of deriving it from RingBufferSize ×
	// SpillThresholdPercent. Lowering it trades throughput for latency: a
	// shallower queue means messages spend less time waiting. 0 (default) keeps
	// the computed HWM (zero behavior regression).
	DepthHighWMOverride uint64 `json:"depth_high_wm_override"`

	// ========================================
	// Write-Ahead Log (WAL)
	// ========================================

	// WALBatchSize is how many messages to batch before flushing WAL
	// Larger = better throughput, higher latency, more data loss risk on crash
	// Smaller = lower latency, more fsync calls, lower throughput
	// Default: 1,000 messages
	WALBatchSize int `json:"wal_batch_size"`

	// WALBatchTimeoutMS is retained for configuration compatibility but no
	// longer gates WAL batch flushing: the batch writer flushes as soon as its
	// request channel is drained (drain-then-flush group commit), so a lone
	// synchronous writer is never stalled waiting for a batch timer.
	// Default: 10 (10ms)
	WALBatchTimeoutMS int64 `json:"wal_batch_timeout_ms"`

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

	// WALCRCDisabled is the internal (inverted-sense) transport for the user
	// Storage.CRCCheck flag. Zero value (false) = CRC ON (the safe default),
	// so every zero-value EngineConfig{} — used by many constructors and tests —
	// keeps CRC verification. Only the config layer sets it, from GetEngine():
	// WALCRCDisabled = !CRCEnabled(). When true, the WAL and segment write
	// paths skip CRC32 computation (write zero in the CRC field) and the read
	// path skips verification for zero-CRC records.
	WALCRCDisabled bool `json:"wal_crc_disabled"`

	// WALSyncDisabled is the internal (inverted-sense) transport for the user
	// Storage.Fsync flag. Zero value (false) = fsync ON (the durable default),
	// so every zero-value EngineConfig{} — used by many constructors and tests —
	// stays durable. Only the config layer sets it, from GetEngine():
	// WALSyncDisabled = !FsyncEnabled(). When true, the WAL group-commit
	// fdatasync is skipped (Storage.Fsync=false). It never disables the
	// transaction fsync (WriteTxAtomic is always durable).
	WALSyncDisabled bool `json:"wal_sync_disabled"`

	// SharedBodyThreshold (ITER5) is the minimum body size, in bytes, at which a
	// DURABLE fan-out to >=2 queues writes the body ONCE as a shared WAL BodyBlock
	// plus one tiny reference record per target, instead of one full-body copy per
	// target. Below it (and for single-queue or transient publishes) every copy is
	// inline, byte-for-byte as before. 0 => the default (4096). Set very large to
	// effectively disable the feature.
	// Default: 4096
	SharedBodyThreshold int `json:"shared_body_threshold"`

	// SharedBodyMaxPinAgeMS (ITER5 cold-tail hardening) bounds, in milliseconds,
	// how long a rolled WAL file carrying a shared BodyBlock may be kept out of
	// checkpoint before it is force-re-inlined (N copies) and deleted. 0 disables
	// the age backstop and relies on WALCleanupCheckIntervalMS/retention: a
	// permanently-unacked reference then pins its file holding exactly ONE body
	// copy (never the N-copy blowup).
	// Default: 0 (disabled)
	SharedBodyMaxPinAgeMS int64 `json:"shared_body_max_pin_age_ms"`

	// ========================================
	// Segment Storage (Cold Path)
	// ========================================

	// SegmentSize is the max size of each segment file before creating new one
	// Larger = fewer files, longer compaction time
	// Smaller = more files, faster compaction, more file handles
	// Default: 1,073,741,824 (1 GB)
	SegmentSize int64 `json:"segment_size"`

	// SegmentCheckpointIntervalMS is how often to checkpoint segment metadata (milliseconds)
	// Longer = less I/O overhead, more replay on restart
	// Shorter = more I/O overhead, faster restart
	// Default: 300000 (5 minutes)
	SegmentCheckpointIntervalMS int64 `json:"segment_checkpoint_interval_ms"`

	// CompactionThreshold is the fraction of deleted messages to trigger compaction (0.0-1.0)
	// Higher = less frequent compaction, more wasted space
	// Lower = more frequent compaction, less wasted space, more I/O
	// Default: 0.5 (50% deleted)
	CompactionThreshold float64 `json:"compaction_threshold"`

	// CompactionIntervalMS is how often to check for segments needing compaction (milliseconds)
	// Longer = less overhead checking, slower space reclamation
	// Shorter = more frequent checks, faster space reclamation
	// Default: 1800000 (30 minutes)
	CompactionIntervalMS int64 `json:"compaction_interval_ms"`

	// ========================================
	// Consumer Delivery
	// ========================================

	// ConsumerSelectTimeoutMS is how long to wait when no messages available (milliseconds)
	// Longer = less CPU spinning, higher latency when messages arrive
	// Shorter = lower latency, more CPU usage
	// Default: 1 (500 microseconds rounded up to 1ms)
	ConsumerSelectTimeoutMS int64 `json:"consumer_select_timeout_ms"`

	// ConsumerMaxBatchSize is max messages to deliver per consumer per poll
	// Larger = better throughput, potential unfairness between consumers
	// Smaller = better fairness, lower throughput
	// Default: 100
	ConsumerMaxBatchSize int `json:"consumer_max_batch_size"`

	// UnlimitedPrefetchCap is the finite prefetch-gate cap applied to a
	// manual-ack consumer that requests prefetch-count 0 ("unlimited" per AMQP
	// 0.9.1). A true unbounded unacked set makes the ack cursor's lowest-unacked
	// rescan O(n) per ack; a finite cap keeps it small. The cap MUST exceed the
	// coarsest client ack cadence you intend to support: a consumer that
	// multi-acks every N messages needs a window > N to ever emit the ack that
	// reopens the gate (e.g. multi-ack-every-1000 requires a cap of at least
	// ~1024). No-ack consumers ignore this entirely (they bypass the gate).
	// Default: 2000
	UnlimitedPrefetchCap int `json:"unlimited_prefetch_cap"`

	// ========================================
	// Background Maintenance
	// ========================================

	// WALCleanupCheckIntervalMS is how often to check for old WAL files to delete (milliseconds)
	// Longer = slower cleanup, more disk usage
	// Shorter = faster cleanup, more overhead
	// Default: 300000 (5 minutes)
	WALCleanupCheckIntervalMS int64 `json:"wal_cleanup_check_interval_ms"`
}
