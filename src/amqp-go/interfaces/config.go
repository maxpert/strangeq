package interfaces

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

	// PublishInflightBudgetBytes (WS-I Phase B) bounds DECLARED publish body
	// bytes (plus fixed framing) in flight between the content-header read
	// and durability/return/teardown — the header-time fair publish lease G.
	// It covers exactly that one quantity; the per-connection frame budget,
	// reader overflow cap, queue-resident messages, WAL staged buffers and tx
	// staging are bounded (or, for tx, unbounded) separately. 0 → the default
	// 4 × WAL batch seal (256 MiB); −1 → disabled; any other value must be
	// ≥ 1 MiB. Tied to the WAL byte target, never re-tuned after the fact.
	PublishInflightBudgetBytes int64
}

// Accepted values of StorageConfig.CrashModel. The empty string is also
// accepted and means CrashModelBarrier (the shipped default).
const (
	// CrashModelBarrier is the default confirm primitive: fcntl(F_BARRIERFSYNC)
	// on macOS. A confirmed message survives a process kill and an OS crash; a
	// sudden power loss can still lose the most recent confirmed messages that
	// the drive had accepted but not yet written from its volatile cache.
	CrashModelBarrier = "barrier"

	// CrashModelForceFlush is the opt-in full drive-cache flush:
	// fcntl(F_FULLFSYNC) on macOS. Confirmed messages then also survive power
	// loss, restoring the pre-barrier behaviour exactly.
	CrashModelForceFlush = "force_flush"
)

// Accepted values of StorageConfig.Durability — the single user-facing
// durability option. Each names the crash class a confirmed message is
// guaranteed to survive on darwin; on Linux and every non-darwin platform
// barrier and flush both fdatasync per confirmed message (which already flushes
// the drive cache), so barrier vs flush is inert there, but none issues no
// group-commit barrier at all and is NOT power-loss safe.
const (
	// DurabilityNone issues no WAL group-commit barrier (the retired
	// fsync: false): a confirmed message survives the broker staying up but
	// not a process kill. Transactions (WriteTxAtomic) always fsync.
	DurabilityNone = "none"

	// DurabilityBarrier is the default: fcntl(F_BARRIERFSYNC) on macOS. A
	// confirmed message survives a process kill and an OS crash; a sudden
	// power loss can still lose the most recent confirmed messages the drive
	// accepted but had not written from its volatile cache (the retired
	// fsync: true + crash_model: barrier).
	DurabilityBarrier = "barrier"

	// DurabilityFlush is the full drive-cache flush: fcntl(F_FULLFSYNC) on
	// macOS, so confirmed messages also survive power loss (the retired
	// fsync: true + crash_model: force_flush).
	DurabilityFlush = "flush"
)

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	// Path is the directory where all persistent data is stored
	Path string

	// Durability is the single user-facing durability option. It is one of
	// DurabilityNone, DurabilityBarrier (the default when the key is absent or
	// ""), or DurabilityFlush:
	//
	//   none    - no WAL group-commit barrier (the retired fsync: false).
	//   barrier - fcntl(F_BARRIERFSYNC) on macOS: a confirmed message survives
	//             a process kill and an OS crash; a power loss may still lose
	//             the drive-cache tail (the retired fsync: true + barrier).
	//   flush   - fcntl(F_FULLFSYNC) on macOS: confirmed messages also survive
	//             power loss (the retired fsync: true + force_flush).
	//
	// GetEngine maps this onto the internal EngineConfig transport
	// (WALSyncDisabled / CrashModel); nothing under the storage engine reads
	// this field directly. Any value other than the three above is rejected by
	// Validate. Transactions (WriteTxAtomic) always fdatasync regardless.
	Durability string

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

	// UnsafeRecovery (--unsafe-recovery) lets the broker START on a data
	// directory whose recovery could not be completed safely, DISCARDING
	// whatever could not be interpreted. Zero value (false) is the safe
	// default: any recovery outcome worse than RecoveryBenign refuses the boot
	// with a message naming the artifact, the cause, and this flag.
	//
	// This is a data-loss switch, not a tuning knob. With it set the broker
	// boots, logs one ERROR line per discarded artifact naming it BY PATH,
	// pins a process-lifetime metric, and keeps re-warning for as long as it
	// runs. Never set it in a config file "just in case" — a directory that
	// needs it is a directory whose durable contents are already forfeit.
	UnsafeRecovery bool
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

	// AuthFailureLimit is the per-peer failed-authentication budget (default
	// 5). A peer that exhausts it is refused by access policy BEFORE bcrypt
	// runs, so credential guessing costs the broker no CPU. 0 disables the
	// limiter. Peers are keyed per IPv4 address and per IPv6 /64.
	AuthFailureLimit int

	// AuthFailureWindowMS is the refill window for AuthFailureLimit in
	// milliseconds (default 60000). The bucket refills continuously: limit
	// tokens per window.
	AuthFailureWindowMS int64
}

// TelemetryConfig configures the optional Prometheus/health HTTP endpoint.
type TelemetryConfig struct {
	Enabled bool   // default false
	Address string // default "127.0.0.1" (loopback only)
	Port    int    // default 9419
	// PprofEnabled registers /debug/pprof/* and enables mutex/block
	// profiling. Off by default: heap and goroutine dumps are
	// unauthenticated and can leak message metadata.
	PprofEnabled bool
}

// ManagementConfig configures the management (control-plane) HTTP surface
// (spec mgmt-01 §2). The zero value is the safe default: DISABLED, loopback
// bind, broker-auth inherited, mutations refused. Every key is deniable —
// there is deliberately no capability-requirement knob (AUTH-023).
//
// Canonical keys: management.{enabled,address,port,node_name,authentication,
// allow_unauthenticated,allow_unauthenticated_mutations,allow_connection_close,
// admin_users,trusted_proxies,basic.*,tls.*,cors.*,session.*,limits.*,
// rate_limits.*,definitions.*,async.*,get.*,audit.*}
type ManagementConfig struct {
	Enabled        bool   `json:"enabled"`        // default false (SEC-001)
	Address        string `json:"address"`        // default "127.0.0.1" (SEC-002)
	Port           int    `json:"port"`           // default 15672
	NodeName       string `json:"node_name"`      // "" -> "amqp-go@"+hostname
	Authentication string `json:"authentication"` // inherit|basic|none

	AllowUnauthenticated          bool `json:"allow_unauthenticated"`
	AllowUnauthenticatedMutations bool `json:"allow_unauthenticated_mutations"`
	AllowConnectionClose          bool `json:"allow_connection_close"`

	AdminUsers     []string `json:"admin_users"`
	TrustedProxies []string `json:"trusted_proxies"` // CIDRs (SEC-112)

	Basic ManagementBasicConfig `json:"basic"`
	TLS   ManagementTLSConfig   `json:"tls"`
	CORS  ManagementCORSConfig  `json:"cors"`

	Session     ManagementSessionConfig     `json:"session"`
	Limits      ManagementLimitsConfig      `json:"limits"`
	RateLimits  ManagementRateLimitsConfig  `json:"rate_limits"`
	Definitions ManagementDefinitionsConfig `json:"definitions"`
	Async       ManagementAsyncConfig       `json:"async"`
	Get         ManagementGetConfig         `json:"get"`
	Audit       ManagementAuditConfig       `json:"audit"`
}

// ManagementBasicConfig is management.basic.* (AUTH-003).
type ManagementBasicConfig struct {
	Username     string `json:"username"`
	PasswordHash string `json:"password_hash"`
	Admin        bool   `json:"admin"`
}

// ManagementTLSConfig is management.tls.*. mTLS identity mapping is deferred
// (OQ-015), so a CA file is accepted but client-certificate authentication is
// refused until a mapping exists (secure default).
type ManagementTLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
}

// ManagementCORSConfig is management.cors.*. Empty means CORS is off (SEC-094).
type ManagementCORSConfig struct {
	AllowedOrigins []string `json:"allowed_origins"`
}

// ManagementSessionConfig is management.session.* (AUTH-031/032, SEC-040/042).
type ManagementSessionConfig struct {
	IdleTimeoutSeconds int `json:"idle_timeout_seconds"` // default 28800
	MaxSessions        int `json:"max_sessions"`         // default 1024
}

// ManagementLimitsConfig is management.limits.*.
type ManagementLimitsConfig struct {
	MaxPageSize           int `json:"max_page_size"` // default 500
	RequestTimeoutSeconds int `json:"request_timeout_seconds"`
	MaxInFlight           int `json:"max_in_flight"` // default 64 (SEC-061)
	MaxBodyBytes          int `json:"max_body_bytes"`
	MaxImportBodyBytes    int `json:"max_import_body_bytes"`
	MaxResponseBytes      int `json:"max_response_bytes"` // default 4 MiB
	MaxNameBytes          int `json:"max_name_bytes"`     // default 256 (SEC-085)
}

// ManagementRateLimitsConfig is management.rate_limits.* (SEC-067/068/069).
// Zero means unlimited.
type ManagementRateLimitsConfig struct {
	Publish                  int `json:"publish"`
	Get                      int `json:"get"`
	Purge                    int `json:"purge"`
	Declare                  int `json:"declare"`
	DefinitionsImportSeconds int `json:"definitions_import_seconds"`
}

// ManagementDefinitionsConfig is management.definitions.* (COMPAT-027).
type ManagementDefinitionsConfig struct {
	MaxQueues    int `json:"max_queues"`
	MaxExchanges int `json:"max_exchanges"`
	MaxBindings  int `json:"max_bindings"`
}

// ManagementAsyncConfig is management.async.* (FR-API-140…143, FR-CONV-100).
type ManagementAsyncConfig struct {
	MessageThreshold    int `json:"message_threshold"` // default 10000
	EntityThreshold     int `json:"entity_threshold"`  // default 1000
	MaxJobs             int `json:"max_jobs"`          // default 128
	JobRetentionSeconds int `json:"job_retention_seconds"`
}

// ManagementGetConfig is management.get.* (FR-API-081).
type ManagementGetConfig struct {
	MaxCount int `json:"max_count"` // default 100
}

// ManagementAuditConfig is management.audit.*.
type ManagementAuditConfig struct {
	Logfile string `json:"logfile"`
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

	// MaxQueues caps the number of queues (all kinds: durable, transient,
	// exclusive, auto-delete, server-named). 0 = unlimited.
	MaxQueues int
	// MaxExchanges caps the number of user-declared exchanges; the default
	// exchange and amq.* are not counted. 0 = unlimited.
	MaxExchanges int

	// Timeouts and intervals (in milliseconds)
	CleanupIntervalMS int64 // Background cleanup interval
}

// DefaultMaxQueueBacklog is the shipped per-queue publisher backpressure depth
// high-water mark, in MESSAGES. It is the value the retired pre-segstore knobs
// implied: RingBufferSize (65536) x SpillThresholdPercent (80) / 100 = 52428.
// An EngineConfig.MaxQueueBacklog of 0 means "use this default".
const DefaultMaxQueueBacklog uint64 = 52428

// EngineConfig holds internal engine tuning parameters
// Phase 6G: Configurable hot-path parameters for performance optimization
type EngineConfig struct {
	// ========================================
	// Publisher Backpressure Depth
	// ========================================

	// MaxQueueBacklog is the per-queue publisher backpressure depth high-water
	// mark in MESSAGES: at this many messages in flight (head - minAckCursor)
	// the publisher blocks or spills. It replaces the retired
	// RingBufferSize x SpillThresholdPercent derivation and the separate legacy
	// depth-override key (both rejected at boot now).
	//
	// It is a SECONDARY gate: the hot-cache BYTE budget (HotCacheQueueBytes)
	// usually binds first (see broker.publisherGate).
	//
	// 0 means the default (DefaultMaxQueueBacklog = 52428).
	MaxQueueBacklog uint64 `json:"max_queue_backlog"`

	// ========================================
	// Hot Cache (Segstore v1)
	// ========================================

	// HotCacheQueueBytes is the per-queue byte budget for the in-memory hot
	// cache that serves recent messages without disk reads. 0 (default) maps to
	// 64 MiB. Range: [1, 64] MiB.
	//
	// The default is sized so the ready set is not SMALLER than the 64K-message
	// ring the hot cache replaces (80% spill threshold = 52,428 messages): a
	// smaller budget makes the LRU evict the FIFO consumer's next target, which
	// turns every read into a cold read.
	HotCacheQueueBytes int64 `json:"hot_cache_queue_bytes"`

	// HotCacheGlobalBytes is the global byte budget shared across all queues
	// for the hot cache. Must be >= HotCacheQueueBytes and <= 256 MiB.
	// 0 (default) maps to 256 MiB.
	HotCacheGlobalBytes int64 `json:"hot_cache_global_bytes"`
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
	// Default: 5 (5ms)
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

	// WALSyncDisabled is the internal (inverted-sense) transport for the
	// StorageConfig.Durability "none" level. Zero value (false) = fsync ON
	// (the durable default), so every zero-value EngineConfig{} — used by many
	// constructors and tests — stays durable. Only the config layer sets it,
	// from GetEngine(): a durability of none sets it true. When true, the WAL
	// group-commit fdatasync is skipped. It never disables the transaction
	// fsync (WriteTxAtomic is always durable).
	WALSyncDisabled bool `json:"wal_sync_disabled"`

	// CrashModel is the internal transport for the StorageConfig.Durability
	// "flush" level. Zero value ("") and "barrier" both request the
	// group-commit and transaction confirm barriers issue
	// fcntl(F_BARRIERFSYNC) on darwin; "force_flush" requests
	// fcntl(F_FULLFSYNC), the pre-barrier full drive-cache flush. On Linux and
	// every non-darwin platform both behave identically (fdatasync), so the
	// value is inert there. Only the config layer sets it, from GetEngine();
	// storage maps anything other than "force_flush" to the barrier default,
	// so a zero-value EngineConfig{} stays on the safe-by-default barrier.
	CrashModel string `json:"wal_crash_model"`

	// Durability is the internal MIRROR of the user-facing
	// StorageConfig.Durability choice. It is written only by the config
	// layer's GetEngine mapping and is NOT read by the storage engine, which
	// consumes WALSyncDisabled and CrashModel above. It exists so an operator
	// or tool reading EngineConfig can see the selected mode without
	// re-deriving it from the two transport fields.
	Durability string `json:"wal_durability"`

	// UnsafeRecovery is the internal transport for the user-facing
	// StorageConfig.UnsafeRecovery flag (--unsafe-recovery). Zero value
	// (false) = refuse to start on an unrecoverable data directory, which is
	// the safe default for every zero-value EngineConfig{} used by
	// constructors and tests. Only the config layer sets it, from GetEngine().
	// It reaches storage construction because the first thing that can prove a
	// directory unrecoverable — a WAL file whose framing version this build
	// cannot parse — is detected in createSharedWAL, before any server object
	// exists.
	UnsafeRecovery bool `json:"unsafe_recovery"`

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
	// Default: 67,108,864 (64 MiB)
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
	// Default: 1000 (1 second)
	WALCleanupCheckIntervalMS int64 `json:"wal_cleanup_check_interval_ms"`
}
