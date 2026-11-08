package config

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/storage"
	yamlv3 "gopkg.in/yaml.v3"
)

// boolPtr returns a pointer to b. Used for tri-state config flags (see
// StorageConfig.Fsync) where a non-nil pointer expresses an explicit choice.
func boolPtr(b bool) *bool { return &b }

// DefaultConfig creates a configuration with sensible defaults
func DefaultConfig() *AMQPConfig {
	return &AMQPConfig{
		Network: interfaces.NetworkConfig{
			Address:                "127.0.0.1:5672",
			Port:                   5672,
			MaxConnections:         1000,
			HeartbeatIntervalMS:    60000, // 60 seconds
			TCPKeepAlive:           true,
			TCPKeepAliveIntervalMS: 30000,      // 30 seconds
			ReadBufferSize:         256 * 1024, // 256 KiB SO_RCVBUF (SQ-2)
			WriteBufferSize:        256 * 1024, // 256 KiB SO_SNDBUF (SQ-2)
			ReadCoalesceBufferSize: 64 * 1024,  // 64 KiB bufio read-coalescing buffer (SQ-1)

			ReaderOverflowFlowBytes:    8 << 20,  // 8 MiB: assert channel.flow(false)
			ReaderOverflowHardCapBytes: 64 << 20, // 64 MiB: close the connection
			ReaderBackpressureProbeMS:  5,        // 5 ms reader-backpressure probe (SQ-13)
			// WS-I Phase B: 0 = default publish-inflight lease budget
			// (4 × storage.WALBatchSealBytesDefault = 256 MiB).
			PublishInflightBudgetBytes: 0,
		},
		Storage: interfaces.StorageConfig{
			Path: "./data",
			// Durable by default: the barrier. Set durability: none to opt out
			// of the group-commit fence, or durability: flush for the full
			// F_FULLFSYNC drive-cache flush (power-loss safe). Emitted by
			// --generate-config so the no-config path is unambiguous.
			Durability:  interfaces.DurabilityBarrier,
			CRCCheck:    boolPtr(true),
			CacheMB:     64,       // 64 MB metadata cache
			MaxFiles:    100,      // Max open file handles
			RetentionMS: 86400000, // 24 hours
		},
		Security: interfaces.SecurityConfig{
			TLSEnabled:             false,
			TLSCertFile:            "",
			TLSKeyFile:             "",
			TLSCAFile:              "",
			AuthenticationEnabled:  false,
			AuthenticationBackend:  "file",
			AuthenticationConfig:   make(map[string]interface{}),
			AuthenticationFilePath: "./auth.json",
			AuthMechanisms:         []string{"PLAIN"},
			AuthorizationEnabled:   false,
			DefaultVHost:           "/",
			AllowedUsers:           []string{},
			BlockedUsers:           []string{},
			AllowedHosts:           []string{},
			BlockedHosts:           []string{},
			AuthFailureLimit:       5,
			AuthFailureWindowMS:    60000,
		},
		ResourceAlarms: interfaces.ResourceAlarmConfig{
			// Dormant by default so the unset zero-cost contract holds under
			// sustained-throughput benches (no monitor goroutine, no RSS/Statfs
			// sampling). Thresholds match RabbitMQ's defaults so an operator who
			// flips Enabled=true gets RabbitMQ-equivalent behavior.
			Enabled:             false,
			MemoryHighWatermark: 0.4,              // 40% of total RAM (vm_memory_high_watermark)
			DiskFreeLimitBytes:  50 * 1024 * 1024, // 50 MB free-space floor (disk_free_limit)
			Hysteresis:          0.05,             // 5% set/clear margin to avoid flapping
			CheckIntervalMS:     2000,             // 2s monitor cadence (faster than the 60s metrics ticker)
		},
		Telemetry: interfaces.TelemetryConfig{
			Enabled:      false,
			Address:      "127.0.0.1",
			Port:         9419,
			PprofEnabled: false,
		},
		Server: interfaces.ServerConfig{
			Name:                     "amqp-go-server",
			Version:                  "0.1.0-dev",
			Product:                  "AMQP-Go",
			Platform:                 "Go",
			Copyright:                "Maxpert AMQP-Go Server",
			LogLevel:                 "info",
			LogFile:                  "",
			PidFile:                  "",
			Daemonize:                false,
			MaxChannelsPerConnection: 2047,
			MaxFrameSize:             131072,   // 128KB
			MaxMessageSize:           16777216, // 16MB
			MaxQueues:                0,        // 0 = unlimited
			MaxExchanges:             0,        // 0 = unlimited
			CleanupIntervalMS:        300000,   // 5 minutes
		},
		Engine: interfaces.EngineConfig{
			// Ring Buffer (Hot Path) — retained as legacy message-count depth-HWM input
			RingBufferSize:        65536, // 64K messages = ~6.5 MB per queue
			SpillThresholdPercent: 80,    // Start spilling at 80% = 51,200 messages
			DepthHighWMOverride:   0,     // 0 = compute HWM from ring size × spill % (default)

			// Write-Ahead Log (WAL)
			WALBatchSize:      1000,              // 1,000 messages per batch
			WALBatchTimeoutMS: 5,                 // retained for compatibility; flushing is drain-then-flush, not timer-gated
			WALFileSize:       512 * 1024 * 1024, // 512 MB per WAL file (segstore v1)
			WALChannelBuffer:  10000,             // 10K buffered requests

			// Segment Storage (Cold Path)
			SegmentSize:                 64 * 1024 * 1024, // 64 MB per segment (segstore v1)
			SegmentCheckpointIntervalMS: 300000,           // 5 minutes
			CompactionThreshold:         0.5,              // Compact at 50% deleted
			CompactionIntervalMS:        1800000,          // 30 minutes

			// Hot Cache (Segstore v1)
			//
			// The per-queue budget is a BYTE budget, and it is deliberately
			// byte-bounded rather than entry-bounded.
			//
			// The hot cache replaces the 64K-slot ring, whose 80% spill
			// threshold was 52,428 ENTRIES -- an entry count, with no byte
			// bound at all, so its capacity in bytes grew with the body size.
			// An entry-count budget of 52,428 would therefore hold 3.3 GiB of
			// 64 KiB bodies for ONE queue, which is exactly the memory the
			// byte bound exists to prevent.
			//
			// 16 MiB was too small: at the charge formula
			// (512 + queue + body + ...) it holds only ~25.4k messages, and
			// the LRU evicts the OLDEST entry -- which is precisely a FIFO
			// consumer's next target -- so nearly every read became a cold
			// read and the durable queue's delivery rate was pinned to the
			// fsync cadence. 64 MiB roughly doubles the resident set.
			//
			// The byte budget holds the ring's 52,428 entries only for small
			// bodies: 129,553 at an empty body, 65,154 at 512 B, 52,184 at
			// 768 B, 43,520 at 1 KiB and 14,544 at 4 KiB. At 1 KiB and above
			// the resident set is smaller than the ring's, by design: that
			// is the memory the 64 KiB rows win on.
			//
			// The global budget stays at 256 MiB.
			HotCacheQueueBytes:  storage.DefaultHotCacheQueueBytes,  // 64 MiB per queue
			HotCacheGlobalBytes: storage.DefaultHotCacheGlobalBytes, // 256 MiB global

			// Consumer Delivery
			ConsumerSelectTimeoutMS: 1,    // 1 millisecond (500µs rounded up)
			ConsumerMaxBatchSize:    100,  // Max 100 messages per consumer per poll
			UnlimitedPrefetchCap:    2000, // Finite gate cap for prefetch-0 manual-ack consumers

			// Background Maintenance
			//
			// The WAL RECLAIM (unlink a file whose records are all
			// acknowledged) runs every second. Its LATENCY IS A MEMORY BOUND:
			// a file that is not reclaimed keeps its per-file inventory alive,
			// and at 64 KiB bodies the WAL rolls ~28 times a second, so a
			// slow reclaim retains thousands of files' worth of inventory.
			// The sweep itself is cheap -- it walks the old files and unlinks
			// the fully acknowledged ones; it does not touch the acknowledged
			// records themselves.
			//
			// MEASURED at 1 KiB bodies with 10 producers and 10 consumers:
			// a 5-second interval leaves 460 MB resident, a 1-second
			// interval 340 MB, and the throughput is the same (124,030 vs
			// 124,526 msg/s).
			WALCleanupCheckIntervalMS: 1000, // 1 second
		},
	}
}

// AMQPConfig implements the Config interface
type AMQPConfig struct {
	Network        interfaces.NetworkConfig       `json:"network"`
	Storage        interfaces.StorageConfig       `json:"storage"`
	Security       interfaces.SecurityConfig      `json:"security"`
	Server         interfaces.ServerConfig        `json:"server"`
	Engine         interfaces.EngineConfig        `json:"engine"`
	ResourceAlarms interfaces.ResourceAlarmConfig `json:"resource_alarms"`
	Telemetry      interfaces.TelemetryConfig     `json:"telemetry"`
}

// GetNetwork returns network configuration
func (c *AMQPConfig) GetNetwork() interfaces.NetworkConfig {
	return c.Network
}

// GetStorage returns storage configuration
func (c *AMQPConfig) GetStorage() interfaces.StorageConfig {
	return c.Storage
}

// GetSecurity returns security configuration
func (c *AMQPConfig) GetSecurity() interfaces.SecurityConfig {
	return c.Security
}

// GetServer returns server information configuration
func (c *AMQPConfig) GetServer() interfaces.ServerConfig {
	return c.Server
}

// Durability returns the effective durability level. An omitted key ("" on a
// zero-value Load caller) normalizes to DurabilityBarrier, the safe shipped
// default, so every zero-value path stays on the ordered barrier.
func (c *AMQPConfig) Durability() string {
	switch c.Storage.Durability {
	case interfaces.DurabilityNone, interfaces.DurabilityBarrier, interfaces.DurabilityFlush:
		return c.Storage.Durability
	default:
		return interfaces.DurabilityBarrier
	}
}

// CRCEnabled reports whether WAL/segment record CRC32 integrity checks are
// active. The tri-state Storage.CRCCheck defaults to ON: nil (omitted) or an
// explicit true both enable it; only an explicit false disables it.
func (c *AMQPConfig) CRCEnabled() bool {
	return c.Storage.CRCCheck == nil || *c.Storage.CRCCheck
}

// GetEngine returns engine tuning configuration. It maps the single
// StorageConfig.Durability option onto the internal (WALSyncDisabled,
// CrashModel) transport and the CRC knob onto WALCRCDisabled so the storage
// layer honors both while every zero-value path stays durable and
// integrity-checked:
//
//	none    -> WALSyncDisabled=true,  CrashModel=""  (skip the group-commit fence)
//	barrier -> WALSyncDisabled=false, CrashModel=""  (empty == fence; the default)
//	flush   -> WALSyncDisabled=false, CrashModel="force_flush"
//
// Storage.UnsafeRecovery rides the same overlay (false = refuse to start on an
// unrecoverable directory) because storage CONSTRUCTION is where an unparseable
// WAL framing version is first detected.
func (c *AMQPConfig) GetEngine() interfaces.EngineConfig {
	e := c.Engine
	e.WALCRCDisabled = !c.CRCEnabled()
	switch c.Durability() {
	case interfaces.DurabilityNone:
		e.WALSyncDisabled = true
		e.CrashModel = ""
	case interfaces.DurabilityFlush:
		e.WALSyncDisabled = false
		e.CrashModel = interfaces.CrashModelForceFlush
	default: // DurabilityBarrier
		e.WALSyncDisabled = false
		e.CrashModel = ""
	}
	e.Durability = c.Durability()
	e.UnsafeRecovery = c.Storage.UnsafeRecovery
	return e
}

// GetResourceAlarms returns the resource-alarm configuration (SQ-12).
func (c *AMQPConfig) GetResourceAlarms() interfaces.ResourceAlarmConfig {
	return c.ResourceAlarms
}

// Validate validates the configuration
func (c *AMQPConfig) Validate() error {
	// Validate network configuration
	if c.Network.Port <= 0 || c.Network.Port > 65535 {
		return fmt.Errorf("invalid network port: %d", c.Network.Port)
	}

	if c.Network.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive: %d", c.Network.MaxConnections)
	}

	if c.Network.ReaderBackpressureProbeMS < 0 {
		return fmt.Errorf("reader backpressure probe interval must be non-negative: %d ms", c.Network.ReaderBackpressureProbeMS)
	}

	// WS-I Phase B: the publish-inflight lease budget is -1 (disabled), 0
	// (default 256 MiB), or an explicit budget of at least 1 MiB.
	if v := c.Network.PublishInflightBudgetBytes; v < -1 || (v > 0 && v < 1<<20) {
		return fmt.Errorf("publish inflight budget must be -1 (disabled), 0 (default), or >= 1 MiB: %d", v)
	}

	// Validate storage configuration
	if c.Storage.Path == "" {
		return fmt.Errorf("storage path cannot be empty")
	}

	// The single durability option. "" is accepted for zero-value Load callers
	// and normalizes to the barrier default; anything else outside the three
	// documented values FAILS at boot rather than silently selecting a
	// durability level nobody asked for.
	switch c.Storage.Durability {
	case "", interfaces.DurabilityNone, interfaces.DurabilityBarrier, interfaces.DurabilityFlush:
	default:
		return fmt.Errorf("invalid storage.durability %q: allowed values are %q, %q, and %q (omit for the default %q)",
			c.Storage.Durability, interfaces.DurabilityNone, interfaces.DurabilityBarrier, interfaces.DurabilityFlush, interfaces.DurabilityBarrier)
	}

	// Validate security configuration
	if c.Security.TLSEnabled {
		if c.Security.TLSCertFile == "" || c.Security.TLSKeyFile == "" {
			return fmt.Errorf("TLS cert and key files required when TLS is enabled")
		}

		if _, err := os.Stat(c.Security.TLSCertFile); os.IsNotExist(err) {
			return fmt.Errorf("TLS cert file does not exist: %s", c.Security.TLSCertFile)
		}

		if _, err := os.Stat(c.Security.TLSKeyFile); os.IsNotExist(err) {
			return fmt.Errorf("TLS key file does not exist: %s", c.Security.TLSKeyFile)
		}

		if c.Security.TLSCAFile != "" {
			if _, err := os.Stat(c.Security.TLSCAFile); os.IsNotExist(err) {
				return fmt.Errorf("TLS CA file does not exist: %s", c.Security.TLSCAFile)
			}
		}
	}

	// Validate server configuration
	if c.Server.MaxChannelsPerConnection <= 0 {
		return fmt.Errorf("max channels per connection must be positive: %d", c.Server.MaxChannelsPerConnection)
	}

	if c.Server.MaxFrameSize <= 0 {
		return fmt.Errorf("max frame size must be positive: %d", c.Server.MaxFrameSize)
	}

	if c.Server.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive: %d", c.Server.MaxMessageSize)
	}

	if c.Server.MaxQueues < 0 {
		return fmt.Errorf("max queues must be >= 0 (0 = unlimited): %d", c.Server.MaxQueues)
	}

	if c.Server.MaxExchanges < 0 {
		return fmt.Errorf("max exchanges must be >= 0 (0 = unlimited): %d", c.Server.MaxExchanges)
	}

	// Validate engine configuration
	if c.Engine.RingBufferSize <= 0 {
		return fmt.Errorf("ring buffer size must be positive: %d", c.Engine.RingBufferSize)
	}

	// Hot cache validation (segstore v1)
	//
	// THE COMPARISON IS BETWEEN THE EFFECTIVE BUDGETS.
	//
	// The storage layer SUBSTITUTES a default when the engine configuration
	// leaves a budget unset (storage.DefaultHotCacheQueueBytes /
	// storage.DefaultHotCacheGlobalBytes), so the RAW engine value is 0 in
	// exactly the case where a default is what the broker will actually run
	// with. Comparing the RAW values therefore let a configuration through
	// whose GLOBAL budget is smaller than the PER-QUEUE budget the storage
	// layer will apply -- e.g. `hot_cache_queue_bytes: 0` (unset, so 64 MiB)
	// with `hot_cache_global_bytes: 33554432` (32 MiB). That configuration
	// produces newHotCache(64 MiB, 32 MiB): the per-queue budget can never
	// bind, the GLOBAL eviction is always the one that fires, and the
	// queue-local eviction -- the fix for the multi-queue starvation -- is
	// dead in that configuration.
	//
	// So the defaults are resolved FIRST and the EFFECTIVE budgets are the
	// ones compared.
	effectiveQueueBytes := c.Engine.HotCacheQueueBytes
	if effectiveQueueBytes <= 0 {
		effectiveQueueBytes = storage.DefaultHotCacheQueueBytes
	}
	effectiveGlobalBytes := c.Engine.HotCacheGlobalBytes
	if effectiveGlobalBytes <= 0 {
		effectiveGlobalBytes = storage.DefaultHotCacheGlobalBytes
	}
	if c.Engine.HotCacheQueueBytes != 0 {
		qb := c.Engine.HotCacheQueueBytes
		if qb < 1*1024*1024 || qb > 64*1024*1024 {
			return fmt.Errorf("hot cache queue bytes must be between 1 and 64 MiB: %d", qb)
		}
	}
	if c.Engine.HotCacheGlobalBytes != 0 {
		gb := c.Engine.HotCacheGlobalBytes
		if gb > 256*1024*1024 {
			return fmt.Errorf("hot cache global bytes must be between queue bytes and 256 MiB: %d", gb)
		}
	}
	if effectiveGlobalBytes < effectiveQueueBytes {
		return fmt.Errorf("hot cache global bytes must be at least the effective per-queue budget: global %d, per-queue %d",
			effectiveGlobalBytes, effectiveQueueBytes)
	}
	if c.Engine.SpillThresholdPercent <= 0 || c.Engine.SpillThresholdPercent > 100 {
		return fmt.Errorf("spill threshold percent must be between 1-100: %d", c.Engine.SpillThresholdPercent)
	}

	if c.Engine.WALBatchSize <= 0 {
		return fmt.Errorf("WAL batch size must be positive: %d", c.Engine.WALBatchSize)
	}

	if c.Engine.WALBatchTimeoutMS <= 0 {
		return fmt.Errorf("WAL batch timeout must be positive: %d ms", c.Engine.WALBatchTimeoutMS)
	}

	if c.Engine.WALFileSize <= 0 {
		return fmt.Errorf("WAL file size must be positive: %d", c.Engine.WALFileSize)
	}

	if c.Engine.WALChannelBuffer <= 0 {
		return fmt.Errorf("WAL channel buffer must be positive: %d", c.Engine.WALChannelBuffer)
	}

	if c.Engine.SegmentSize <= 0 {
		return fmt.Errorf("segment size must be positive: %d", c.Engine.SegmentSize)
	}

	if c.Engine.SegmentCheckpointIntervalMS <= 0 {
		return fmt.Errorf("segment checkpoint interval must be positive: %d ms", c.Engine.SegmentCheckpointIntervalMS)
	}

	if c.Engine.CompactionThreshold < 0 || c.Engine.CompactionThreshold > 1 {
		return fmt.Errorf("compaction threshold must be between 0.0-1.0: %f", c.Engine.CompactionThreshold)
	}

	if c.Engine.CompactionIntervalMS <= 0 {
		return fmt.Errorf("compaction interval must be positive: %d ms", c.Engine.CompactionIntervalMS)
	}

	if c.Engine.ConsumerSelectTimeoutMS < 0 {
		return fmt.Errorf("consumer select timeout must be non-negative: %d ms", c.Engine.ConsumerSelectTimeoutMS)
	}

	if c.Engine.ConsumerMaxBatchSize <= 0 {
		return fmt.Errorf("consumer max batch size must be positive: %d", c.Engine.ConsumerMaxBatchSize)
	}

	if c.Engine.WALCleanupCheckIntervalMS <= 0 {
		return fmt.Errorf("WAL cleanup check interval must be positive: %d ms", c.Engine.WALCleanupCheckIntervalMS)
	}

	// Telemetry. The port is checked unconditionally because the default is
	// valid; CLI overrides land in cfg before Validate.
	if c.Telemetry.Port < 1 || c.Telemetry.Port > 65535 {
		return fmt.Errorf("invalid telemetry port: %d", c.Telemetry.Port)
	}
	if c.Telemetry.Address != "" && !strings.EqualFold(c.Telemetry.Address, "localhost") {
		if _, err := netip.ParseAddr(c.Telemetry.Address); err != nil {
			return fmt.Errorf("telemetry address must be \"\", \"localhost\", or an IP literal: %q", c.Telemetry.Address)
		}
	}

	// Log level. "" is accepted for zero-value Load callers (mapped to info);
	// "silent" is accepted because benchmarks use it.
	switch c.Server.LogLevel {
	case "", "debug", "info", "warn", "error", "silent":
	default:
		return fmt.Errorf("invalid log level %q: allowed levels are debug, info, warn, error, silent (or empty for info)", c.Server.LogLevel)
	}

	// Access control lists. Host entries must be IPs/CIDRs/localhost: hostname
	// matching is deliberately unsupported (see ParseHostList).
	if _, err := ParseHostList("security.allowedhosts", c.Security.AllowedHosts); err != nil {
		return err
	}
	if _, err := ParseHostList("security.blockedhosts", c.Security.BlockedHosts); err != nil {
		return err
	}
	for _, field := range []struct {
		name    string
		entries []string
	}{
		{"security.allowedusers", c.Security.AllowedUsers},
		{"security.blockedusers", c.Security.BlockedUsers},
	} {
		for _, user := range field.entries {
			if strings.TrimSpace(user) == "" {
				return fmt.Errorf("%s: user entries must be non-empty strings", field.name)
			}
		}
	}
	if c.Security.AuthFailureLimit < 0 {
		return fmt.Errorf("security.authfailurelimit must be non-negative: %d", c.Security.AuthFailureLimit)
	}
	if c.Security.AuthFailureWindowMS < 0 {
		return fmt.Errorf("security.authfailurewindowms must be non-negative: %d", c.Security.AuthFailureWindowMS)
	}

	return nil
}

// Load reads configuration from a file, then applies AMQP_-prefixed environment
// variable overrides, decoding onto the receiver (which existing callers pass
// as a zero value). It still ends with Validate so the old contract holds.
//
// It ALWAYS parses with the YAML parser, whatever the file is named. YAML is a
// superset of JSON, so a .json file happens to work; TOML does not parse.
//
// Keys are normalized before binding: every segment is lower-cased and
// underscores are removed, so both the canonical concatenated spellings
// ("authenticationenabled") and snake_case ("authentication_enabled") bind.
// Unknown keys are ignored here; LoadConfig reports them so main can warn.
// AMQP_<SECTION>_<KEY> env names are transformed the same way (KEY may use
// underscores); names whose first segment is not a known section are ignored.
//
// The amqp-server --config path does not use this method: LoadConfig seeds
// DefaultConfig() first so omitted keys keep their defaults.
func (c *AMQPConfig) Load(source string) error {
	raw := koanf.New(".")
	if source != "" {
		// Always the YAML parser, regardless of the file's extension.
		if err := raw.Load(file.Provider(source), yaml.Parser()); err != nil {
			return fmt.Errorf("failed to load config file: %w", err)
		}
	}

	k, err := normalizedKoanf(raw)
	if err != nil {
		return err
	}
	if err := loadEnv(k, nil); err != nil {
		return err
	}
	if err := decodeInto(k, c, nil); err != nil {
		return err
	}
	// Zero-value Load callers omit telemetry entirely; Validate checks the
	// port unconditionally, so seed the documented default here. An explicit
	// telemetry.port: 0 stays 0 and is rejected.
	if c.Telemetry.Port == 0 && !k.Exists("telemetry.port") {
		c.Telemetry.Port = DefaultConfig().Telemetry.Port
	}
	// Segstore v1: seed hot-cache defaults when absent
	if c.Engine.HotCacheQueueBytes == 0 && !k.Exists("engine.hot_cache_queue_bytes") {
		c.Engine.HotCacheQueueBytes = DefaultConfig().Engine.HotCacheQueueBytes
	}
	if c.Engine.HotCacheGlobalBytes == 0 && !k.Exists("engine.hot_cache_global_bytes") {
		c.Engine.HotCacheGlobalBytes = DefaultConfig().Engine.HotCacheGlobalBytes
	}
	return c.Validate()
}

// ResolvePaths makes every filesystem path in cfg absolute relative to base
// (normally the process's cwd). Daemonizing chdirs to /, so paths must be
// resolved before that happens; applying it in non-daemon mode too keeps log
// lines unambiguous. Empty strings stay empty, absolute paths are unchanged.
func (c *AMQPConfig) ResolvePaths(base string) {
	resolve := func(path string) string {
		if path == "" || filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(base, path)
	}
	c.Storage.Path = resolve(c.Storage.Path)
	c.Server.LogFile = resolve(c.Server.LogFile)
	c.Server.PidFile = resolve(c.Server.PidFile)
	c.Security.AuthenticationFilePath = resolve(c.Security.AuthenticationFilePath)
	c.Security.TLSCertFile = resolve(c.Security.TLSCertFile)
	c.Security.TLSKeyFile = resolve(c.Security.TLSKeyFile)
	c.Security.TLSCAFile = resolve(c.Security.TLSCAFile)
	if c.Security.AuthenticationConfig != nil {
		if userFile, ok := c.Security.AuthenticationConfig["user_file"].(string); ok {
			c.Security.AuthenticationConfig["user_file"] = resolve(userFile)
		}
	}
}

// Save saves configuration to a YAML file
func (c *AMQPConfig) Save(destination string) error {
	// Ensure destination directory exists
	dir := filepath.Dir(destination)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("failed to create configuration directory: %w", err)
	}

	data, err := yamlv3.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal configuration: %w", err)
	}

	// 0600: configs can name credential paths and may later hold secrets.
	if err := os.WriteFile(destination, data, 0600); err != nil {
		return fmt.Errorf("failed to write configuration file: %w", err)
	}

	return nil
}
