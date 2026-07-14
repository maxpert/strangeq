package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/maxpert/amqp-go/interfaces"
	yamlv3 "gopkg.in/yaml.v3"
)

// boolPtr returns a pointer to b. Used for tri-state config flags (see
// StorageConfig.Fsync) where a non-nil pointer expresses an explicit choice.
func boolPtr(b bool) *bool { return &b }

// DefaultConfig creates a configuration with sensible defaults
func DefaultConfig() *AMQPConfig {
	return &AMQPConfig{
		Network: interfaces.NetworkConfig{
			Address:                ":5672",
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
		},
		Storage: interfaces.StorageConfig{
			Path: "./data",
			// Durable by default: an explicit non-nil true so --generate-config
			// emits `fsync: true` (not `fsync: null`) and the no-config path is
			// unambiguously durable. Set fsync:false to opt out of the barrier.
			Fsync:                boolPtr(true),
			CRCCheck:             boolPtr(true),
			CacheMB:              64,       // 64 MB metadata cache
			MaxFiles:             100,      // Max open file handles
			RetentionMS:          86400000, // 24 hours
			CheckpointIntervalMS: 5000,     // 5 seconds
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
		Server: interfaces.ServerConfig{
			Name:                     "amqp-go-server",
			Version:                  "0.9.1",
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
			CleanupIntervalMS:        300000,   // 5 minutes
		},
		Engine: interfaces.EngineConfig{
			// Ring Buffer (Hot Path)
			RingBufferSize:        65536, // 64K messages = ~6.5 MB per queue
			SpillThresholdPercent: 80,    // Start spilling at 80% = 51,200 messages

			// Write-Ahead Log (WAL)
			WALBatchSize:      1000,              // 1,000 messages per batch
			WALBatchTimeoutMS: 5,                 // retained for compatibility; flushing is drain-then-flush, not timer-gated
			WALFileSize:       512 * 1024 * 1024, // 512 MB per WAL file
			WALChannelBuffer:  10000,             // 10K buffered requests

			// Segment Storage (Cold Path)
			SegmentSize:                 256 * 1024 * 1024, // 256 MB per segment (4x faster compaction vs 1GB)
			SegmentCheckpointIntervalMS: 300000,            // 5 minutes
			CompactionThreshold:         0.5,               // Compact at 50% deleted
			CompactionIntervalMS:        1800000,           // 30 minutes

			// Consumer Delivery
			ConsumerSelectTimeoutMS: 1,    // 1 millisecond (500µs rounded up)
			ConsumerMaxBatchSize:    100,  // Max 100 messages per consumer per poll
			UnlimitedPrefetchCap:    2000, // Finite gate cap for prefetch-0 manual-ack consumers

			// Background Maintenance
			WALCleanupCheckIntervalMS: 300000, // 5 minutes
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

// FsyncEnabled reports whether the WAL group-commit fsync durability barrier is
// active. The tri-state Storage.Fsync defaults to ON: nil (omitted) or an
// explicit true both enable it; only an explicit false disables it.
func (c *AMQPConfig) FsyncEnabled() bool {
	return c.Storage.Fsync == nil || *c.Storage.Fsync
}

// CRCEnabled reports whether WAL/segment record CRC32 integrity checks are
// active. The tri-state Storage.CRCCheck defaults to ON: nil (omitted) or an
// explicit true both enable it; only an explicit false disables it.
func (c *AMQPConfig) CRCEnabled() bool {
	return c.Storage.CRCCheck == nil || *c.Storage.CRCCheck
}

// GetEngine returns engine tuning configuration. It overlays the single user
// fsync knob onto the internal (inverted) WALSyncDisabled transport and the
// CRC knob onto WALCRCDisabled so the storage layer honors both while every
// zero-value path stays durable and integrity-checked.
func (c *AMQPConfig) GetEngine() interfaces.EngineConfig {
	e := c.Engine
	e.WALSyncDisabled = !c.FsyncEnabled()
	e.WALCRCDisabled = !c.CRCEnabled()
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

	// Validate storage configuration
	if c.Storage.Path == "" {
		return fmt.Errorf("storage path cannot be empty")
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

	// Validate engine configuration
	if c.Engine.RingBufferSize <= 0 || (c.Engine.RingBufferSize&(c.Engine.RingBufferSize-1)) != 0 {
		return fmt.Errorf("ring buffer size must be a power of 2: %d", c.Engine.RingBufferSize)
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

	return nil
}

// Load loads configuration from a JSON file
// Load loads configuration from file (YAML/JSON/TOML) with environment variable overrides
// Environment variables use AMQP_ prefix and underscore separators (e.g., AMQP_NETWORK_PORT=5673)
func (c *AMQPConfig) Load(source string) error {
	k := koanf.New(".")

	// Load from file (supports YAML, JSON, TOML based on extension)
	if err := k.Load(file.Provider(source), yaml.Parser()); err != nil {
		return fmt.Errorf("failed to load config file: %w", err)
	}

	// Load environment variables with AMQP_ prefix
	// Converts AMQP_NETWORK_PORT -> network.port
	if err := k.Load(env.Provider(".", env.Opt{
		Prefix: "AMQP_",
		TransformFunc: func(k, v string) (string, interface{}) {
			key := strings.Replace(strings.ToLower(
				strings.TrimPrefix(k, "AMQP_")), "_", ".", -1)
			return key, v
		},
	}), nil); err != nil {
		return fmt.Errorf("failed to load environment variables: %w", err)
	}

	// Unmarshal into config struct
	if err := k.Unmarshal("", c); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return c.Validate()
}

// Save saves configuration to a YAML file
func (c *AMQPConfig) Save(destination string) error {
	// Ensure destination directory exists
	dir := filepath.Dir(destination)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create configuration directory: %w", err)
	}

	data, err := yamlv3.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal configuration: %w", err)
	}

	if err := os.WriteFile(destination, data, 0644); err != nil {
		return fmt.Errorf("failed to write configuration file: %w", err)
	}

	return nil
}
