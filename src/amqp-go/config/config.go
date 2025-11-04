package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/maxpert/amqp-go/interfaces"
	yamlv3 "gopkg.in/yaml.v3"
)

// DefaultConfig creates a configuration with sensible defaults
func DefaultConfig() *AMQPConfig {
	return &AMQPConfig{
		Network: interfaces.NetworkConfig{
			Address:              ":5672",
			Port:                 5672,
			MaxConnections:       1000,
			ConnectionTimeout:    30 * time.Second,
			HeartbeatInterval:    60 * time.Second,
			TCPKeepAlive:         true,
			TCPKeepAliveInterval: 30 * time.Second,
			ReadBufferSize:       8192,
			WriteBufferSize:      8192,
		},
		Storage: interfaces.StorageConfig{
			Backend:                  "badger",
			Path:                     "./data",
			Options:                  make(map[string]interface{}),
			Persistent:               true,
			SyncWrites:               false,
			CacheSize:                64 * 1024 * 1024, // 64MB
			MaxOpenFiles:             100,
			CompactionAge:            24 * time.Hour,
			OffsetCheckpointInterval: 5 * time.Second, // Phase 4: Default 5s checkpoint interval
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
			ChannelTimeout:           60 * time.Second,
			MessageTimeout:           30 * time.Second,
			CleanupInterval:          5 * time.Minute,
			MemoryLimitPercent:       60, // 60% of RAM (RabbitMQ default)
			MemoryLimitBytes:         0,  // 0 = use percentage
		},
		Engine: interfaces.EngineConfig{
			// Queue State Management
			AvailableChannelBuffer: 10000000, // 10M = 80 MB per queue (Phase 6E)

			// Ring Buffer (Hot Path)
			RingBufferSize:        65536, // 64K messages = ~6.5 MB per queue
			SpillThresholdPercent: 80,    // Start spilling at 80% = 51,200 messages

			// Write-Ahead Log (WAL)
			WALBatchSize:     1000,                  // 1,000 messages per batch
			WALBatchTimeout:  10 * time.Millisecond, // 10ms max wait
			WALFileSize:      512 * 1024 * 1024,     // 512 MB per WAL file
			WALChannelBuffer: 10000,                 // 10K buffered requests

			// Segment Storage (Cold Path)
			SegmentSize:               1024 * 1024 * 1024, // 1 GB per segment
			SegmentCheckpointInterval: 5 * time.Minute,    // Checkpoint every 5 minutes
			CompactionThreshold:       0.5,                // Compact at 50% deleted
			CompactionInterval:        30 * time.Minute,   // Check every 30 minutes

			// Consumer Delivery
			ConsumerSelectTimeout: 500 * time.Microsecond, // 500µs wait when no messages
			ConsumerMaxBatchSize:  100,                    // Max 100 messages per consumer per poll

			// Background Maintenance
			ExpiredMessageCheckInterval: 60 * time.Second, // Check for expired messages every 60s
			WALCleanupCheckInterval:     5 * time.Minute,  // Check for old WAL files every 5 minutes
			OffsetCleanupBatchSize:      1000,             // Delete 1,000 offsets per cycle
			OffsetCleanupInterval:       30 * time.Second, // Cleanup ACKed offsets every 30s
		},
	}
}

// AMQPConfig implements the Config interface
type AMQPConfig struct {
	Network  interfaces.NetworkConfig  `json:"network"`
	Storage  interfaces.StorageConfig  `json:"storage"`
	Security interfaces.SecurityConfig `json:"security"`
	Server   interfaces.ServerConfig   `json:"server"`
	Engine   interfaces.EngineConfig   `json:"engine"`
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

// GetEngine returns engine tuning configuration
func (c *AMQPConfig) GetEngine() interfaces.EngineConfig {
	return c.Engine
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

	if c.Network.ConnectionTimeout <= 0 {
		return fmt.Errorf("connection timeout must be positive: %v", c.Network.ConnectionTimeout)
	}

	// Validate storage configuration
	if c.Storage.Backend == "" {
		return fmt.Errorf("storage backend cannot be empty")
	}

	if c.Storage.Backend == "badger" && c.Storage.Path == "" {
		return fmt.Errorf("storage path required for badger backend")
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
	if c.Engine.AvailableChannelBuffer <= 0 {
		return fmt.Errorf("available channel buffer must be positive: %d", c.Engine.AvailableChannelBuffer)
	}

	if c.Engine.RingBufferSize <= 0 || (c.Engine.RingBufferSize&(c.Engine.RingBufferSize-1)) != 0 {
		return fmt.Errorf("ring buffer size must be a power of 2: %d", c.Engine.RingBufferSize)
	}

	if c.Engine.SpillThresholdPercent <= 0 || c.Engine.SpillThresholdPercent > 100 {
		return fmt.Errorf("spill threshold percent must be between 1-100: %d", c.Engine.SpillThresholdPercent)
	}

	if c.Engine.WALBatchSize <= 0 {
		return fmt.Errorf("WAL batch size must be positive: %d", c.Engine.WALBatchSize)
	}

	if c.Engine.WALBatchTimeout <= 0 {
		return fmt.Errorf("WAL batch timeout must be positive: %v", c.Engine.WALBatchTimeout)
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

	if c.Engine.SegmentCheckpointInterval <= 0 {
		return fmt.Errorf("segment checkpoint interval must be positive: %v", c.Engine.SegmentCheckpointInterval)
	}

	if c.Engine.CompactionThreshold < 0 || c.Engine.CompactionThreshold > 1 {
		return fmt.Errorf("compaction threshold must be between 0.0-1.0: %f", c.Engine.CompactionThreshold)
	}

	if c.Engine.CompactionInterval <= 0 {
		return fmt.Errorf("compaction interval must be positive: %v", c.Engine.CompactionInterval)
	}

	if c.Engine.ConsumerSelectTimeout <= 0 {
		return fmt.Errorf("consumer select timeout must be positive: %v", c.Engine.ConsumerSelectTimeout)
	}

	if c.Engine.ConsumerMaxBatchSize <= 0 {
		return fmt.Errorf("consumer max batch size must be positive: %d", c.Engine.ConsumerMaxBatchSize)
	}

	if c.Engine.ExpiredMessageCheckInterval <= 0 {
		return fmt.Errorf("expired message check interval must be positive: %v", c.Engine.ExpiredMessageCheckInterval)
	}

	if c.Engine.WALCleanupCheckInterval <= 0 {
		return fmt.Errorf("WAL cleanup check interval must be positive: %v", c.Engine.WALCleanupCheckInterval)
	}

	if c.Engine.OffsetCleanupBatchSize <= 0 {
		return fmt.Errorf("offset cleanup batch size must be positive: %d", c.Engine.OffsetCleanupBatchSize)
	}

	if c.Engine.OffsetCleanupInterval <= 0 {
		return fmt.Errorf("offset cleanup interval must be positive: %v", c.Engine.OffsetCleanupInterval)
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
