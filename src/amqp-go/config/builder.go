package config

import (
	"time"
)

// ConfigBuilder provides a fluent API for building configuration
type ConfigBuilder struct {
	config *AMQPConfig
}

// NewConfigBuilder creates a new configuration builder with defaults
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: DefaultConfig(),
	}
}

// FromConfig creates a builder from an existing configuration
func FromConfig(config *AMQPConfig) *ConfigBuilder {
	// Deep copy the configuration
	builder := NewConfigBuilder()
	*builder.config = *config
	return builder
}

// Network Configuration

// WithAddress sets the server address
func (b *ConfigBuilder) WithAddress(address string) *ConfigBuilder {
	b.config.Network.Address = address
	return b
}

// WithPort sets the server port
func (b *ConfigBuilder) WithPort(port int) *ConfigBuilder {
	b.config.Network.Port = port
	return b
}

// WithMaxConnections sets the maximum number of connections
func (b *ConfigBuilder) WithMaxConnections(max int) *ConfigBuilder {
	b.config.Network.MaxConnections = max
	return b
}

// Storage Configuration

// WithStoragePath sets the data directory path
func (b *ConfigBuilder) WithStoragePath(path string) *ConfigBuilder {
	b.config.Storage.Path = path
	return b
}

// WithDurability sets the single durability option: interfaces.DurabilityNone,
// interfaces.DurabilityBarrier (the default), or interfaces.DurabilityFlush.
// Any other value is rejected by Validate/Build.
func (b *ConfigBuilder) WithDurability(durability string) *ConfigBuilder {
	b.config.Storage.Durability = durability
	return b
}

// WithCacheSize sets the storage cache size in megabytes
func (b *ConfigBuilder) WithCacheSize(sizeMB int64) *ConfigBuilder {
	b.config.Storage.CacheMB = sizeMB
	return b
}

// WithMaxFiles sets the maximum number of open file handles
func (b *ConfigBuilder) WithMaxFiles(maxFiles int) *ConfigBuilder {
	b.config.Storage.MaxFiles = maxFiles
	return b
}

// WithRetention sets the segment retention period
func (b *ConfigBuilder) WithRetention(retention time.Duration) *ConfigBuilder {
	b.config.Storage.RetentionMS = retention.Milliseconds()
	return b
}

// Security Configuration

// WithTLS enables TLS with the specified certificate and key files
func (b *ConfigBuilder) WithTLS(certFile, keyFile string) *ConfigBuilder {
	b.config.Security.TLSEnabled = true
	b.config.Security.TLSCertFile = certFile
	b.config.Security.TLSKeyFile = keyFile
	return b
}

// Server Configuration

// WithServerInfo sets server identification information
func (b *ConfigBuilder) WithServerInfo(name, version, product, platform, copyright string) *ConfigBuilder {
	b.config.Server.Name = name
	b.config.Server.Version = version
	b.config.Server.Product = product
	b.config.Server.Platform = platform
	b.config.Server.Copyright = copyright
	return b
}

// WithLogging configures logging settings
func (b *ConfigBuilder) WithLogging(level, logFile string) *ConfigBuilder {
	b.config.Server.LogLevel = level
	b.config.Server.LogFile = logFile
	return b
}

// Build returns the configured AMQPConfig
func (b *ConfigBuilder) Build() (*AMQPConfig, error) {
	if err := b.config.Validate(); err != nil {
		return nil, err
	}
	return b.config, nil
}

// BuildUnsafe returns the configured AMQPConfig without validation
func (b *ConfigBuilder) BuildUnsafe() *AMQPConfig {
	return b.config
}
