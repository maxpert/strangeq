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

// WithConnectionTimeout sets the connection timeout
func (b *ConfigBuilder) WithConnectionTimeout(timeout time.Duration) *ConfigBuilder {
	b.config.Network.ConnectionTimeout = timeout
	return b
}

// WithHeartbeat sets the heartbeat interval
func (b *ConfigBuilder) WithHeartbeat(interval time.Duration) *ConfigBuilder {
	b.config.Network.HeartbeatInterval = interval
	return b
}

// WithTCPKeepAlive enables/disables TCP keep-alive
func (b *ConfigBuilder) WithTCPKeepAlive(enabled bool, interval time.Duration) *ConfigBuilder {
	b.config.Network.TCPKeepAlive = enabled
	b.config.Network.TCPKeepAliveInterval = interval
	return b
}

// WithBufferSizes sets read and write buffer sizes
func (b *ConfigBuilder) WithBufferSizes(readSize, writeSize int) *ConfigBuilder {
	b.config.Network.ReadBufferSize = readSize
	b.config.Network.WriteBufferSize = writeSize
	return b
}

// Storage Configuration

// WithMemoryStorage configures in-memory storage
func (b *ConfigBuilder) WithMemoryStorage() *ConfigBuilder {
	b.config.Storage.Backend = "memory"
	b.config.Storage.Path = ""
	b.config.Storage.Persistent = false
	return b
}

// WithBBoltStorage configures BBolt storage
func (b *ConfigBuilder) WithBBoltStorage(path string) *ConfigBuilder {
	b.config.Storage.Backend = "bbolt"
	b.config.Storage.Path = path
	b.config.Storage.Persistent = true
	return b
}

// WithBadgerStorage configures Badger storage
func (b *ConfigBuilder) WithBadgerStorage(path string) *ConfigBuilder {
	b.config.Storage.Backend = "badger"
	b.config.Storage.Path = path
	b.config.Storage.Persistent = true
	return b
}

// WithStorageOptions sets storage-specific options
func (b *ConfigBuilder) WithStorageOptions(options map[string]interface{}) *ConfigBuilder {
	if b.config.Storage.Options == nil {
		b.config.Storage.Options = make(map[string]interface{})
	}
	for k, v := range options {
		b.config.Storage.Options[k] = v
	}
	return b
}

// WithSyncWrites enables/disables synchronous writes
func (b *ConfigBuilder) WithSyncWrites(enabled bool) *ConfigBuilder {
	b.config.Storage.SyncWrites = enabled
	return b
}

// WithCacheSize sets the storage cache size
func (b *ConfigBuilder) WithCacheSize(size int64) *ConfigBuilder {
	b.config.Storage.CacheSize = size
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

// WithTLSCA sets the TLS CA file
func (b *ConfigBuilder) WithTLSCA(caFile string) *ConfigBuilder {
	b.config.Security.TLSCAFile = caFile
	return b
}

// WithAuthentication enables authentication with the specified backend
func (b *ConfigBuilder) WithAuthentication(backend string, options map[string]interface{}) *ConfigBuilder {
	b.config.Security.AuthenticationEnabled = true
	b.config.Security.AuthenticationBackend = backend
	b.config.Security.AuthenticationConfig = options
	return b
}

// WithFileAuthentication enables file-based authentication
func (b *ConfigBuilder) WithFileAuthentication(userFile string) *ConfigBuilder {
	return b.WithAuthentication("file", map[string]interface{}{
		"user_file": userFile,
	})
}

// WithAuthorization enables authorization
func (b *ConfigBuilder) WithAuthorization(enabled bool) *ConfigBuilder {
	b.config.Security.AuthorizationEnabled = enabled
	return b
}

// WithVirtualHost sets the default virtual host
func (b *ConfigBuilder) WithVirtualHost(vhost string) *ConfigBuilder {
	b.config.Security.DefaultVHost = vhost
	return b
}

// WithAccessControl sets allowed and blocked users/hosts
func (b *ConfigBuilder) WithAccessControl(allowedUsers, blockedUsers, allowedHosts, blockedHosts []string) *ConfigBuilder {
	b.config.Security.AllowedUsers = allowedUsers
	b.config.Security.BlockedUsers = blockedUsers
	b.config.Security.AllowedHosts = allowedHosts
	b.config.Security.BlockedHosts = blockedHosts
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

// WithDaemonize enables/disables daemon mode
func (b *ConfigBuilder) WithDaemonize(enabled bool, pidFile string) *ConfigBuilder {
	b.config.Server.Daemonize = enabled
	b.config.Server.PidFile = pidFile
	return b
}

// WithProtocolLimits sets AMQP protocol limits
func (b *ConfigBuilder) WithProtocolLimits(maxChannels int, maxFrameSize int, maxMessageSize int64) *ConfigBuilder {
	b.config.Server.MaxChannelsPerConnection = maxChannels
	b.config.Server.MaxFrameSize = maxFrameSize
	b.config.Server.MaxMessageSize = maxMessageSize
	return b
}

// WithTimeouts sets various timeout settings
func (b *ConfigBuilder) WithTimeouts(channelTimeout, messageTimeout, cleanupInterval time.Duration) *ConfigBuilder {
	b.config.Server.ChannelTimeout = channelTimeout
	b.config.Server.MessageTimeout = messageTimeout
	b.config.Server.CleanupInterval = cleanupInterval
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