package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
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
			Backend:       "memory",
			Path:          "",
			Options:       make(map[string]interface{}),
			Persistent:    false,
			SyncWrites:    false,
			CacheSize:     64 * 1024 * 1024, // 64MB
			MaxOpenFiles:  100,
			CompactionAge: 24 * time.Hour,
		},
		Security: interfaces.SecurityConfig{
			TLSEnabled:            false,
			TLSCertFile:           "",
			TLSKeyFile:            "",
			TLSCAFile:             "",
			AuthenticationEnabled: false,
			AuthenticationBackend: "file",
			AuthenticationConfig:  make(map[string]interface{}),
			AuthorizationEnabled:  false,
			DefaultVHost:          "/",
			AllowedUsers:          []string{},
			BlockedUsers:          []string{},
			AllowedHosts:          []string{},
			BlockedHosts:          []string{},
		},
		Server: interfaces.ServerConfig{
			Name:                      "amqp-go-server",
			Version:                   "0.9.1",
			Product:                   "AMQP-Go",
			Platform:                  "Go",
			Copyright:                 "Maxpert AMQP-Go Server",
			LogLevel:                  "info",
			LogFile:                   "",
			PidFile:                   "",
			Daemonize:                 false,
			MaxChannelsPerConnection:  2047,
			MaxFrameSize:              131072, // 128KB
			MaxMessageSize:            16777216, // 16MB
			ChannelTimeout:            60 * time.Second,
			MessageTimeout:            30 * time.Second,
			CleanupInterval:           5 * time.Minute,
		},
	}
}

// AMQPConfig implements the Config interface
type AMQPConfig struct {
	Network  interfaces.NetworkConfig  `json:"network"`
	Storage  interfaces.StorageConfig  `json:"storage"`
	Security interfaces.SecurityConfig `json:"security"`
	Server   interfaces.ServerConfig   `json:"server"`
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
	
	if c.Storage.Backend != "memory" && c.Storage.Path == "" {
		return fmt.Errorf("storage path required for backend: %s", c.Storage.Backend)
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

	return nil
}

// Load loads configuration from a JSON file
func (c *AMQPConfig) Load(source string) error {
	// Determine file format based on extension
	ext := filepath.Ext(source)
	if ext != ".json" {
		return fmt.Errorf("unsupported configuration format: %s (only JSON supported)", ext)
	}

	data, err := os.ReadFile(source)
	if err != nil {
		return fmt.Errorf("failed to read configuration file: %w", err)
	}

	if err := json.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse configuration file: %w", err)
	}

	return c.Validate()
}

// Save saves configuration to a JSON file
func (c *AMQPConfig) Save(destination string) error {
	// Ensure destination directory exists
	dir := filepath.Dir(destination)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create configuration directory: %w", err)
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal configuration: %w", err)
	}

	if err := os.WriteFile(destination, data, 0644); err != nil {
		return fmt.Errorf("failed to write configuration file: %w", err)
	}

	return nil
}