package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// Test default values
	assert.Equal(t, ":5672", config.Network.Address)
	assert.Equal(t, 5672, config.Network.Port)
	assert.Equal(t, 1000, config.Network.MaxConnections)
	assert.Equal(t, "badger", config.Storage.Backend)
	assert.Equal(t, "./data", config.Storage.Path)
	assert.Equal(t, false, config.Security.TLSEnabled)
	assert.Equal(t, "amqp-go-server", config.Server.Name)

	// Test validation passes
	err := config.Validate()
	assert.NoError(t, err)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*AMQPConfig)
		wantErr bool
	}{
		{
			name: "valid config",
			modify: func(c *AMQPConfig) {
				// Default config should be valid
			},
			wantErr: false,
		},
		{
			name: "invalid port - zero",
			modify: func(c *AMQPConfig) {
				c.Network.Port = 0
			},
			wantErr: true,
		},
		{
			name: "invalid port - too high",
			modify: func(c *AMQPConfig) {
				c.Network.Port = 70000
			},
			wantErr: true,
		},
		{
			name: "invalid max connections",
			modify: func(c *AMQPConfig) {
				c.Network.MaxConnections = -1
			},
			wantErr: true,
		},
		{
			name: "empty storage backend",
			modify: func(c *AMQPConfig) {
				c.Storage.Backend = ""
			},
			wantErr: true,
		},
		{
			name: "TLS enabled without cert file",
			modify: func(c *AMQPConfig) {
				c.Security.TLSEnabled = true
				c.Security.TLSCertFile = ""
			},
			wantErr: true,
		},
		{
			name: "invalid max channels",
			modify: func(c *AMQPConfig) {
				c.Server.MaxChannelsPerConnection = 0
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			tt.modify(config)

			err := config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigSaveLoad(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test-config.json")

	// Create a config with custom values
	originalConfig := DefaultConfig()
	originalConfig.Network.Address = ":8080"
	originalConfig.Network.MaxConnections = 500
	originalConfig.Storage.Backend = "bbolt"
	originalConfig.Storage.Path = "/tmp/amqp.db"
	originalConfig.Server.LogLevel = "debug"

	// Save the config
	err := originalConfig.Save(configFile)
	require.NoError(t, err)

	// Verify file was created
	assert.FileExists(t, configFile)

	// Load the config
	loadedConfig := &AMQPConfig{}
	err = loadedConfig.Load(configFile)
	require.NoError(t, err)

	// Verify values were preserved
	assert.Equal(t, ":8080", loadedConfig.Network.Address)
	assert.Equal(t, 500, loadedConfig.Network.MaxConnections)
	assert.Equal(t, "bbolt", loadedConfig.Storage.Backend)
	assert.Equal(t, "/tmp/amqp.db", loadedConfig.Storage.Path)
	assert.Equal(t, "debug", loadedConfig.Server.LogLevel)
}

func TestConfigLoadNonexistent(t *testing.T) {
	config := &AMQPConfig{}
	err := config.Load("/nonexistent/path.json")
	assert.Error(t, err)
}

func TestConfigLoadInvalidJSON(t *testing.T) {
	// Create a temporary file with invalid JSON
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid.json")

	err := os.WriteFile(configFile, []byte("invalid json content"), 0644)
	require.NoError(t, err)

	config := &AMQPConfig{}
	err = config.Load(configFile)
	assert.Error(t, err)
}

func TestConfigLoadYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test.yaml")

	yamlContent := `
network:
  address: ":8080"
  port: 8080
  maxconnections: 1000
  connectiontimeout: 30s
storage:
  backend: badger
  path: ./test-data
server:
  loglevel: debug
  maxchannelsperconnection: 2047
  maxframesize: 131072
  maxmessagesize: 16777216
engine:
  availablechannelbuffer: 10000000
  ringbuffersize: 65536
  spillthresholdpercent: 80
  walbatchsize: 1000
  walbatchtimeout: 10ms
  walfilesize: 536870912
  walchannelbuffer: 10000
  segmentsize: 1073741824
  segmentcheckpointinterval: 5m
  compactionthreshold: 0.5
  compactioninterval: 30m
  consumerselecttimeout: 500µs
  consumermaxbatchsize: 100
  expiredmessagecheckinterval: 1m
  walcleanupcheckinterval: 5m
  offsetcleanupbatchsize: 1000
  offsetcleanupinterval: 30s
`
	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	config := &AMQPConfig{}
	err = config.Load(configFile)
	assert.NoError(t, err)
	assert.Equal(t, ":8080", config.Network.Address)
	assert.Equal(t, "debug", config.Server.LogLevel)
}

func TestConfigBuilder(t *testing.T) {
	config, err := NewConfigBuilder().
		WithAddress(":9090").
		WithPort(9090).
		WithMaxConnections(2000).
		WithConnectionTimeout(45*time.Second).
		WithMemoryStorage().
		WithLogging("debug", "/var/log/amqp.log").
		WithServerInfo("test-server", "1.0.0", "Test AMQP", "Test", "Test Corp").
		Build()

	require.NoError(t, err)

	assert.Equal(t, ":9090", config.Network.Address)
	assert.Equal(t, 9090, config.Network.Port)
	assert.Equal(t, 2000, config.Network.MaxConnections)
	assert.Equal(t, 45*time.Second, config.Network.ConnectionTimeout)
	assert.Equal(t, "memory", config.Storage.Backend)
	assert.Equal(t, "debug", config.Server.LogLevel)
	assert.Equal(t, "/var/log/amqp.log", config.Server.LogFile)
	assert.Equal(t, "test-server", config.Server.Name)
}

func TestConfigBuilderFromExisting(t *testing.T) {
	originalConfig := DefaultConfig()
	originalConfig.Network.Address = ":8080"

	newConfig, err := FromConfig(originalConfig).
		WithPort(9090).
		Build()

	require.NoError(t, err)

	// Should preserve original address
	assert.Equal(t, ":8080", newConfig.Network.Address)
	// Should have new port
	assert.Equal(t, 9090, newConfig.Network.Port)
}

func TestConfigBuilderTLS(t *testing.T) {
	// Create temporary cert files for testing
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	err := os.WriteFile(certFile, []byte("fake cert"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, []byte("fake key"), 0644)
	require.NoError(t, err)

	config, err := NewConfigBuilder().
		WithTLS(certFile, keyFile).
		Build()

	require.NoError(t, err)

	assert.True(t, config.Security.TLSEnabled)
	assert.Equal(t, certFile, config.Security.TLSCertFile)
	assert.Equal(t, keyFile, config.Security.TLSKeyFile)
}

func TestConfigBuilderValidationError(t *testing.T) {
	_, err := NewConfigBuilder().
		WithPort(0). // Invalid port
		Build()

	assert.Error(t, err)
}

func TestConfigBuilderBuildUnsafe(t *testing.T) {
	config := NewConfigBuilder().
		WithPort(0). // Invalid port
		BuildUnsafe()

	// Should return config without validation
	assert.Equal(t, 0, config.Network.Port)

	// But validation should fail
	err := config.Validate()
	assert.Error(t, err)
}
