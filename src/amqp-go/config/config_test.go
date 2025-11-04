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
			name: "empty storage path",
			modify: func(c *AMQPConfig) {
				c.Storage.Path = ""
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
	originalConfig.Storage.Path = "/tmp/amqp-data"
	originalConfig.Storage.Fsync = true
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
	assert.Equal(t, "/tmp/amqp-data", loadedConfig.Storage.Path)
	assert.Equal(t, true, loadedConfig.Storage.Fsync)
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
  connectiontimeoutms: 30000
  heartbeatintervalms: 60000
  tcpkeepalive: true
  tcpkeepaliveintervalms: 30000
storage:
  path: ./test-data
  fsync: false
  cachemb: 64
  maxfiles: 100
  retentionms: 86400000
  checkpointintervalms: 5000
server:
  loglevel: debug
  maxchannelsperconnection: 2047
  maxframesize: 131072
  maxmessagesize: 16777216
  channeltimeoutms: 60000
  messagetimeoutms: 30000
  cleanupintervalms: 300000
engine:
  availablechannelbuffer: 10000000
  ringbuffersize: 65536
  spillthresholdpercent: 80
  walbatchsize: 1000
  walbatchtimeoutms: 10
  walfilesize: 536870912
  walchannelbuffer: 10000
  segmentsize: 1073741824
  segmentcheckpointintervalms: 300000
  compactionthreshold: 0.5
  compactionintervalms: 1800000
  consumerselecttimeoutms: 1
  consumermaxbatchsize: 100
  expiredmessagecheckintervalms: 60000
  walcleanupcheckintervalms: 300000
  offsetcleanupbatchsize: 1000
  offsetcleanupintervalms: 30000
`
	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	config := &AMQPConfig{}
	err = config.Load(configFile)
	assert.NoError(t, err)
	assert.Equal(t, ":8080", config.Network.Address)
	assert.Equal(t, "debug", config.Server.LogLevel)
	assert.Equal(t, int64(30000), config.Network.ConnectionTimeoutMS)
	assert.Equal(t, false, config.Storage.Fsync)
}

func TestConfigBuilder(t *testing.T) {
	config, err := NewConfigBuilder().
		WithAddress(":9090").
		WithPort(9090).
		WithMaxConnections(2000).
		WithConnectionTimeout(45*time.Second).
		WithStoragePath("/tmp/test-storage").
		WithFsync(true).
		WithLogging("debug", "/var/log/amqp.log").
		WithServerInfo("test-server", "1.0.0", "Test AMQP", "Test", "Test Corp").
		Build()

	require.NoError(t, err)

	assert.Equal(t, ":9090", config.Network.Address)
	assert.Equal(t, 9090, config.Network.Port)
	assert.Equal(t, 2000, config.Network.MaxConnections)
	assert.Equal(t, int64(45000), config.Network.ConnectionTimeoutMS)
	assert.Equal(t, "/tmp/test-storage", config.Storage.Path)
	assert.Equal(t, true, config.Storage.Fsync)
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
