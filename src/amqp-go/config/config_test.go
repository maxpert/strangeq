package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
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

	// WALBatchTimeoutMS was 10ms → now 5ms (halves tail latency for lone durable messages)
	assert.Equal(t, int64(5), config.Engine.WALBatchTimeoutMS,
		"WALBatchTimeoutMS should be 5ms, was 10ms")

	// SegmentSize was 1GB → now 256MB (4x faster compaction)
	assert.Equal(t, int64(256*1024*1024), config.Engine.SegmentSize,
		"SegmentSize should be 256MB, was 1GB")

	// HeartbeatIntervalMS is wired into sendConnectionTune (server package)
	assert.Equal(t, int64(60000), config.Network.HeartbeatIntervalMS,
		"HeartbeatIntervalMS should be 60s by default")

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
			name: "TLS enabled without key file",
			modify: func(c *AMQPConfig) {
				c.Security.TLSEnabled = true
				c.Security.TLSCertFile = "/dev/null"
				c.Security.TLSKeyFile = ""
			},
			wantErr: true,
		},
		{
			name: "TLS enabled with non-existent cert",
			modify: func(c *AMQPConfig) {
				c.Security.TLSEnabled = true
				c.Security.TLSCertFile = "/nonexistent/cert.pem"
				c.Security.TLSKeyFile = "/dev/null"
			},
			wantErr: true,
		},
		{
			name: "TLS enabled with non-existent key",
			modify: func(c *AMQPConfig) {
				c.Security.TLSEnabled = true
				c.Security.TLSCertFile = "/dev/null"
				c.Security.TLSKeyFile = "/nonexistent/key.pem"
			},
			wantErr: true,
		},
		{
			name: "TLS enabled with non-existent CA file",
			modify: func(c *AMQPConfig) {
				c.Security.TLSEnabled = true
				c.Security.TLSCertFile = "/dev/null"
				c.Security.TLSKeyFile = "/dev/null"
				c.Security.TLSCAFile = "/nonexistent/ca.pem"
			},
			wantErr: true,
		},
		{
			name: "TLS disabled with non-existent CA file (CA not checked when TLS off)",
			modify: func(c *AMQPConfig) {
				c.Security.TLSEnabled = false
				c.Security.TLSCAFile = "/nonexistent/ca.pem"
			},
			wantErr: false,
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
	originalConfig.Storage.Fsync = boolPtr(true)
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
	require.NotNil(t, loadedConfig.Storage.Fsync)
	assert.Equal(t, true, *loadedConfig.Storage.Fsync)
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
  cleanupintervalms: 300000
engine:
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
  walcleanupcheckintervalms: 300000
`
	err := os.WriteFile(configFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	config := &AMQPConfig{}
	err = config.Load(configFile)
	assert.NoError(t, err)
	assert.Equal(t, ":8080", config.Network.Address)
	assert.Equal(t, "debug", config.Server.LogLevel)
	require.NotNil(t, config.Storage.Fsync)
	assert.Equal(t, false, *config.Storage.Fsync)
}

func TestConfigBuilder(t *testing.T) {
	config, err := NewConfigBuilder().
		WithAddress(":9090").
		WithPort(9090).
		WithMaxConnections(2000).
		WithStoragePath("/tmp/test-storage").
		WithFsync(true).
		WithLogging("debug", "/var/log/amqp.log").
		WithServerInfo("test-server", "1.0.0", "Test AMQP", "Test", "Test Corp").
		Build()

	require.NoError(t, err)

	assert.Equal(t, ":9090", config.Network.Address)
	assert.Equal(t, 9090, config.Network.Port)
	assert.Equal(t, 2000, config.Network.MaxConnections)
	assert.Equal(t, "/tmp/test-storage", config.Storage.Path)
	require.NotNil(t, config.Storage.Fsync)
	assert.Equal(t, true, *config.Storage.Fsync)
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

// validYAMLWithFsync builds a full, Validate()-passing config YAML whose
// storage.fsync line is fsyncLine (pass "" to omit the key entirely).
func validYAMLWithFsync(fsyncLine string) string {
	return `
network:
  address: ":8080"
  port: 8080
  maxconnections: 1000
  heartbeatintervalms: 60000
  tcpkeepalive: true
  tcpkeepaliveintervalms: 30000
storage:
  path: ./test-data
` + fsyncLine + `  cachemb: 64
  maxfiles: 100
  retentionms: 86400000
  checkpointintervalms: 5000
server:
  loglevel: debug
  maxchannelsperconnection: 2047
  maxframesize: 131072
  maxmessagesize: 16777216
  cleanupintervalms: 300000
engine:
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
  walcleanupcheckintervalms: 300000
`
}

func loadYAML(t *testing.T, content string) *AMQPConfig {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "cfg.yaml")
	require.NoError(t, os.WriteFile(f, []byte(content), 0644))
	// The file-load path in cmd/amqp-server seeds a ZERO-VALUE AMQPConfig{}
	// before Load (not DefaultConfig), so the tri-state must be exercised from
	// the zero value: an omitted fsync key stays nil there.
	c := &AMQPConfig{}
	require.NoError(t, c.Load(f))
	return c
}

// TestFsyncTriState_LoadPaths pins the tri-state user flag semantics and the
// loader trap: on a zero-value AMQPConfig{} (the --config path) an OMITTED
// fsync key must default to ON (durable), so honoring the flag can never
// silently make an upgraded broker non-durable. Only an explicit fsync:false
// disables it.
func TestFsyncTriState_LoadPaths(t *testing.T) {
	// Default flip: DefaultConfig is explicitly durable (non-nil true).
	def := DefaultConfig()
	require.NotNil(t, def.Storage.Fsync, "DefaultConfig must set an explicit fsync")
	assert.True(t, *def.Storage.Fsync)
	assert.True(t, def.FsyncEnabled())

	// (i) omitted key on a zero-value config => nil => ON.
	cOmit := loadYAML(t, validYAMLWithFsync(""))
	assert.Nil(t, cOmit.Storage.Fsync, "omitted fsync must stay nil on a zero-value load")
	assert.True(t, cOmit.FsyncEnabled(), "omitted fsync must default to ON (durable)")

	// (ii) explicit false => opt out.
	cFalse := loadYAML(t, validYAMLWithFsync("  fsync: false\n"))
	require.NotNil(t, cFalse.Storage.Fsync)
	assert.False(t, *cFalse.Storage.Fsync)
	assert.False(t, cFalse.FsyncEnabled())

	// (iii) explicit true => ON.
	cTrue := loadYAML(t, validYAMLWithFsync("  fsync: true\n"))
	require.NotNil(t, cTrue.Storage.Fsync)
	assert.True(t, *cTrue.Storage.Fsync)
	assert.True(t, cTrue.FsyncEnabled())
}

// TestFsyncTriState_EngineOverlay pins that the user flag threads to the engine
// view as the INVERTED WALSyncDisabled, and that every zero value means ON.
func TestFsyncTriState_EngineOverlay(t *testing.T) {
	// (iv) EngineConfig zero value => sync ON.
	assert.False(t, interfaces.EngineConfig{}.WALSyncDisabled, "zero-value EngineConfig must fsync")

	// Default config's engine view fsyncs.
	assert.False(t, DefaultConfig().GetEngine().WALSyncDisabled)

	// nil user flag (zero-value config) => engine view fsyncs.
	assert.False(t, (&AMQPConfig{}).GetEngine().WALSyncDisabled)

	// Explicit opt-out threads through GetEngine as WALSyncDisabled=true.
	c := DefaultConfig()
	dis := false
	c.Storage.Fsync = &dis
	assert.True(t, c.GetEngine().WALSyncDisabled, "fsync:false must set WALSyncDisabled=true")
}

// TestCRCTriState_LoadPaths pins the tri-state user flag semantics for CRC,
// mirroring TestFsyncTriState_LoadPaths: an OMITTED crc_check key must default
// to ON (safe), so honoring the flag can never silently disable integrity checks.
func TestCRCTriState_LoadPaths(t *testing.T) {
	def := DefaultConfig()
	require.NotNil(t, def.Storage.CRCCheck, "DefaultConfig must set an explicit crc_check")
	assert.True(t, *def.Storage.CRCCheck)
	assert.True(t, def.CRCEnabled())

	cOmit := loadYAML(t, validYAMLWithFsync(""))
	assert.Nil(t, cOmit.Storage.CRCCheck, "omitted crc_check must stay nil on a zero-value load")
	assert.True(t, cOmit.CRCEnabled(), "omitted crc_check must default to ON (safe)")

	cFalse := loadYAML(t, validYAMLWithFsync("  crccheck: false\n"))
	require.NotNil(t, cFalse.Storage.CRCCheck)
	assert.False(t, *cFalse.Storage.CRCCheck)
	assert.False(t, cFalse.CRCEnabled())

	cTrue := loadYAML(t, validYAMLWithFsync("  crccheck: true\n"))
	require.NotNil(t, cTrue.Storage.CRCCheck)
	assert.True(t, *cTrue.Storage.CRCCheck)
	assert.True(t, cTrue.CRCEnabled())
}

// TestCRCTriState_EngineOverlay pins that the user flag threads to the engine
// view as the INVERTED WALCRCDisabled, and that every zero value means ON.
func TestCRCTriState_EngineOverlay(t *testing.T) {
	assert.False(t, interfaces.EngineConfig{}.WALCRCDisabled, "zero-value EngineConfig must have CRC on")
	assert.False(t, DefaultConfig().GetEngine().WALCRCDisabled)
	assert.False(t, (&AMQPConfig{}).GetEngine().WALCRCDisabled)

	c := DefaultConfig()
	dis := false
	c.Storage.CRCCheck = &dis
	assert.True(t, c.GetEngine().WALCRCDisabled, "crc_check:false must set WALCRCDisabled=true")
}
