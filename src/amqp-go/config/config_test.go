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
	assert.Equal(t, "127.0.0.1:5672", config.Network.Address)
	assert.Equal(t, 5672, config.Network.Port)
	assert.Equal(t, 1000, config.Network.MaxConnections)
	assert.Equal(t, "./data", config.Storage.Path)
	assert.Equal(t, false, config.Security.TLSEnabled)
	assert.Equal(t, "amqp-go-server", config.Server.Name)

	// WALBatchTimeoutMS was 10ms → now 5ms (halves tail latency for lone durable messages)
	assert.Equal(t, int64(5), config.Engine.WALBatchTimeoutMS,
		"WALBatchTimeoutMS should be 5ms, was 10ms")

	// SegmentSize is 64MB (segstore v1)
	assert.Equal(t, int64(64*1024*1024), config.Engine.SegmentSize,
		"SegmentSize should be 64MB (segstore v1)")
	// HeartbeatIntervalMS is wired into sendConnectionTune (server package)
	assert.Equal(t, int64(60000), config.Network.HeartbeatIntervalMS,
		"HeartbeatIntervalMS should be 60s by default")

	// Hot cache budgets. The per-queue budget is 64 MiB: the hot cache
	// REPLACES the 64K-message ring, whose 80% spill threshold is 52,428
	// messages. A 16 MiB per-queue budget holds only ~25.4k messages at the
	// current charge formula, i.e. LESS than the ring it replaces, so the
	// FIFO consumer's next target was evicted and every read became a cold
	// read. The global budget stays at 256 MiB and must stay >= the
	// per-queue budget.
	assert.Equal(t, int64(64*1024*1024), config.Engine.HotCacheQueueBytes,
		"HotCacheQueueBytes should be 64 MiB per queue")
	assert.Equal(t, int64(256*1024*1024), config.Engine.HotCacheGlobalBytes,
		"HotCacheGlobalBytes should be 256 MiB globally")

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
		{
			name: "hot cache queue bytes at the 64 MiB maximum",
			modify: func(c *AMQPConfig) {
				c.Engine.HotCacheQueueBytes = 64 * 1024 * 1024
			},
			wantErr: false,
		},
		{
			name: "hot cache queue bytes above the 64 MiB maximum",
			modify: func(c *AMQPConfig) {
				c.Engine.HotCacheQueueBytes = 64*1024*1024 + 1
			},
			wantErr: true,
		},
		{
			name: "hot cache queue bytes below the 1 MiB minimum",
			modify: func(c *AMQPConfig) {
				c.Engine.HotCacheQueueBytes = 1024*1024 - 1
			},
			wantErr: true,
		},
		{
			name: "hot cache global bytes below the per-queue budget",
			modify: func(c *AMQPConfig) {
				c.Engine.HotCacheQueueBytes = 64 * 1024 * 1024
				c.Engine.HotCacheGlobalBytes = 32 * 1024 * 1024
			},
			wantErr: true,
		},
		{
			name: "hot cache global bytes above the 256 MiB maximum",
			modify: func(c *AMQPConfig) {
				c.Engine.HotCacheGlobalBytes = 256*1024*1024 + 1
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
	originalConfig.Storage.Durability = interfaces.DurabilityBarrier
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
	assert.Equal(t, interfaces.DurabilityBarrier, loadedConfig.Durability())
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
  durability: none
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
  maxqueuebacklog: 52428
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
	assert.Equal(t, interfaces.DurabilityNone, config.Storage.Durability)
	assert.Equal(t, interfaces.DurabilityNone, config.Durability())
}

func TestConfigBuilder(t *testing.T) {
	config, err := NewConfigBuilder().
		WithAddress(":9090").
		WithPort(9090).
		WithMaxConnections(2000).
		WithStoragePath("/tmp/test-storage").
		WithDurability(interfaces.DurabilityFlush).
		WithLogging("debug", "/var/log/amqp.log").
		WithServerInfo("test-server", "1.0.0", "Test AMQP", "Test", "Test Corp").
		Build()

	require.NoError(t, err)

	assert.Equal(t, ":9090", config.Network.Address)
	assert.Equal(t, 9090, config.Network.Port)
	assert.Equal(t, 2000, config.Network.MaxConnections)
	assert.Equal(t, "/tmp/test-storage", config.Storage.Path)
	assert.Equal(t, interfaces.DurabilityFlush, config.Durability())
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

// validYAMLWithStorageExtra builds a full, Validate()-passing config YAML whose
// storage section additionally carries the (already indented) lines in extra.
// Pass "" to omit them. It backs both the durability-key tests and the CRC
// tri-state tests.
func validYAMLWithStorageExtra(extra string) string {
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
` + extra + `  cachemb: 64
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
  maxqueuebacklog: 52428
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

// validYAMLWithEngineExtra builds a full, Validate()-passing config YAML whose
// engine section additionally carries the (already indented) lines in extra.
// Pass "" to omit them. It backs the single depth-high-water-mark key tests and
// the retired-engine-key rejection tests.
func validYAMLWithEngineExtra(extra string) string {
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
` + extra + `  walbatchsize: 1000
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

// validYAMLWithDurability is validYAMLWithStorageExtra specialised to a single
// storage.durability line ("" omits the key).
func validYAMLWithDurability(durabilityLine string) string {
	return validYAMLWithStorageExtra(durabilityLine)
}

func loadYAML(t *testing.T, content string) *AMQPConfig {
	t.Helper()
	dir := t.TempDir()
	f := filepath.Join(dir, "cfg.yaml")
	require.NoError(t, os.WriteFile(f, []byte(content), 0644))
	// The file-load path in cmd/amqp-server seeds a ZERO-VALUE AMQPConfig{}
	// before Load (not DefaultConfig), so an omitted durability key must be
	// exercised from the zero value.
	c := &AMQPConfig{}
	require.NoError(t, c.Load(f))
	return c
}

// TestDurability_DefaultsAndOverlay pins the single durability option end to
// end at the config layer: the barrier is the default, a zero-value config
// normalizes "" to the barrier, and GetEngine maps each of the three values
// onto the internal (WALSyncDisabled, CrashModel) transport.
func TestDurability_DefaultsAndOverlay(t *testing.T) {
	// DefaultConfig is explicitly durable (the barrier).
	assert.Equal(t, interfaces.DurabilityBarrier, DefaultConfig().Storage.Durability)
	assert.Equal(t, interfaces.DurabilityBarrier, DefaultConfig().Durability())

	// A zero-value config (and an omitted key) normalizes "" -> barrier.
	assert.Equal(t, interfaces.DurabilityBarrier, (&AMQPConfig{}).Durability())

	// The mapping table: each value -> (WALSyncDisabled, CrashModel).
	for _, tc := range []struct {
		dur         string
		wantSyncDis bool
		wantCrash   string
	}{
		{interfaces.DurabilityNone, true, ""},
		{interfaces.DurabilityBarrier, false, ""},
		{interfaces.DurabilityFlush, false, interfaces.CrashModelForceFlush},
	} {
		t.Run(tc.dur, func(t *testing.T) {
			c := DefaultConfig()
			c.Storage.Durability = tc.dur
			e := c.GetEngine()
			assert.Equal(t, tc.wantSyncDis, e.WALSyncDisabled, "WALSyncDisabled for durability %q", tc.dur)
			assert.Equal(t, tc.wantCrash, e.CrashModel, "CrashModel for durability %q", tc.dur)
			assert.Equal(t, tc.dur, e.Durability, "engine mirrors the durability mode")
		})
	}

	// A raw zero-value EngineConfig{} stays durable (fsync ON, barrier).
	assert.False(t, interfaces.EngineConfig{}.WALSyncDisabled, "zero-value EngineConfig must fsync")
	assert.Equal(t, "", interfaces.EngineConfig{}.CrashModel)
}

// TestDurability_LoadPaths pins the file-load path: an omitted key stays "" on
// a zero-value load but resolves to the barrier, and each explicit value
// round-trips.
func TestDurability_LoadPaths(t *testing.T) {
	cOmit := loadYAML(t, validYAMLWithDurability(""))
	assert.Equal(t, "", cOmit.Storage.Durability, "omitted durability stays empty on a zero-value load")
	assert.Equal(t, interfaces.DurabilityBarrier, cOmit.Durability(), "omitted durability defaults to the barrier")

	for _, v := range []string{interfaces.DurabilityNone, interfaces.DurabilityBarrier, interfaces.DurabilityFlush} {
		c := loadYAML(t, validYAMLWithDurability("  durability: "+v+"\n"))
		assert.Equal(t, v, c.Storage.Durability)
		assert.Equal(t, v, c.Durability())
	}
}

// TestCRCTriState_LoadPaths pins the tri-state user flag semantics for CRC: an
// OMITTED crc_check key must default to ON (safe), so honoring the flag can
// never silently disable integrity checks.
func TestCRCTriState_LoadPaths(t *testing.T) {
	def := DefaultConfig()
	require.NotNil(t, def.Storage.CRCCheck, "DefaultConfig must set an explicit crc_check")
	assert.True(t, *def.Storage.CRCCheck)
	assert.True(t, def.CRCEnabled())

	cOmit := loadYAML(t, validYAMLWithStorageExtra(""))
	assert.Nil(t, cOmit.Storage.CRCCheck, "omitted crc_check must stay nil on a zero-value load")
	assert.True(t, cOmit.CRCEnabled(), "omitted crc_check must default to ON (safe)")

	cFalse := loadYAML(t, validYAMLWithStorageExtra("  crccheck: false\n"))
	require.NotNil(t, cFalse.Storage.CRCCheck)
	assert.False(t, *cFalse.Storage.CRCCheck)
	assert.False(t, cFalse.CRCEnabled())

	cTrue := loadYAML(t, validYAMLWithStorageExtra("  crccheck: true\n"))
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

// TestValidateHotCacheEffectiveBudgets pins that the hot-cache budgets are
// validated against the EFFECTIVE values -- the ones the storage layer will
// actually apply -- and not against the RAW engine configuration.
//
// THE DEFECT: the validation compared the global budget against the RAW
// per-queue value, which is 0 when the per-queue budget is unset. The storage
// layer substitutes a 64 MiB per-queue default in that case, so
// `hot_cache_queue_bytes: 0` with `hot_cache_global_bytes: 33554432`
// (32 MiB) PASSED validation and produced newHotCache(64 MiB, 32 MiB): the
// per-queue budget could never bind, the GLOBAL eviction was always the one
// that fired, and the queue-local eviction -- the fix for the multi-queue
// starvation -- was dead in that configuration.
//
// WHAT A MUTATION WOULD BREAK: comparing the raw values again (or dropping
// the effective-value resolution) makes the 32 MiB-global / unset-per-queue
// combination validate, and the "must be rejected" assertion below fails.
func TestValidateHotCacheEffectiveBudgets(t *testing.T) {
	t.Run("the shipped defaults validate", func(t *testing.T) {
		cfg := DefaultConfig()
		require.NoError(t, cfg.Validate(),
			"the shipped defaults must validate: the global budget is larger than the per-queue budget")
	})

	t.Run("a 32 MiB global budget with an unset per-queue budget is rejected", func(t *testing.T) {
		cfg := DefaultConfig()
		// An explicit 0 is what a configuration file that sets
		// `hot_cache_queue_bytes: 0` binds to; the storage layer then
		// substitutes its 64 MiB default.
		cfg.Engine.HotCacheQueueBytes = 0
		cfg.Engine.HotCacheGlobalBytes = 32 * 1024 * 1024

		err := cfg.Validate()
		require.Error(t, err,
			"a global budget smaller than the EFFECTIVE per-queue budget must be rejected")
		assert.Contains(t, err.Error(), "hot cache global bytes")
	})

	t.Run("a global budget equal to the effective per-queue budget is accepted", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Engine.HotCacheQueueBytes = 0
		cfg.Engine.HotCacheGlobalBytes = 64 * 1024 * 1024
		require.NoError(t, cfg.Validate(),
			"a global budget equal to the effective per-queue budget is admissible")
	})

	t.Run("an explicitly set per-queue budget still bounds the global budget", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Engine.HotCacheQueueBytes = 64 * 1024 * 1024
		cfg.Engine.HotCacheGlobalBytes = 32 * 1024 * 1024
		require.Error(t, cfg.Validate(),
			"a global budget below the explicit per-queue budget must be rejected")
	})
}

// TestDurability_InvalidRejectedAtBoot pins that a value outside the three
// documented ones FAILS at boot through both entry points: the file-load path
// (Load) and Validate directly (which cmd/amqp-server and server.Builder call).
func TestDurability_InvalidRejectedAtBoot(t *testing.T) {
	for _, bad := range []string{"bogus", "true", "NONE", "force_flush", "flushed"} {
		t.Run(bad, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "cfg.yaml")
			require.NoError(t, os.WriteFile(f, []byte(validYAMLWithDurability("  durability: "+bad+"\n")), 0644))

			c := &AMQPConfig{}
			err := c.Load(f)
			require.Error(t, err, "an unknown durability must fail at boot")
			assert.Contains(t, err.Error(), "storage.durability")
			assert.Contains(t, err.Error(), interfaces.DurabilityNone)
			assert.Contains(t, err.Error(), interfaces.DurabilityBarrier)
			assert.Contains(t, err.Error(), interfaces.DurabilityFlush)

			// Validate directly, the entry point main and server.Builder use.
			v := DefaultConfig()
			v.Storage.Durability = bad
			require.Error(t, v.Validate(), "Validate must reject an unknown durability")
		})
	}
}

// TestLegacyDurabilityKeysRejected pins the HARD rejection of the retired
// storage.fsync and storage.crash_model keys, from a file AND from the
// environment. They must never fall through to the generic unknown-key warning,
// because ignoring them silently downgrades durability (crash_model: force_flush
// -> barrier) or silently changes throughput (fsync: false -> barrier).
func TestLegacyDurabilityKeysRejected(t *testing.T) {
	fileCases := []struct {
		name string
		body string
	}{
		{"fsync true", validYAMLWithStorageExtra("  fsync: true\n")},
		{"fsync false", validYAMLWithStorageExtra("  fsync: false\n")},
		{"crash_model barrier", validYAMLWithStorageExtra("  crash_model: barrier\n")},
		{"crash_model force_flush", validYAMLWithStorageExtra("  crash_model: force_flush\n")},
	}
	for _, tc := range fileCases {
		t.Run("file/"+tc.name, func(t *testing.T) {
			path := writeConfigFile(t, tc.body)
			_, report, err := LoadConfig(path)
			require.Error(t, err, "a retired durability key must fail at boot")
			assert.Nil(t, report, "LoadConfig must return no report on a hard error")
			assert.Contains(t, err.Error(), "storage.durability")
			assert.Contains(t, err.Error(), "durability: none")
			assert.Contains(t, err.Error(), "durability: barrier")
			assert.Contains(t, err.Error(), "durability: flush")
		})
	}

	t.Run("method/Load rejects fsync", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithStorageExtra("  fsync: true\n"))
		c := &AMQPConfig{}
		err := c.Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage.durability")
	})

	t.Run("env/AMQP_STORAGE_FSYNC", func(t *testing.T) {
		t.Setenv("AMQP_STORAGE_FSYNC", "false")
		_, _, err := LoadConfig("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage.durability")
	})

	t.Run("env/AMQP_STORAGE_CRASH_MODEL", func(t *testing.T) {
		t.Setenv("AMQP_STORAGE_CRASH_MODEL", "force_flush")
		_, _, err := LoadConfig("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage.durability")
	})

	t.Run("no legacy key means no error", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithDurability("  durability: flush\n"))
		cfg, report, err := LoadConfig(path)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		assert.NotContains(t, report.UnknownKeys, "storage.fsync")
		assert.NotContains(t, report.UnknownKeys, "storage.crashmodel")
		assert.Equal(t, interfaces.DurabilityFlush, cfg.Durability())
	})
}

// TestEngineMaxQueueBacklog_SingleKey pins the single honest key engine.maxqueuebacklog:
// DefaultConfig carries the shipped default (the old 65536 x 80 / 100 = 52428),
// an absent key keeps it, an explicit value binds verbatim, and an explicit 0
// binds to 0 at the config layer (the broker resolves 0 to the default at use).
func TestEngineMaxQueueBacklog_SingleKey(t *testing.T) {
	assert.Equal(t, uint64(52428), interfaces.DefaultMaxQueueBacklog)
	assert.Equal(t, interfaces.DefaultMaxQueueBacklog, DefaultConfig().Engine.MaxQueueBacklog)

	t.Run("absent key keeps the default", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithEngineExtra(""))
		cfg, report, err := LoadConfig(path)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		assert.Equal(t, uint64(52428), cfg.Engine.MaxQueueBacklog)
		assert.NotContains(t, report.UnknownKeys, "engine.maxqueuebacklog")
	})

	t.Run("explicit value binds", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithEngineExtra("  maxqueuebacklog: 1000\n"))
		cfg, report, err := LoadConfig(path)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		assert.Equal(t, uint64(1000), cfg.Engine.MaxQueueBacklog)
		assert.NotContains(t, report.UnknownKeys, "engine.maxqueuebacklog")
	})

	t.Run("zero binds to zero at the config layer", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithEngineExtra("  maxqueuebacklog: 0\n"))
		cfg, report, err := LoadConfig(path)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate(), "0 is legal: the broker resolves it to the default")
		assert.Equal(t, uint64(0), cfg.Engine.MaxQueueBacklog)
		assert.NotContains(t, report.UnknownKeys, "engine.maxqueuebacklog")
	})
}

// TestLegacyEngineDepthKeysRejected pins the HARD rejection of the retired
// engine.ringbuffersize / engine.spillthresholdpercent /
// engine.depthhighwmoverride / engine.depthhighwm keys, from a file AND from the
// environment. They must never fall through to the generic unknown-key warning:
// ignoring ringbuffersize/spillthresholdpercent silently changes the per-queue
// publisher backpressure depth, and ignoring depthhighwm silently drops the
// publisher's chosen backlog to the default, so the retired keys must be
// migrated, not dropped.
func TestLegacyEngineDepthKeysRejected(t *testing.T) {
	fileCases := []struct {
		name string
		line string
		seen string
	}{
		{"ringbuffersize", "  ringbuffersize: 65536\n", "engine.ringbuffersize"},
		{"spillthresholdpercent", "  spillthresholdpercent: 80\n", "engine.spillthresholdpercent"},
		{"depthhighwmoverride", "  depthhighwmoverride: 1000\n", "engine.depthhighwmoverride"},
		{"depthhighwm", "  depthhighwm: 1000\n", "engine.depthhighwm"},
	}
	for _, tc := range fileCases {
		t.Run("file/"+tc.name, func(t *testing.T) {
			path := writeConfigFile(t, validYAMLWithEngineExtra(tc.line))
			_, report, err := LoadConfig(path)
			require.Error(t, err, "a retired engine depth key must fail at boot")
			assert.Nil(t, report, "LoadConfig must return no report on a hard error")
			assert.Contains(t, err.Error(), tc.seen+" is no longer supported; use engine.maxqueuebacklog instead")
			assert.Contains(t, err.Error(), "ringbuffersize: N + spillthresholdpercent: P -> maxqueuebacklog: N*P/100")
			assert.Contains(t, err.Error(), "depthhighwmoverride: N")
			assert.Contains(t, err.Error(), "depthhighwm: N")
		})
	}

	// The snake_case spelling normalizes onto the same dotted path.
	t.Run("file/snake_case ring_buffer_size", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithEngineExtra("  ring_buffer_size: 65536\n"))
		_, _, err := LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "engine.ringbuffersize is no longer supported")
	})

	envCases := []struct {
		name  string
		env   string
		value string
		seen  string
	}{
		{"ringbuffersize", "AMQP_ENGINE_RINGBUFFERSIZE", "65536", "engine.ringbuffersize"},
		{"spillthresholdpercent", "AMQP_ENGINE_SPILLTHRESHOLDPERCENT", "80", "engine.spillthresholdpercent"},
		{"depthhighwmoverride", "AMQP_ENGINE_DEPTHHIGHWMOVERRIDE", "1000", "engine.depthhighwmoverride"},
		{"depthhighwm", "AMQP_ENGINE_DEPTHHIGHWM", "1000", "engine.depthhighwm"},
	}
	for _, tc := range envCases {
		t.Run("env/"+tc.name, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			_, _, err := LoadConfig("")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.seen+" is no longer supported")
		})
	}

	t.Run("the new key means no error", func(t *testing.T) {
		path := writeConfigFile(t, validYAMLWithEngineExtra("  maxqueuebacklog: 4096\n"))
		cfg, report, err := LoadConfig(path)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		assert.Equal(t, uint64(4096), cfg.Engine.MaxQueueBacklog)
		assert.NotContains(t, report.UnknownKeys, "engine.maxqueuebacklog")
	})
}

// TestGenerateConfig_EmitsDurability pins that --generate-config writes the new
// key and none of the retired ones.
func TestGenerateConfig_EmitsDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generated.yaml")
	require.NoError(t, DefaultConfig().Save(path))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	out := string(data)
	assert.Contains(t, out, "durability: barrier")
	assert.NotContains(t, out, "fsync")
	assert.NotContains(t, out, "crash_model")
}

// TestDurability_BuilderWiring pins the fluent builder setter.
func TestDurability_BuilderWiring(t *testing.T) {
	cfg, err := NewConfigBuilder().WithDurability(interfaces.DurabilityFlush).Build()
	require.NoError(t, err)
	assert.Equal(t, interfaces.DurabilityFlush, cfg.Durability())

	_, err = NewConfigBuilder().WithDurability("bogus").Build()
	require.Error(t, err, "Build must reject an unknown durability")
}
