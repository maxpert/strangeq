package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServerBuilder(t *testing.T) {
	builder := NewServerBuilder()
	assert.NotNil(t, builder)
	assert.NotNil(t, builder.config)
	assert.Equal(t, ":5672", builder.config.Network.Address)
}

func TestServerBuilderWithConfig(t *testing.T) {
	customConfig := config.DefaultConfig()
	customConfig.Network.Address = ":8080"
	
	builder := NewServerBuilderWithConfig(customConfig)
	assert.NotNil(t, builder)
	assert.Equal(t, ":8080", builder.config.Network.Address)
}

func TestServerBuilderFluentAPI(t *testing.T) {
	builder := NewServerBuilder().
		WithAddress(":9090").
		WithPort(9090).
		WithMaxConnections(500).
		WithZapLogger("debug").
		WithDefaultBroker().
		WithMemoryStorage()
	
	assert.Equal(t, ":9090", builder.config.Network.Address)
	assert.Equal(t, 9090, builder.config.Network.Port)
	assert.Equal(t, 500, builder.config.Network.MaxConnections)
	assert.NotNil(t, builder.logger)
	assert.NotNil(t, builder.broker)
	assert.Equal(t, "memory", builder.config.Storage.Backend)
}

func TestServerBuilderWithTLS(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	
	// Create dummy cert files
	err := os.WriteFile(certFile, []byte("fake cert"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, []byte("fake key"), 0644)
	require.NoError(t, err)
	
	builder := NewServerBuilder().
		WithTLS(certFile, keyFile)
	
	assert.True(t, builder.config.Security.TLSEnabled)
	assert.Equal(t, certFile, builder.config.Security.TLSCertFile)
	assert.Equal(t, keyFile, builder.config.Security.TLSKeyFile)
}

func TestServerBuilderWithFileAuthentication(t *testing.T) {
	tmpDir := t.TempDir()
	userFile := filepath.Join(tmpDir, "users.txt")
	
	builder := NewServerBuilder().
		WithFileAuthentication(userFile)
	
	assert.True(t, builder.config.Security.AuthenticationEnabled)
	assert.Equal(t, "file", builder.config.Security.AuthenticationBackend)
	assert.Equal(t, userFile, builder.config.Security.AuthenticationConfig["user_file"])
}

func TestServerBuilderWithProtocolLimits(t *testing.T) {
	builder := NewServerBuilder().
		WithProtocolLimits(1000, 64*1024, 8*1024*1024)
	
	assert.Equal(t, 1000, builder.config.Server.MaxChannelsPerConnection)
	assert.Equal(t, 64*1024, builder.config.Server.MaxFrameSize)
	assert.Equal(t, int64(8*1024*1024), builder.config.Server.MaxMessageSize)
}

func TestServerBuilderBuild(t *testing.T) {
	server, err := NewServerBuilder().
		WithAddress(":8080").
		WithDefaultBroker().
		WithZapLogger("info").
		Build()
	
	require.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, ":8080", server.Addr)
	assert.NotNil(t, server.Log)
	assert.NotNil(t, server.Broker)
	assert.NotNil(t, server.Config)
}

func TestServerBuilderBuildValidationError(t *testing.T) {
	_, err := NewServerBuilder().
		WithPort(0). // Invalid port
		Build()
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid configuration")
}

func TestServerBuilderBuildUnsafe(t *testing.T) {
	server := NewServerBuilder().
		WithPort(0). // Invalid port
		BuildUnsafe()
	
	assert.NotNil(t, server)
	// Server is created despite invalid configuration
}

func TestServerBuilderWithCustomConfig(t *testing.T) {
	customConfig := config.DefaultConfig()
	customConfig.Network.MaxConnections = 2000
	customConfig.Server.LogLevel = "debug"
	
	server, err := NewServerBuilder().
		WithConfig(customConfig).
		Build()
	
	require.NoError(t, err)
	assert.Equal(t, 2000, server.Config.Network.MaxConnections)
	assert.Equal(t, "debug", server.Config.Server.LogLevel)
}

func TestZapLoggerAdapter(t *testing.T) {
	builder := NewServerBuilder().WithZapLogger("debug")
	assert.NotNil(t, builder.logger)
	
	// Test that we can call logging methods without panic
	adapter := builder.logger.(*ZapLoggerAdapter)
	adapter.Info("test message")
	adapter.Debug("debug message")
	adapter.Warn("warning message")
	adapter.Error("error message")
	
	// Test With method
	newLogger := adapter.With()
	assert.NotNil(t, newLogger)
	
	// Test Sync (may fail with stderr sync error in test environment)
	_ = adapter.Sync()
}

func TestBrokerAdapter(t *testing.T) {
	builder := NewServerBuilder().WithDefaultBroker()
	assert.NotNil(t, builder.broker)
	
	adapter := builder.broker.(*BrokerAdapter)
	assert.NotNil(t, adapter.broker)
	
	// Test that basic broker operations don't panic
	err := adapter.DeclareExchange("test", "direct", false, false, false, nil)
	assert.NoError(t, err)
	
	_, err = adapter.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)
	
	err = adapter.BindQueue("test-queue", "test", "routing.key", nil)
	assert.NoError(t, err)
}

func TestServerBuilderLogLevels(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error", "invalid"}
	
	for _, level := range levels {
		t.Run("level_"+level, func(t *testing.T) {
			builder := NewServerBuilder().WithZapLogger(level)
			assert.NotNil(t, builder.logger)
			
			server, err := builder.Build()
			require.NoError(t, err)
			assert.NotNil(t, server.Log)
		})
	}
}