package config

// S6 seam test (WS-D limits × WS-E loader): server.maxqueues and
// server.maxexchanges are KNOWN keys — both spellings bind with no
// unknown-key warning, the AMQP_SERVER_* env aliases map onto them, and 0
// (unlimited) loads and validates.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_ServerLimits_KnownKeys(t *testing.T) {
	for name, yamlDoc := range map[string]string{
		"concatenated": "server:\n  maxqueues: 10\n  maxexchanges: 20\n",
		"snake_case":   "server:\n  max_queues: 10\n  max_exchanges: 20\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, report, err := LoadConfig(writeConfigFile(t, yamlDoc))
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			assert.Empty(t, report.UnknownKeys, "server limits must be known keys")
			assert.Equal(t, 10, cfg.Server.MaxQueues)
			assert.Equal(t, 20, cfg.Server.MaxExchanges)
		})
	}

	t.Run("env aliases bind", func(t *testing.T) {
		t.Setenv("AMQP_SERVER_MAXQUEUES", "7")
		t.Setenv("AMQP_SERVER_MAX_EXCHANGES", "9")
		cfg, report, err := LoadConfig("")
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		assert.Equal(t, 7, cfg.Server.MaxQueues)
		assert.Equal(t, 9, cfg.Server.MaxExchanges)
		assert.Contains(t, report.EnvKeys, "AMQP_SERVER_MAXQUEUES")
		assert.Contains(t, report.EnvKeys, "AMQP_SERVER_MAX_EXCHANGES")
		assert.NotContains(t, report.IgnoredEnv, "AMQP_SERVER_MAXQUEUES")
		assert.NotContains(t, report.IgnoredEnv, "AMQP_SERVER_MAX_EXCHANGES")
		assert.Empty(t, report.UnknownKeys)
	})

	t.Run("zero means unlimited and validates", func(t *testing.T) {
		cfg, report, err := LoadConfig(writeConfigFile(t, "server:\n  maxqueues: 0\n  maxexchanges: 0\n"))
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		assert.Empty(t, report.UnknownKeys)
		assert.Equal(t, 0, cfg.Server.MaxQueues)
		assert.Equal(t, 0, cfg.Server.MaxExchanges)
	})
}
