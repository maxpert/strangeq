package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigValidate_NegativeMaxQueues (T15.4): negative limits are a
// validation error; 0 (the default) means unlimited.
func TestConfigValidate_NegativeMaxQueues(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, 0, cfg.Server.MaxQueues, "default MaxQueues is 0 (unlimited)")
	assert.Equal(t, 0, cfg.Server.MaxExchanges, "default MaxExchanges is 0 (unlimited)")
	require.NoError(t, cfg.Validate())

	cfg = DefaultConfig()
	cfg.Server.MaxQueues = -1
	require.Error(t, cfg.Validate())
	assert.Contains(t, cfg.Validate().Error(), "max queues")

	cfg = DefaultConfig()
	cfg.Server.MaxExchanges = -2
	require.Error(t, cfg.Validate())
	assert.Contains(t, cfg.Validate().Error(), "max exchanges")

	// Positive values validate.
	cfg = DefaultConfig()
	cfg.Server.MaxQueues = 100
	cfg.Server.MaxExchanges = 50
	require.NoError(t, cfg.Validate())
}
