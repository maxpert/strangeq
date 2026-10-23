package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCrashScript_ConfigGeneration proves the crash/stress scripts generate a
// configuration the loader accepts with zero warnings. It exercises the real
// shell helper rather than duplicating the YAML, so a script-only edit that
// breaks the loader surfaces here.
func TestCrashScript_ConfigGeneration(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	helper, err := filepath.Abs("../scripts/lib/test_env.sh")
	require.NoError(t, err)
	_, err = os.Stat(helper)
	require.NoError(t, err, "scripts/lib/test_env.sh must exist")

	storagePath := t.TempDir()
	cmd := exec.Command(bash, "-c", `. ../scripts/lib/test_env.sh && gen_config "$0" 23456 23457`, storagePath)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "AMQP_PORT=", "AMQP_TELEMETRY_PORT=")
	out, err := cmd.Output()
	require.NoError(t, err, "gen_config failed: %s", out)

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, out, 0600))

	cfg, report, err := LoadConfig(configPath)
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	assert.Empty(t, report.UnknownKeys, "generated config must not contain unknown keys")
	assert.Equal(t, storagePath, cfg.Storage.Path)
	assert.Equal(t, "127.0.0.1:23456", cfg.Network.Address)
	assert.True(t, cfg.Telemetry.Enabled)
	assert.Equal(t, 23457, cfg.Telemetry.Port)
	assert.Equal(t, "127.0.0.1", cfg.Telemetry.Address)
	assert.True(t, cfg.FsyncEnabled())
}
