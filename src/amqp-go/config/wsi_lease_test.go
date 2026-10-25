package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// CFG-1 (WS-I Phase B): publish-inflight lease budget configuration.
func TestWSI_LeaseConfig(t *testing.T) {
	// 0 → the default G = 4 × WALBatchSealBytesDefault = 256 MiB.
	cfg := DefaultConfig()
	assert.Equal(t, int64(0), cfg.Network.PublishInflightBudgetBytes,
		"the default-config diff is exactly PublishInflightBudgetBytes: 0")
	assert.Equal(t, int64(4*storage.WALBatchSealBytesDefault), cfg.PublishInflightBudget())
	assert.Equal(t, int64(256<<20), cfg.PublishInflightBudget())

	// −1 → disabled (the limiter grants everything).
	cfg = DefaultConfig()
	cfg.Network.PublishInflightBudgetBytes = -1
	require.NoError(t, cfg.Validate())
	assert.Equal(t, int64(-1), cfg.PublishInflightBudget())
	l := cfg.PublishInflightBudget()
	assert.LessOrEqual(t, l, int64(0), "a disabled budget is non-positive for NewPublishLimiter")

	// −2 is rejected.
	cfg = DefaultConfig()
	cfg.Network.PublishInflightBudgetBytes = -2
	require.Error(t, cfg.Validate(), "values < -1 must be rejected")

	// 512 KiB is rejected (explicit budgets must be ≥ 1 MiB).
	cfg = DefaultConfig()
	cfg.Network.PublishInflightBudgetBytes = 512 << 10
	require.Error(t, cfg.Validate(), "0 < v < 1 MiB must be rejected")

	// 1 MiB is the smallest explicit budget and resolves to itself.
	cfg = DefaultConfig()
	cfg.Network.PublishInflightBudgetBytes = 1 << 20
	require.NoError(t, cfg.Validate())
	assert.Equal(t, int64(1<<20), cfg.PublishInflightBudget())

	// An explicit large budget resolves to itself.
	cfg = DefaultConfig()
	cfg.Network.PublishInflightBudgetBytes = 300 << 20
	require.NoError(t, cfg.Validate())
	assert.Equal(t, int64(300<<20), cfg.PublishInflightBudget())
}

// CFG-1 (generate-config): the emitted default config carries the new key
// with value 0, and the diff against the pre-WS-I keys is exactly one key.
func TestWSI_LeaseGenerateConfigDiff(t *testing.T) {
	out := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, DefaultConfig().Save(out))
	data, err := os.ReadFile(out)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, yaml.Unmarshal(data, &doc))
	network, ok := doc["network"].(map[string]interface{})
	require.True(t, ok, "generated config must have a network section")
	v, ok := network["publishinflightbudgetbytes"]
	require.True(t, ok, "generate-config must emit publishinflightbudgetbytes")
	assert.Equal(t, 0, v, "the emitted value is 0 (= default 256 MiB)")

	// The pre-WS-I network keys are exactly the old set + this one.
	for _, k := range []string{
		"address", "port", "maxconnections", "heartbeatintervalms",
		"tcpkeepalive", "tcpkeepaliveintervalms", "readbuffersize", "writebuffersize",
		"readcoalescebuffersize", "readeroverflowflowbytes", "readeroverflowhardcapbytes",
		"readerbackpressureprobems", "publishinflightbudgetbytes",
	} {
		assert.Contains(t, network, k)
	}
	assert.Len(t, network, 13, "the generate-config diff is exactly one key")
}

// CFG-1: the documented sample config loads with the key and no warnings.
func TestWSI_LeaseSampleConfigLoads(t *testing.T) {
	body, err := os.ReadFile("../config.sample.yaml")
	require.NoError(t, err)
	require.Contains(t, string(body), "publishinflightbudgetbytes: 0")
	cfg, report, err := LoadConfig("../config.sample.yaml")
	require.NoError(t, err)
	assert.Empty(t, report.UnknownKeys)
	assert.Equal(t, int64(0), cfg.Network.PublishInflightBudgetBytes)
	assert.True(t, strings.Contains(string(body), "publishinflightbudgetbytes"))
}
