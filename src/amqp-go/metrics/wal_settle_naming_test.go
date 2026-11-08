package metrics

// FINDING 13 — amqp_wal_settle_drained_total was a GAUGE carrying a `_total`
// name, which Prometheus convention reserves for counters (and which makes
// `rate()` on it meaningless). It is renamed to amqp_wal_settle_drained, which
// is what the type and the doc now say.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestWALSettleDrained_IsNotTotalNamed(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := newCollector(reg, "amqp")
	c.BindWALBookkeepingStats(func() (int, int, uint64) { return 1, 1, 7 })

	names := make(map[string]bool)
	for _, n := range gatheredFamilyNames(t, reg) {
		names[n] = true
	}

	require.True(t, names["amqp_wal_settle_drained"],
		"the settle-drained gauge must be exposed as amqp_wal_settle_drained (no _total suffix)")
	require.False(t, names["amqp_wal_settle_drained_total"],
		"a gauge must not carry a _total name; the old name must be gone")
}
