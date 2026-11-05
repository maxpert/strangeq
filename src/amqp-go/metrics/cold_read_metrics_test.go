package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestColdReadMetrics_Registered pins the cold-read metric names KEPT by the
// safe core (WS-COLDREAD-PATH, spec §6). The campaign harness scrapes
// /metrics into raw/metrics/<cell>.final.txt, so the series must be registered
// under the shipped "amqp" namespace with these exact names.
//
// The pool/lease series (cold_read_buffer_pool_*, cold_read_leases_outstanding,
// hot_cache_bytes) are deliberately ABSENT: the pooled-buffer mechanism was
// rejected on measurement and rolled back to this safe core.
func TestColdReadMetrics_Registered(t *testing.T) {
	// Namespace "amqp" is the production one; no other test in this package
	// registers it, so this is the only registration of these names.
	NewCollector("amqp")

	want := []string{
		"amqp_delivery_hot_hits_total",
		"amqp_delivery_cold_reads_total",
		"amqp_cold_read_one_pread_total",
		"amqp_cold_read_two_pread_fallback_total",
		"amqp_cold_read_current_file_total",
		"amqp_cold_read_old_file_total",
	}
	present := make(map[string]bool, len(want))
	for _, n := range want {
		present[n] = false
	}

	fams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, fam := range fams {
		if _, ok := present[fam.GetName()]; ok {
			present[fam.GetName()] = true
		}
	}
	for _, n := range want {
		if !present[n] {
			t.Errorf("metric %q is not registered on the default registry", n)
		}
	}
}
