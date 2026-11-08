package metrics

// FINDING 4 — the Grafana dashboard must not query a family this broker can
// never emit.
//
// The dashboard (scripts/monitoring/grafana-dashboards/amqp-dashboard.json) had
// a panel using rate(amqp_messages_dropped_total[1m]); that family has no
// production writer, so the panel was silently empty forever. This gate parses
// every PromQL `expr` in the dashboard, extracts the amqp_* metric names, and
// asserts each names a family in the pinned registry golden. A dashboard that
// references a removed/dead family fails here, at build time, rather than
// plotting a flat line an operator cannot explain.

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const grafanaDashboardPath = "../../../scripts/monitoring/grafana-dashboards/amqp-dashboard.json"

var amqpMetricRef = regexp.MustCompile(`\bamqp_[a-zA-Z0-9_]+`)

// deadFamilies are families that are registered (or were) but can never be
// emitted in production, or were removed outright. A dashboard query naming any
// of them is a permanently-empty panel.
var deadFamilies = map[string]bool{
	"amqp_messages_dropped_total":          true,
	"amqp_message_age_seconds":             true,
	"amqp_segment_checkpoint_errors_total": true,
}

// collectExprs walks decoded JSON for every "expr" string.
func collectExprs(v any, out *[]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if k == "expr" {
				if s, ok := child.(string); ok {
					*out = append(*out, s)
				}
			}
			collectExprs(child, out)
		}
	case []any:
		for _, child := range t {
			collectExprs(child, out)
		}
	}
}

func TestGrafanaDashboard_ExpressionsReferenceKnownFamilies(t *testing.T) {
	data, err := os.ReadFile(grafanaDashboardPath)
	require.NoError(t, err, "the Grafana dashboard must exist at %s", grafanaDashboardPath)

	var doc any
	require.NoError(t, json.Unmarshal(data, &doc))

	var exprs []string
	collectExprs(doc, &exprs)
	require.NotEmpty(t, exprs, "the dashboard must have at least one PromQL expression")

	golden := make(map[string]bool, len(amqpFamilyGolden))
	for _, name := range amqpFamilyGolden {
		golden[name] = true
	}

	for _, expr := range exprs {
		for _, raw := range amqpMetricRef.FindAllString(expr, -1) {
			name := normalizeHistogramSuffix(raw, golden)

			require.False(t, deadFamilies[name],
				"dashboard expr %q references dead/never-emitted family %q", expr, name)

			require.True(t, golden[name],
				"dashboard expr %q references %q, which is not a registered family "+
					"(the panel would be permanently empty)", expr, name)
		}
	}
}

// normalizeHistogramSuffix maps the derived histogram series (_bucket/_count/
// _sum) back to the base family when that base is a known family.
func normalizeHistogramSuffix(name string, golden map[string]bool) string {
	for _, suffix := range []string{"_bucket", "_count", "_sum"} {
		if strings.HasSuffix(name, suffix) {
			base := strings.TrimSuffix(name, suffix)
			if golden[base] {
				return base
			}
		}
	}
	return name
}
