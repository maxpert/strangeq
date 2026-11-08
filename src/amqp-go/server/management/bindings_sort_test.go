package management

import (
	"net/http"
	"sort"
	"strings"
	"testing"
)

// TestBindings_DefaultSortStable pins WS4 review finding 6: with no `sort`
// parameter, GET /api/bindings must apply a stable default order so paging is
// deterministic even though the broker's ListBindings returns sync.Map Range
// order.
func TestBindings_DefaultSortStable(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "sort-ex", "topic")
	// Four queues, bound in a deliberately unsorted order.
	for _, q := range []string{"sort-q-d", "sort-q-a", "sort-q-c", "sort-q-b"} {
		e.declareQueue(t, q, queueDeclareRequest{})
	}
	for _, rk := range []string{"rk-d", "rk-a", "rk-c", "rk-b"} {
		wantStatus(t, e.bindQueue(t, "sort-ex", "sort-q-"+rk[len(rk)-1:], rk, nil), http.StatusCreated)
	}

	routingKeys := func() []string {
		rec := e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader())
		wantStatus(t, rec, http.StatusOK)
		items := decode[[]map[string]any](t, rec)
		out := make([]string, 0, len(items))
		for _, it := range items {
			rk, _ := it["routing_key"].(string)
			out = append(out, rk)
		}
		return out
	}

	first := routingKeys()
	if len(first) != 4 {
		t.Fatalf("bindings = %v, want the four declared", first)
	}
	if !sort.StringsAreSorted(first) {
		t.Fatalf("default binding order must be sorted by routing_key: %v", first)
	}
	for i := 0; i < 20; i++ {
		got := routingKeys()
		if strings.Join(got, ",") != strings.Join(first, ",") {
			t.Fatalf("binding order is not stable across calls: %v vs %v", got, first)
		}
	}
}
