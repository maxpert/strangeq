package management

import (
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/broker"
)

// bindingsPage is the minimal `page`-envelope shape (FR-CONV-052) used to read
// one page of /api/bindings.
type bindingsPage struct {
	Items []map[string]any `json:"items"`
}

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

	routingKeys := func(target string) []string {
		rec := e.do(http.MethodGet, target, nil, basicAuthHeader())
		wantStatus(t, rec, http.StatusOK)
		var items []map[string]any
		if strings.Contains(target, "page=") {
			items = decode[bindingsPage](t, rec).Items
		} else {
			items = decode[[]map[string]any](t, rec)
		}
		out := make([]string, 0, len(items))
		for _, it := range items {
			rk, _ := it["routing_key"].(string)
			out = append(out, rk)
		}
		return out
	}

	full := routingKeys("/api/bindings")
	if len(full) != 4 {
		t.Fatalf("bindings = %v, want the four declared", full)
	}
	if !sort.StringsAreSorted(full) {
		t.Fatalf("default binding order must be sorted by routing_key: %v", full)
	}

	// Paging must reproduce the full sorted list, identically every rep: the
	// concatenation across pages is the canonical order.
	for rep := 0; rep < 10; rep++ {
		p1 := routingKeys("/api/bindings?page=1&page_size=2")
		p2 := routingKeys("/api/bindings?page=2&page_size=2")
		concat := append(append([]string{}, p1...), p2...)
		if strings.Join(concat, ",") != strings.Join(full, ",") {
			t.Fatalf("rep %d: page1+page2 = %v, want the full sorted list %v", rep, concat, full)
		}
	}
}

// TestBindings_DefaultSortTieBreakByPropertiesKey pins the final tie-break of
// the default order: two bindings that differ ONLY in their arguments share
// routing_key/source/destination/destination_type, so their relative order must
// follow the synthesised properties_key.
func TestBindings_DefaultSortTieBreakByPropertiesKey(t *testing.T) {
	e := basicEnv(t, true, nil)
	fake := installFakeBindings(t, e)
	argsA := map[string]interface{}{"k": "a"}
	argsB := map[string]interface{}{"k": "b"}

	pkA := propertiesKey("tie-rk", argsA)
	pkB := propertiesKey("tie-rk", argsB)
	if pkA == pkB {
		t.Fatalf("fixture premise broken: distinct arguments must yield distinct properties_keys")
	}
	want := []string{pkA, pkB}
	sort.Strings(want)

	// Seed in DESCENDING properties_key order so a handler that applies no
	// default sort is caught deterministically (the fake returns seed order).
	higher, lower := argsA, argsB
	if pkB > pkA {
		higher, lower = argsB, argsA
	}
	fake.seed(broker.BindingView{Source: "tie-ex", Destination: "tie-q", DestinationType: "queue", RoutingKey: "tie-rk", Arguments: higher})
	fake.seed(broker.BindingView{Source: "tie-ex", Destination: "tie-q", DestinationType: "queue", RoutingKey: "tie-rk", Arguments: lower})

	rec := e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	items := decode[[]map[string]any](t, rec)
	got := make([]string, 0, len(items))
	for _, it := range items {
		pk, _ := it["properties_key"].(string)
		got = append(got, pk)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("tie-break order = %v, want properties_key ascending %v", got, want)
	}
}
