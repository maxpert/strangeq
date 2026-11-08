package management

import (
	"net/http"
	"strings"
	"testing"
)

// TestOverview_FieldsAndCoherence (FR-API-001/002/008, spec §11 test 38).
func TestOverview_FieldsAndCoherence(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)

	out := decode[map[string]any](t, rec)
	for _, key := range []string{
		"management_version", "product", "product_version", "amqp_go_version",
		"platform", "erlang_version", "node", "uptime_seconds", "listeners",
		"snapshot_coherent", "object_totals", "queue_totals", "alarms", "unsafe_recovery",
	} {
		if _, ok := out[key]; !ok {
			t.Fatalf("overview is missing %q: %s", key, truncate(rec.Body.String()))
		}
	}
	if out["erlang_version"] != nil {
		t.Fatalf("erlang_version must be null on a Go broker")
	}
	if out["product"] != "StrangeQ" {
		t.Fatalf("product = %v", out["product"])
	}
	totals, _ := out["object_totals"].(map[string]any)
	if totals == nil {
		t.Fatalf("object_totals missing")
	}
	if v, present := totals["bindings"]; !present || v != nil {
		t.Fatalf("object_totals.bindings must be present and null (BOB-4 unbuilt), got %#v", v)
	}
	// BOB-11 is unbuilt in this tree: the snapshot must honestly report
	// incoherence rather than claim a coherent read it did not perform.
	if coherent, ok := out["snapshot_coherent"].(bool); !ok || coherent {
		t.Fatalf("snapshot_coherent must be false when the BOB-11 seam is absent, got %#v", out["snapshot_coherent"])
	}
	listeners, _ := out["listeners"].([]any)
	if len(listeners) != 1 {
		t.Fatalf("listeners = %#v, want one amqp listener", out["listeners"])
	}
}

// TestExchanges_TypeEnumAndMessageStatsNull (FR-API-050/DIV-4, spec §11 test 49).
func TestExchanges_TypeEnumAndMessageStatsNull(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/exchanges", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	exs := decode[[]exchangeInfo](t, rec)
	if len(exs) == 0 {
		t.Fatalf("the default exchange set is non-empty")
	}
	valid := map[string]bool{"direct": true, "fanout": true, "topic": true, "headers": true}
	for _, ex := range exs {
		if !valid[ex.Type] {
			t.Fatalf("exchange %q has a type outside the enum: %q", ex.Name, ex.Type)
		}
		if ex.MessageStats != nil {
			t.Fatalf("message_stats must be null while BOB-2 is unbuilt (%q)", ex.Name)
		}
		if ex.VHost != "/" {
			t.Fatalf("exchange vhost = %q, want /", ex.VHost)
		}
	}
	if !strings.Contains(rec.Body.String(), `"message_stats":null`) {
		t.Fatalf("message_stats must serialise as null: %s", truncate(rec.Body.String()))
	}
}

// TestConnections_FieldsAndState (FR-API-020/025, spec §11 test 44).
func TestConnections_FieldsAndState(t *testing.T) {
	e := basicEnv(t, true, nil)
	peer := e.injectConnection(t, "shape-conn", "alice")
	defer peer.Close()

	rec := e.do(http.MethodGet, "/api/connections", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	conns := decode[[]connectionInfo](t, rec)
	if len(conns) != 1 {
		t.Fatalf("connections = %+v", conns)
	}
	c := conns[0]
	if c.Name != "shape-conn" || c.User != "alice" || c.VHost != "/" {
		t.Fatalf("connection identity = %+v", c)
	}
	if c.Protocol != "AMQP 0-9-1" {
		t.Fatalf("protocol = %q", c.Protocol)
	}
	if c.State != "running" {
		t.Fatalf("state = %q, want running", c.State)
	}
	if c.Timeout != nil || c.RecvOct != nil || c.SendOct != nil || c.ChannelsDetail != nil {
		t.Fatalf("unsourced fields must be null: %+v", c)
	}
	if c.PeerHost == "" || c.PeerPort == nil {
		t.Fatalf("peer host/port must be derived from the socket: %+v", c)
	}

	// Single connection + 404.
	if got := e.do(http.MethodGet, "/api/connections/shape-conn", nil, basicAuthHeader()); got.Code != http.StatusOK {
		t.Fatalf("single connection must be 200, got %d", got.Code)
	}
	if got := e.do(http.MethodGet, "/api/connections/nope", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("absent connection must be 404, got %d", got.Code)
	}
	byUser := e.do(http.MethodGet, "/api/connections/username/alice", nil, basicAuthHeader())
	wantStatus(t, byUser, http.StatusOK)
	if got := decode[[]connectionInfo](t, byUser); len(got) != 1 {
		t.Fatalf("by-username must return the connection: %+v", got)
	}
}

// TestConsumers_BaseFieldsWithoutSeam pins the M0 fallback: without the BOB-8
// seam the core fields are served and the linkage fields are null.
func TestConsumers_BaseFieldsWithoutSeam(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "cons-q", queueDeclareRequest{Durable: true})
	c := consumerForTest("tag-1", "cons-q")
	if err := e.srv.Broker.RegisterConsumer("cons-q", c.ID, c); err != nil {
		t.Fatalf("register: %v", err)
	}
	rec := e.do(http.MethodGet, "/api/consumers", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	out := decode[[]consumerInfo](t, rec)
	if len(out) != 1 {
		t.Fatalf("consumers = %+v", out)
	}
	if out[0].ConsumerTag != "tag-1" || out[0].Queue != "cons-q" {
		t.Fatalf("base fields = %+v", out[0])
	}
	if out[0].Channel != nil || out[0].Connection != nil || out[0].Unacked != nil {
		t.Fatalf("linkage fields must be null without the BOB-8 seam: %+v", out[0])
	}
}
