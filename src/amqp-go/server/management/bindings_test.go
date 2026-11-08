package management

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
)

// --- propertiesKey (FR-API-090/DATA-010/DIV-12) ---

// TestPropertiesKey_EmptyArgsIsRoutingKey (spec §5.1 test 7).
func TestPropertiesKey_EmptyArgsIsRoutingKey(t *testing.T) {
	if got := propertiesKey("rk", nil); got != "rk" {
		t.Fatalf("propertiesKey(rk, nil) = %q, want rk", got)
	}
	if got := propertiesKey("rk", map[string]any{}); got != "rk" {
		t.Fatalf("propertiesKey(rk, empty) = %q, want rk", got)
	}
}

// TestPropertiesKey_DeterministicSortedKeys (spec §5.1 test 8).
func TestPropertiesKey_DeterministicSortedKeys(t *testing.T) {
	a := propertiesKey("rk", map[string]any{"b": 2, "a": 1})
	b := propertiesKey("rk", map[string]any{"a": 1, "b": 2})
	if a != b {
		t.Fatalf("key insertion order must not matter: %q != %q", a, b)
	}
	if !strings.HasPrefix(a, "rk~") {
		t.Fatalf("a non-empty argument table must produce rk~<hex>, got %q", a)
	}
	if h := strings.TrimPrefix(a, "rk~"); len(h) != 64 {
		t.Fatalf("the digest must be 64 hex chars, got %d (%q)", len(h), h)
	}
	c := propertiesKey("rk", map[string]any{"a": 1, "b": 3})
	if a == c {
		t.Fatalf("changing a value must change the key: %q", a)
	}
}

// --- BOB-4: list endpoints ---

func (e *env) declareExchange(t *testing.T, name, typ string) {
	t.Helper()
	rec := e.do(http.MethodPut, "/api/exchanges/%2F/"+name, e.jsonBody(t, exchangeDeclareRequest{Type: typ}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
}

func (e *env) bindQueue(t *testing.T, exchange, queue, rk string, args map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body := e.jsonBody(t, bindingCreateRequest{RoutingKey: rk, Arguments: args})
	return e.do(http.MethodPost, "/api/bindings/%2F/e/"+exchange+"/q/"+queue, body, basicAuthHeader())
}

func (e *env) bindExchange(t *testing.T, src, dst, rk string, args map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body := e.jsonBody(t, bindingCreateRequest{RoutingKey: rk, Arguments: args})
	return e.do(http.MethodPost, "/api/bindings/%2F/e/"+src+"/e/"+dst, body, basicAuthHeader())
}

// TestBindings_List_ShapeAndEnvelope (FR-API-090/153, spec §5.4 test 23).
func TestBindings_List_ShapeAndEnvelope(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "x", "q", "rk", map[string]any{"k": "v"}), http.StatusCreated)

	bare := e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader())
	wantStatus(t, bare, http.StatusOK)
	items := decode[[]map[string]any](t, bare)
	if len(items) != 1 {
		t.Fatalf("want one binding, got %d (%s)", len(items), truncate(bare.Body.String()))
	}
	wantKeys := []string{"source", "vhost", "destination", "destination_type", "routing_key", "arguments", "properties_key"}
	if len(items[0]) != len(wantKeys) {
		t.Fatalf("item must have exactly %d keys, got %d: %v", len(wantKeys), len(items[0]), items[0])
	}
	for _, k := range wantKeys {
		if _, ok := items[0][k]; !ok {
			t.Fatalf("item missing key %q: %v", k, items[0])
		}
	}
	if items[0]["destination_type"] != "queue" || items[0]["vhost"] != "/" {
		t.Fatalf("item fields = %v", items[0])
	}

	// page present => envelope.
	env := e.do(http.MethodGet, "/api/bindings?page=1", nil, basicAuthHeader())
	page := decode[pageEnvelope](t, env)
	if page.TotalCount != 1 || page.Page != 1 {
		t.Fatalf("envelope = %+v", page)
	}

	// sort + projection.
	proj := e.do(http.MethodGet, "/api/bindings?page=1&sort=routing_key&columns=source,routing_key,properties_key", nil, basicAuthHeader())
	wantStatus(t, proj, http.StatusOK)
	raw := decode[map[string]any](t, proj)
	arr, _ := raw["items"].([]any)
	if len(arr) != 1 {
		t.Fatalf("projected items = %v", raw)
	}
	first, _ := arr[0].(map[string]any)
	if len(first) != 3 {
		t.Fatalf("projection must keep exactly 3 keys, got %v", first)
	}

	// name matches routing_key; source is an exact filter.
	f := e.do(http.MethodGet, "/api/bindings?name=rk&source=x", nil, basicAuthHeader())
	wantStatus(t, f, http.StatusOK)
	if got := decode[[]map[string]any](t, f); len(got) != 1 {
		t.Fatalf("name+source filter must match the one binding, got %v", got)
	}
	miss := e.do(http.MethodGet, "/api/bindings?source=nope", nil, basicAuthHeader())
	wantStatus(t, miss, http.StatusOK)
	if body := strings.TrimSpace(miss.Body.String()); body != "[]" {
		t.Fatalf("a non-matching filter must yield [] (FR-CONV-051), got %s", body)
	}
}

// TestBindings_QueueAndExchangeListings (FR-API-082/057).
func TestBindings_QueueAndExchangeListings(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareExchange(t, "y", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "x", "q", "rk", nil), http.StatusCreated)
	wantStatus(t, e.bindExchange(t, "x", "y", "erk", nil), http.StatusCreated)

	qb := e.do(http.MethodGet, "/api/queues/%2F/q/bindings", nil, basicAuthHeader())
	wantStatus(t, qb, http.StatusOK)
	qitems := decode[[]bindingItem](t, qb)
	if len(qitems) != 1 || qitems[0].DestinationType != "queue" || qitems[0].Destination != "q" {
		t.Fatalf("queue bindings = %+v", qitems)
	}

	src := e.do(http.MethodGet, "/api/exchanges/%2F/x/bindings/source", nil, basicAuthHeader())
	wantStatus(t, src, http.StatusOK)
	sitems := decode[[]bindingItem](t, src)
	if len(sitems) != 2 {
		t.Fatalf("source listing must show queue and e2e bindings, got %+v", sitems)
	}

	dst := e.do(http.MethodGet, "/api/exchanges/%2F/y/bindings/destination", nil, basicAuthHeader())
	wantStatus(t, dst, http.StatusOK)
	ditems := decode[[]bindingItem](t, dst)
	if len(ditems) != 1 || ditems[0].DestinationType != "exchange" || ditems[0].Source != "x" {
		t.Fatalf("destination listing = %+v", ditems)
	}

	// A missing queue/exchange is 404.
	if got := e.do(http.MethodGet, "/api/queues/%2F/nope/bindings", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("missing queue bindings must be 404, got %d", got.Code)
	}
	if got := e.do(http.MethodGet, "/api/exchanges/%2F/nope/bindings/source", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("missing exchange bindings must be 404, got %d", got.Code)
	}
	// /api/bindings/{vhost} rejects a non-%2F vhost.
	if got := e.do(http.MethodGet, "/api/bindings/other", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("a non-%%2F vhost must be 404, got %d", got.Code)
	}
}

// TestBindings_PostQueue_201Location (FR-API-091/094, spec §5.4 test 24).
func TestBindings_PostQueue_201Location(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})

	rec := e.bindQueue(t, "x", "q", "rk", map[string]any{"a": 1})
	wantStatus(t, rec, http.StatusCreated)
	if rec.Body.Len() != 0 {
		t.Fatalf("a successful bind must have an empty body, got %q", rec.Body.String())
	}
	loc := rec.Header().Get("Location")
	prefix := "/api/bindings/%2F/e/x/q/q/"
	if !strings.HasPrefix(loc, prefix) {
		t.Fatalf("Location = %q, want prefix %q", loc, prefix)
	}
	key := strings.TrimPrefix(loc, prefix)

	listed := decode[[]bindingItem](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	if len(listed) != 1 {
		t.Fatalf("listed = %+v", listed)
	}
	if listed[0].PropertiesKey != key {
		t.Fatalf("the Location key %q must round-trip to the listed properties_key %q", key, listed[0].PropertiesKey)
	}
	if !strings.HasPrefix(listed[0].PropertiesKey, "rk~") {
		t.Fatalf("non-empty args must yield rk~<hex>, got %q", listed[0].PropertiesKey)
	}

	// Missing exchange/queue are 404.
	missingX := e.do(http.MethodPost, "/api/bindings/%2F/e/nope/q/q", e.jsonBody(t, bindingCreateRequest{RoutingKey: "rk"}), basicAuthHeader())
	wantStatus(t, missingX, http.StatusNotFound)
	missingQ := e.do(http.MethodPost, "/api/bindings/%2F/e/x/q/nope", e.jsonBody(t, bindingCreateRequest{RoutingKey: "rk"}), basicAuthHeader())
	wantStatus(t, missingQ, http.StatusNotFound)
}

// TestBindings_PostExchange_Cycle400 (FR-API-092, spec §5.4 test 25).
func TestBindings_PostExchange_Cycle400(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "a", "direct")
	e.declareExchange(t, "b", "direct")

	wantStatus(t, e.bindExchange(t, "a", "b", "rk", nil), http.StatusCreated)
	cycle := e.bindExchange(t, "b", "a", "rk", nil)
	wantStatus(t, cycle, http.StatusBadRequest)
	if env := decode[errorEnvelope](t, cycle); env.Error != errBadRequest {
		t.Fatalf("cycle token = %q, want bad_request", env.Error)
	}

	self := e.bindExchange(t, "a", "a", "rk", nil)
	wantStatus(t, self, http.StatusBadRequest)
}

// TestBindings_DeleteByKey_RoundTrip (FR-API-093/DIV-12, spec §5.4 test 26).
func TestBindings_DeleteByKey_RoundTrip(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "x", "q", "rk", map[string]any{"a": 1}), http.StatusCreated)
	// A second binding on a different routing key with no arguments: a different
	// properties_key that must survive the delete untouched.
	wantStatus(t, e.bindQueue(t, "x", "q", "other", nil), http.StatusCreated)

	listed := decode[[]bindingItem](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	var target bindingItem
	for _, it := range listed {
		if it.RoutingKey == "rk" {
			target = it
		}
	}
	if target.PropertiesKey == "" {
		t.Fatalf("no rk binding listed: %+v", listed)
	}

	del := e.do(http.MethodDelete, "/api/bindings/%2F/e/x/q/q/"+url.PathEscape(target.PropertiesKey), nil, basicAuthHeader())
	wantStatus(t, del, http.StatusNoContent)

	after := decode[[]bindingItem](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	if len(after) != 1 || after[0].RoutingKey != "other" {
		t.Fatalf("only the matching binding must be removed, got %+v", after)
	}
}

// TestBindings_DeleteByKey_Absent404 (spec §5.4 test 27).
func TestBindings_DeleteByKey_Absent404(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "x", "q", "rk", nil), http.StatusCreated)

	got := e.do(http.MethodDelete, "/api/bindings/%2F/e/x/q/q/nope-key", nil, basicAuthHeader())
	wantStatus(t, got, http.StatusNotFound)
}

// TestBindings_DeleteExchangeByKey_RoundTrip.
func TestBindings_DeleteExchangeByKey_RoundTrip(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "a", "direct")
	e.declareExchange(t, "b", "direct")
	wantStatus(t, e.bindExchange(t, "a", "b", "erk", map[string]any{"z": 2}), http.StatusCreated)

	listed := decode[[]bindingItem](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	if len(listed) != 1 {
		t.Fatalf("listed = %+v", listed)
	}
	del := e.do(http.MethodDelete, "/api/bindings/%2F/e/a/e/b/"+url.PathEscape(listed[0].PropertiesKey), nil, basicAuthHeader())
	wantStatus(t, del, http.StatusNoContent)

	after := decode[[]bindingItem](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	if len(after) != 0 {
		t.Fatalf("the binding must be gone, got %+v", after)
	}
}

// TestBindings_Authz_QueueWriteExchangeRead (mgmt-01 §5.6, spec §5.4 test 28).
func TestBindings_Authz_QueueWriteExchangeRead(t *testing.T) {
	// write denied on the queue name.
	e1 := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":"^$","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")}
	wantStatus(t, e1.do(http.MethodPut, "/api/exchanges/%2F/x", e1.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), hdr), http.StatusCreated)
	wantStatus(t, e1.do(http.MethodPut, "/api/queues/%2F/q", e1.jsonBody(t, queueDeclareRequest{}), hdr), http.StatusCreated)
	rec := e1.do(http.MethodPost, "/api/bindings/%2F/e/x/q/q", e1.jsonBody(t, bindingCreateRequest{RoutingKey: "rk"}), hdr)
	wantStatus(t, rec, http.StatusForbidden)

	// read denied on the exchange.
	e2 := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":"^$"}}]}]}`, nil)
	wantStatus(t, e2.do(http.MethodPut, "/api/exchanges/%2F/x", e2.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), hdr), http.StatusCreated)
	wantStatus(t, e2.do(http.MethodPut, "/api/queues/%2F/q", e2.jsonBody(t, queueDeclareRequest{}), hdr), http.StatusCreated)
	rec = e2.do(http.MethodPost, "/api/bindings/%2F/e/x/q/q", e2.jsonBody(t, bindingCreateRequest{RoutingKey: "rk"}), hdr)
	wantStatus(t, rec, http.StatusForbidden)
}

// TestBindings_CapabilityFlips (FR-API-006, spec §5.4 test 29).
func TestBindings_CapabilityFlips(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "x", "q", "rk", nil), http.StatusCreated)

	caps := decode[versionResponse](t, e.do(http.MethodGet, "/api/strangeq/version", nil, basicAuthHeader()))
	if !caps.Capabilities.Bindings {
		t.Fatalf("with the seam present capabilities.bindings must be true")
	}
	wantStatus(t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()), http.StatusOK)

	// Hide the seam: embedding the UnifiedBroker INTERFACE does not promote the
	// ListBindings method, so the management layer must fall back to 501.
	e.srv.Broker = noTotalsBroker{UnifiedBroker: e.srv.Broker}
	caps = decode[versionResponse](t, e.do(http.MethodGet, "/api/strangeq/version", nil, basicAuthHeader()))
	if caps.Capabilities.Bindings {
		t.Fatalf("without the seam capabilities.bindings must be false")
	}
	missing := e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader())
	wantStatus(t, missing, http.StatusNotImplemented)
	if env := decode[errorEnvelope](t, missing); env.Error != errNotImplemented {
		t.Fatalf("token = %q, want not_implemented", env.Error)
	}
}

// TestOverview_BindingsCountCoherent (FR-API-002/DATA-040, spec §5.4 test 30).
func TestOverview_BindingsCountCoherent(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "x", "direct")
	e.declareQueue(t, "q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "x", "q", "a", nil), http.StatusCreated)
	wantStatus(t, e.bindQueue(t, "x", "q", "b", nil), http.StatusCreated)

	lister := e.srv.Broker.(interface {
		ListBindings() ([]broker.BindingView, error)
	})
	views, err := lister.ListBindings()
	if err != nil {
		t.Fatalf("ListBindings: %v", err)
	}

	read := func() int {
		rec := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
		wantStatus(t, rec, http.StatusOK)
		out := decode[map[string]any](t, rec)
		totals, _ := out["object_totals"].(map[string]any)
		n, _ := totals["bindings"].(float64)
		return int(n)
	}
	n := read()
	if n != len(views) {
		t.Fatalf("object_totals.bindings = %d, want len(ListBindings()) = %d", n, len(views))
	}

	// Two reads with no intervening topology change must report the same count.
	if n2 := read(); n != n2 {
		t.Fatalf("two overview reads diverged: %d vs %d", n, n2)
	}

	// A further binding is reflected by the delta counters.
	wantStatus(t, e.bindQueue(t, "x", "q", "c", nil), http.StatusCreated)
	if n3 := read(); n3 != len(views)+1 {
		t.Fatalf("object_totals.bindings = %d, want %d after a new binding", n3, len(views)+1)
	}
}

// --- BOB-3: channel fields ---

// TestChannels_PrefetchTransactionalNotNul (FR-API-030, spec §5.4 test 31).
func TestChannels_PrefetchTransactionalNotNul(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.injectConnection(t, "ch-conn", "u")
	e.srv.Mutex.Lock()
	conn := e.srv.Connections["ch-conn"]
	e.srv.Mutex.Unlock()
	ch := protocol.NewChannel(1, conn)
	ch.PrefetchCount.Store(5)
	ch.Transactional.Store(true)
	conn.Channels.Store(uint16(1), ch)

	rec := e.do(http.MethodGet, "/api/channels", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	items := decode[[]map[string]any](t, rec)
	if len(items) != 1 {
		t.Fatalf("channels = %v", items)
	}
	it := items[0]
	if it["prefetch_count"] != float64(5) {
		t.Fatalf("prefetch_count = %#v, want 5", it["prefetch_count"])
	}
	if it["transactional"] != true {
		t.Fatalf("transactional = %#v, want true", it["transactional"])
	}
	if it["state"] != "running" {
		t.Fatalf("state = %#v, want running", it["state"])
	}
	// A channel with no consumers has a KNOWN unacked set of 0, so stats_partial
	// is false (mgmt-03 §2.1).
	if it["unacked"] != float64(0) {
		t.Fatalf("unacked = %#v, want 0 for a consumerless channel", it["unacked"])
	}
	if it["stats_partial"] != false {
		t.Fatalf("stats_partial = %#v, want false when unacked is known", it["stats_partial"])
	}
	for _, k := range []string{"number", "connection", "user", "vhost", "state", "consumers", "unacked", "prefetch_count", "confirm", "transactional"} {
		if _, ok := it[k]; !ok {
			t.Fatalf("DATA-006 item missing %q: %v", k, it)
		}
	}
}

// TestChannels_ClosingStateMapping pins the DATA-006 state precedence: closing >
// flow > running (FR-API-030, spec §5.4 test 32).
func TestChannels_ClosingStateMapping(t *testing.T) {
	cases := []struct {
		view server.ChannelView
		want string
	}{
		{server.ChannelView{FlowActive: true}, "running"},
		{server.ChannelView{FlowActive: false}, "flow"},
		{server.ChannelView{FlowActive: true, Closing: true}, "closing"},
		{server.ChannelView{FlowActive: false, Closing: true}, "closing"},
	}
	for _, c := range cases {
		if got := channelState(c.view); got != c.want {
			t.Fatalf("channelState(%+v) = %q, want %q", c.view, got, c.want)
		}
	}
}

// TestConnectionChannels_Implemented (FR-API-024, spec §5.4 test 33).
func TestConnectionChannels_Implemented(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.injectConnection(t, "cc-conn", "u")
	e.srv.Mutex.Lock()
	conn := e.srv.Connections["cc-conn"]
	e.srv.Mutex.Unlock()
	ch := protocol.NewChannel(7, conn)
	ch.PrefetchCount.Store(2)
	conn.Channels.Store(uint16(7), ch)

	rec := e.do(http.MethodGet, "/api/connections/cc-conn/channels", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	items := decode[[]map[string]any](t, rec)
	if len(items) != 1 || items[0]["number"] != float64(7) {
		t.Fatalf("connection channels = %v", items)
	}

	if got := e.do(http.MethodGet, "/api/connections/nope/channels", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("absent connection channels must be 404, got %d", got.Code)
	}
}

// --- BOB-5: close by user ---

// TestCloseByUser_ClosesAll proves DELETE /api/connections/username/{user}
// closes every matching connection (spec §5.4 test 38). The injected
// connections carry no connRuntime, so they take the bounded force path; each
// peer observes EOF. The graceful 320 path for a live runtime is proven by
// TestCloseConnection_Graceful320 in the server package.
func TestCloseByUser_ClosesAll(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.AllowConnectionClose = true
	})
	p1 := e.injectConnection(t, "alice-1", "alice")
	p2 := e.injectConnection(t, "alice-2", "alice")
	bob := e.injectConnection(t, "bob-1", "bob")

	rec := e.do(http.MethodDelete, "/api/connections/username/alice", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusNoContent)

	mustSeeEOF(t, p1)
	mustSeeEOF(t, p2)
	// bob's connection must be untouched: it is still registered.
	e.srv.Mutex.RLock()
	_, bobPresent := e.srv.Connections["bob-1"]
	e.srv.Mutex.RUnlock()
	if !bobPresent {
		t.Fatalf("closing alice's connections must not touch bob's")
	}
	_ = bob
}
