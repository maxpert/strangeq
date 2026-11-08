package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// bindingPropsKey is the first fixture row's synthesised properties_key: the
// routing key plus a '~' and a 64-hex argument digest, exactly the shape the
// broker's propertiesKey() produces. It is deliberately DIFFERENT from the
// routing key so a delete that sends routing_key instead of properties_key is
// caught.
func bindingPropsKey() string { return "orders.#~" + strings.Repeat("ab", 32) }

// bindingRows is the aggregate binding set the S10 tests install: two queue
// destinations (one from amq.topic, one from a custom source exchange) and one
// exchange destination. The first row's properties_key is distinct from its
// routing key.
func bindingRows() []map[string]any {
	return []map[string]any{
		{"source": "amq.topic", "destination": "orders", "destination_type": "queue", "routing_key": "orders.#", "properties_key": bindingPropsKey(), "arguments": map[string]any{}},
		{"source": "custom", "destination": "orders", "destination_type": "queue", "routing_key": "rk-1", "properties_key": "rk-1", "arguments": map[string]any{}},
		{"source": "amq.topic", "destination": "custom", "destination_type": "exchange", "routing_key": "rk-2", "properties_key": "rk-2", "arguments": map[string]any{}},
	}
}

// closeChannelItem is one fully-populated channel row (BOB-3): a real
// prefetch_count, a transactional flag and state=closing.
func closingChannelItem() map[string]any {
	return map[string]any{
		"number": 1, "connection": "c1", "user": "guest", "vhost": "/", "state": "closing",
		"consumers": 0, "unacked": 0, "prefetch_count": 25, "confirm": false,
		"transactional": true, "stats_partial": false,
	}
}

// setFieldValue sets an input/select value and dispatches the events the modal
// guard listens for (input for text, change for selects).
func setFieldValue(t *testing.T, ctx context.Context, id, value string) {
	t.Helper()
	mustRun(t, ctx, chromedp.Evaluate[string](fmt.Sprintf(`(function(){
	  var el = document.getElementById(%q);
	  if (!el) return 'missing';
	  el.value = %q;
	  el.dispatchEvent(new Event('input', {bubbles:true}));
	  el.dispatchEvent(new Event('change', {bubbles:true}));
	  return 'ok';
	})()`, id, value)))
}

// clickBindingRowDelete clicks the Delete action of the first binding row for
// which matchJS (evaluated with a per-row `cells[data-col] = textContent` map)
// is true. It fails loudly when the row or its control is missing.
func clickBindingRowDelete(t *testing.T, ctx context.Context, matchJS string) {
	t.Helper()
	res := mustRun(t, ctx, chromedp.Evaluate[string](fmt.Sprintf(`(function(){
	  var rows = document.querySelectorAll('#content tbody tr');
	  for (var i = 0; i < rows.length; i++) {
	    var r = rows[i];
	    var cells = {};
	    Array.prototype.forEach.call(r.querySelectorAll('td'), function(td){ cells[td.dataset.col] = td.textContent; });
	    if (%s) {
	      var b = r.querySelector('td[data-col="actions"] button');
	      if (!b) return 'no-button';
	      if (b.disabled) return 'disabled';
	      b.click();
	      return 'clicked';
	    }
	  }
	  return 'no-row';
	})()`, matchJS)))
	if res != "clicked" {
		t.Fatalf("could not click the binding row (%s): %s", matchJS, res)
	}
}

// confirmBindingDelete waits for the delete modal, acknowledges it and confirms.
func confirmBindingDelete(t *testing.T, ctx context.Context) {
	t.Helper()
	mustDo(t, ctx, chromedp.WaitVisible(chromedp.CSS("#binding-delete-ack")))
	if !evalBool(t, ctx, `document.getElementById('modal-confirm').disabled`) {
		t.Fatal("binding delete Confirm must be disabled before the acknowledgement")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#binding-delete-ack")))
	if evalBool(t, ctx, `document.getElementById('modal-confirm').disabled`) {
		t.Fatal("binding delete Confirm must enable once acknowledged")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))
}

// TestUI_S10_BindingsList proves the bindings screen renders the full DATA-012
// item (source/destination/destination_type/routing_key/properties_key) and no
// "not implemented" notice when capabilities.bindings is true.
func TestUI_S10_BindingsList(t *testing.T) {
	js := string(readAsset(t, "app.js"))
	for _, want := range []string{"properties_key", "destination_type", "create-binding-btn"} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js (S10) is missing %q", want)
		}
	}

	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = true; s.bindingItems = bindingRows() })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#content .data-table")),
	)

	if n := evalInt(t, ctx, `document.querySelectorAll('#content .notice').length`); n != 0 {
		t.Fatalf("bindings are implemented but the screen rendered %d notice block(s)", n)
	}
	headers := evalStr(t, ctx, `Array.prototype.map.call(document.querySelectorAll('#content thead th'), function(th){return th.textContent;}).join('|')`)
	for _, h := range []string{"Source", "Destination", "Type", "Routing key", "Properties key"} {
		if !strings.Contains(headers, h) {
			t.Errorf("bindings table lacks the %q column (headers=%q)", h, headers)
		}
	}
	cell := evalStr(t, ctx, `document.querySelector('#content tbody tr td[data-col="properties_key"]').textContent`)
	if cell != bindingPropsKey() {
		t.Fatalf("properties_key cell = %q, want the synthesised key %q", cell, bindingPropsKey())
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#create-binding-btn').length`); n != 1 {
		t.Fatalf("bindings screen has no create control (found %d)", n)
	}
}

// TestUI_S10_BindingsCreate proves the create dialog POSTs the canonical path
// with the routing key AND the x-* arguments, and carries the CSRF token.
func TestUI_S10_BindingsCreate(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.caps["bindings"] = true
		s.bindingItems = bindingRows()
		s.requireLogin = true
		s.wantCSRF = true
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()
	login(t, ctx, srv.URL)

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#create-binding-btn")),
		chromedp.Click(chromedp.CSS("#create-binding-btn")),
		chromedp.WaitVisible(chromedp.CSS("#binding-source")),
	)
	setFieldValue(t, ctx, "binding-source", "amq.topic")
	setFieldValue(t, ctx, "binding-dest-type", "queue")
	setFieldValue(t, ctx, "binding-destination", "orders2")
	setFieldValue(t, ctx, "binding-routing-key", "orders2.#")
	setFieldValue(t, ctx, "binding-arguments", "x-match=all")
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))

	if !waitFor(t, 3*time.Second, func() bool { return len(stub.bindingCreatePaths()) > 0 }) {
		t.Fatal("create binding never reached the API")
	}
	got := stub.bindingCreatePaths()[0]
	want := "/api/bindings/%2F/e/amq.topic/q/orders2"
	if got != want {
		t.Fatalf("binding create path = %q, want %q", got, want)
	}
	body := stub.bindingCreateBody()
	if body == nil || body["routing_key"] != "orders2.#" {
		t.Fatalf("binding create body = %v, want routing_key orders2.#", body)
	}
	if args, ok := body["arguments"].(map[string]any); !ok || !reflect.DeepEqual(args, map[string]any{"x-match": "all"}) {
		t.Fatalf("binding create arguments = %v, want {\"x-match\":\"all\"}", body["arguments"])
	}
	if csrf := stub.csrfSeen(); csrf != "csrf-token-abc" {
		t.Fatalf("binding create did not carry the CSRF token, got %q", csrf)
	}
}

// TestUI_S10_BindingsCreateExchangeDestination proves a create with the
// exchange destination type POSTs the "e" path letter, not "q".
func TestUI_S10_BindingsCreateExchangeDestination(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.caps["bindings"] = true
		s.bindingItems = bindingRows()
		s.requireLogin = true
		s.wantCSRF = true
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()
	login(t, ctx, srv.URL)

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#create-binding-btn")),
		chromedp.Click(chromedp.CSS("#create-binding-btn")),
		chromedp.WaitVisible(chromedp.CSS("#binding-source")),
	)
	setFieldValue(t, ctx, "binding-source", "amq.topic")
	setFieldValue(t, ctx, "binding-dest-type", "exchange")
	setFieldValue(t, ctx, "binding-destination", "custom")
	setFieldValue(t, ctx, "binding-routing-key", "rk-3")
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))

	if !waitFor(t, 3*time.Second, func() bool { return len(stub.bindingCreatePaths()) > 0 }) {
		t.Fatal("create binding (exchange destination) never reached the API")
	}
	got := stub.bindingCreatePaths()[0]
	want := "/api/bindings/%2F/e/amq.topic/e/custom"
	if got != want {
		t.Fatalf("binding create (exchange destination) path = %q, want %q", got, want)
	}
}

// TestUI_S10_BindingsDelete proves the per-row delete is a confirmed
// destructive action and the DELETE addresses the EXACT synthesised
// properties_key (not the routing key).
func TestUI_S10_BindingsDelete(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.caps["bindings"] = true
		s.bindingItems = bindingRows()
		s.requireLogin = true
		s.wantCSRF = true
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()
	login(t, ctx, srv.URL)

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#content tbody tr")),
	)
	clickBindingRowDelete(t, ctx, fmt.Sprintf("cells['properties_key'] === %q", bindingPropsKey()))
	confirmBindingDelete(t, ctx)

	if !waitFor(t, 3*time.Second, func() bool { return len(stub.bindingDeletePaths()) > 0 }) {
		t.Fatal("delete binding never reached the API")
	}
	raw := stub.bindingDeletePaths()[0]
	wantRaw := "/api/bindings/%2F/e/amq.topic/q/orders/" + url.PathEscape(bindingPropsKey())
	if raw != wantRaw {
		t.Fatalf("binding delete raw path = %q, want %q", raw, wantRaw)
	}
	// Decoded, the vhost segment %2F becomes a literal "/" between the two
	// path separators, so the decoded path carries "///" before e/.
	got, _ := url.PathUnescape(raw)
	want := "/api/bindings///e/amq.topic/q/orders/" + bindingPropsKey()
	if got != want {
		t.Fatalf("binding delete decoded path = %q, want the exact properties_key path %q", got, want)
	}
	if csrf := stub.csrfSeen(); csrf != "csrf-token-abc" {
		t.Fatalf("binding delete did not carry the CSRF token, got %q", csrf)
	}
}

// TestUI_S10_BindingsDeleteExchangeDestination proves a delete of an
// exchange-destination binding uses the "e" path letter, not "q".
func TestUI_S10_BindingsDeleteExchangeDestination(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.caps["bindings"] = true
		s.bindingItems = bindingRows()
		s.requireLogin = true
		s.wantCSRF = true
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()
	login(t, ctx, srv.URL)

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#content tbody tr")),
	)
	clickBindingRowDelete(t, ctx, `cells['destination_type'] === "exchange" && cells['destination'] === "custom"`)
	confirmBindingDelete(t, ctx)

	if !waitFor(t, 3*time.Second, func() bool { return len(stub.bindingDeletePaths()) > 0 }) {
		t.Fatal("delete binding (exchange destination) never reached the API")
	}
	raw := stub.bindingDeletePaths()[0]
	wantRaw := "/api/bindings/%2F/e/amq.topic/e/custom/rk-2"
	if raw != wantRaw {
		t.Fatalf("binding delete (exchange destination) raw path = %q, want %q", raw, wantRaw)
	}
	got, _ := url.PathUnescape(raw)
	want := "/api/bindings///e/amq.topic/e/custom/rk-2"
	if got != want {
		t.Fatalf("binding delete (exchange destination) decoded path = %q, want %q", got, want)
	}
}

// TestUI_S10_QueueBindingsSection proves the queue detail screen lists the
// queue's bindings from the PER-QUEUE endpoint with correct filtering, not the
// aggregate list.
func TestUI_S10_QueueBindingsSection(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = true; s.bindingItems = bindingRows() })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues/orders"))
	if !waitFor(t, 3*time.Second, func() bool {
		return evalInt(t, ctx, `document.querySelectorAll('#queue-bindings .data-table').length`) == 1
	}) {
		t.Fatal("queue detail did not render a bindings table")
	}
	if !stub.pathSeen("queues", "/api/queues/%2F/orders/bindings") {
		t.Fatalf("queue panel did not call the per-queue endpoint; queues paths = %v", stub.pathsFor("queues"))
	}
	txt := evalStr(t, ctx, `document.getElementById('queue-bindings').textContent`)
	if !strings.Contains(txt, "amq.topic") {
		t.Fatalf("queue bindings table = %q, want the source exchange", txt)
	}
	if strings.Contains(txt, "not available") || strings.Contains(txt, "not implemented") {
		t.Fatalf("queue bindings still render the stale unsupported notice: %q", txt)
	}
	// Filtering: only the two queue-destination bindings, never the
	// exchange-destination row (destination "custom", type "exchange").
	if n := evalInt(t, ctx, `document.querySelectorAll('#queue-bindings tbody tr').length`); n != 2 {
		t.Fatalf("queue bindings rendered %d rows, want 2 (the exchange-destination row leaked)", n)
	}
	types := evalStr(t, ctx, `Array.prototype.map.call(document.querySelectorAll('#queue-bindings td[data-col="destination_type"]'), function(td){return td.textContent;}).join('|')`)
	if types != "queue|queue" {
		t.Fatalf("queue bindings destination types = %q, want queue|queue", types)
	}
	dests := evalStr(t, ctx, `Array.prototype.map.call(document.querySelectorAll('#queue-bindings td[data-col="destination"]'), function(td){return td.textContent;}).join('|')`)
	if dests != "orders|orders" {
		t.Fatalf("queue bindings destinations = %q, want orders|orders", dests)
	}
}

// TestUI_S10_ExchangeBindingsSection proves the exchange detail screen renders
// both directions from the PER-EXCHANGE endpoints.
func TestUI_S10_ExchangeBindingsSection(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = true; s.bindingItems = bindingRows() })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/exchanges/custom"))
	for _, id := range []string{"exchange-bindings-source", "exchange-bindings-destination"} {
		if !waitFor(t, 3*time.Second, func() bool {
			return evalInt(t, ctx, fmt.Sprintf(`document.querySelectorAll('#%s .data-table').length`, id)) == 1
		}) {
			t.Fatalf("%s did not render a bindings table", id)
		}
	}
	for _, p := range []string{"/api/exchanges/%2F/custom/bindings/source", "/api/exchanges/%2F/custom/bindings/destination"} {
		if !stub.pathSeen("exchanges", p) {
			t.Fatalf("exchange panel did not call %s; exchanges paths = %v", p, stub.pathsFor("exchanges"))
		}
	}
	// Each direction is filtered: one row each, never all three.
	if n := evalInt(t, ctx, `document.querySelectorAll('#exchange-bindings-source tbody tr').length`); n != 1 {
		t.Fatalf("exchange source bindings rendered %d rows, want 1", n)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#exchange-bindings-destination tbody tr').length`); n != 1 {
		t.Fatalf("exchange destination bindings rendered %d rows, want 1", n)
	}
	src := evalStr(t, ctx, `document.getElementById('exchange-bindings-source').textContent`)
	if !strings.Contains(src, "orders") {
		t.Fatalf("exchange source bindings = %q, want the queue destination", src)
	}
	dst := evalStr(t, ctx, `document.getElementById('exchange-bindings-destination').textContent`)
	if !strings.Contains(dst, "amq.topic") {
		t.Fatalf("exchange destination bindings = %q, want the source exchange", dst)
	}
}

// TestUI_S10_BindingsCapabilityOff proves capability gating: with
// capabilities.bindings false the screen renders a notice carrying the NEW
// reason (not the stale BOB-4 text) and never an enabled create control or a
// data table.
func TestUI_S10_BindingsCapabilityOff(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = false })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/bindings"), chromedp.WaitVisible(chromedp.CSS("#content .notice")))
	txt := evalStr(t, ctx, `document.getElementById('content').textContent`)
	if !strings.Contains(txt, "binding management is not available in this deployment") {
		t.Fatalf("bindings notice = %q, want the current capability reason", txt)
	}
	if strings.Contains(txt, "BOB-4") {
		t.Fatalf("bindings notice still carries the stale BOB-4 text: %q", txt)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#create-binding-btn').length`); n != 0 {
		t.Fatalf("unsupported bindings rendered a create control (%d)", n)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#content .data-table').length`); n != 0 {
		t.Fatalf("unsupported bindings rendered a data table (%d)", n)
	}
}

// TestUI_ChannelDetail_Fields proves the channel detail screen (reached by the
// composite id) renders the real prefetch_count, transactional and the
// closing state, and that the channels list links to it.
func TestUI_ChannelDetail_Fields(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.channelItems = []map[string]any{closingChannelItem()} })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/channels/c1:1"),
		chromedp.WaitVisible(chromedp.CSS("#content .props")),
	)
	txt := evalStr(t, ctx, `document.getElementById('content').textContent`)
	for _, want := range []string{"closing", "25", "true"} {
		if !strings.Contains(txt, want) {
			t.Errorf("channel detail = %q, want it to include %q", txt, want)
		}
	}

	// The list links each channel number to its composite id.
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/channels"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
	if n := evalInt(t, ctx, `document.querySelectorAll('#content tbody a[href="/channels/c1%3A1"]').length`); n != 1 {
		t.Fatalf("the channels list must link the channel number to /channels/c1%%3A1 (found %d)", n)
	}
}

// TestUI_ChannelDetail_StatsPartialNull proves the null discipline: an absent
// stats_partial renders an em-dash, never a fabricated "complete".
func TestUI_ChannelDetail_StatsPartialNull(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.channelItems = []map[string]any{{
			"number": 1, "connection": "c1", "user": "guest", "vhost": "/", "state": "running",
			"consumers": 0, "unacked": 0, "prefetch_count": 0, "confirm": false, "transactional": false,
			// stats_partial deliberately absent (unknown).
		}}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/channels/c1:1"),
		chromedp.WaitVisible(chromedp.CSS("#content .props")),
	)
	stats := evalStr(t, ctx, `(function(){
	  var dts = document.querySelectorAll('#content .props dt');
	  for (var i = 0; i < dts.length; i++) {
	    if (dts[i].textContent === 'stats') return dts[i].nextElementSibling.textContent;
	  }
	  return '<no-stats-row>';
	})()`)
	if strings.TrimSpace(stats) != "\u2014" {
		t.Fatalf("absent stats_partial rendered %q, want an em-dash", stats)
	}
}

// TestUI_ConnectionChannels_Fields proves the connection detail channel panel
// shows prefetch_count/transactional (not fabricated) once the endpoint is
// populated, and links each channel.
func TestUI_ConnectionChannels_Fields(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.channelItems = []map[string]any{closingChannelItem()} })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections/c1"))
	if !waitFor(t, 3*time.Second, func() bool {
		return evalInt(t, ctx, `document.querySelectorAll('#connection-channels .data-table').length`) == 1
	}) {
		t.Fatal("connection detail did not render the channels table")
	}
	headers := evalStr(t, ctx, `Array.prototype.map.call(document.querySelectorAll('#connection-channels thead th'), function(th){return th.textContent;}).join('|')`)
	for _, h := range []string{"Prefetch", "Transactional"} {
		if !strings.Contains(headers, h) {
			t.Errorf("connection channels table lacks %q (headers=%q)", h, headers)
		}
	}
	prefetch := evalStr(t, ctx, `document.querySelector('#connection-channels tbody tr td[data-col="prefetch_count"]').textContent`)
	if strings.TrimSpace(prefetch) != "25" {
		t.Fatalf("channel prefetch_count cell = %q, want 25", prefetch)
	}
	state := evalStr(t, ctx, `document.querySelector('#connection-channels tbody tr td[data-col="state"]').textContent`)
	if strings.TrimSpace(state) != "closing" {
		t.Fatalf("channel state cell = %q, want closing", state)
	}
}

// TestUI_CloseConnection_Reason proves the S2 close dialog carries the operator
// reason the API accepts (the X-Reason header) and still requires the CSRF
// token.
func TestUI_CloseConnection_Reason(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.requireLogin = true; s.wantCSRF = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()
	login(t, ctx, srv.URL)

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/connections/c1"),
		chromedp.WaitVisible(chromedp.CSS("#close-connection-btn")),
		chromedp.Click(chromedp.CSS("#close-connection-btn")),
		chromedp.WaitVisible(chromedp.CSS("#close-reason")),
	)
	setFieldValue(t, ctx, "close-reason", "operator maintenance")
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))
	if !waitFor(t, 3*time.Second, func() bool { return stub.closeReason() != "" }) {
		t.Fatal("the close reason never reached the API")
	}
	if got := stub.closeReason(); got != "operator maintenance" {
		t.Fatalf("close reason = %q, want the operator reason", got)
	}
	if csrf := stub.csrfSeen(); csrf != "csrf-token-abc" {
		t.Fatalf("connection close did not carry the CSRF token, got %q", csrf)
	}
}

// TestUI_S10_HostileValuesInert proves every new surface renders untrusted
// values as inert text: a hostile binding, channel and form value must never
// create an <img> or <script> element or execute.
func TestUI_S10_HostileValuesInert(t *testing.T) {
	const imgHostile = `<img src=x onerror="window.__pwn=1">`
	const scriptHostile = `"><script>window.__pwn=2</script>`

	assertInert := func(t *testing.T, ctx context.Context, scope string) {
		t.Helper()
		if n := evalInt(t, ctx, fmt.Sprintf(`document.querySelectorAll(%q).length`, scope+" img, "+scope+" script")); n != 0 {
			t.Fatalf("%s injected %d img/script element(s)", scope, n)
		}
		if pwn := evalStr(t, ctx, `String(window.__pwn)`); pwn != "undefined" {
			t.Fatalf("a hostile value executed: window.__pwn = %s", pwn)
		}
	}

	t.Run("bindings-list", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) {
			s.caps["bindings"] = true
			s.bindingItems = []map[string]any{
				{"source": imgHostile, "destination": scriptHostile, "destination_type": "queue", "routing_key": imgHostile, "properties_key": scriptHostile, "arguments": map[string]any{}},
			}
		})
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/bindings"), chromedp.WaitVisible(chromedp.CSS("#content .data-table")))
		assertInert(t, ctx, "#content")
		cell := evalStr(t, ctx, `document.querySelector('#content tbody tr td[data-col="source"]').textContent`)
		if !strings.Contains(cell, "<img") {
			t.Fatalf("hostile source did not render as inert text: %q", cell)
		}
	})

	t.Run("create-dialog", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) { s.caps["bindings"] = true; s.bindingItems = bindingRows() })
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx,
			chromedp.Navigate(srv.URL+"/bindings"),
			chromedp.WaitVisible(chromedp.CSS("#create-binding-btn")),
			chromedp.Click(chromedp.CSS("#create-binding-btn")),
			chromedp.WaitVisible(chromedp.CSS("#binding-source")),
		)
		setFieldValue(t, ctx, "binding-source", imgHostile)
		setFieldValue(t, ctx, "binding-destination", scriptHostile)
		setFieldValue(t, ctx, "binding-routing-key", imgHostile)
		assertInert(t, ctx, "#modal-root")
		if v := evalStr(t, ctx, `document.getElementById('binding-destination').value`); v != scriptHostile {
			t.Fatalf("hostile destination did not round-trip inert: %q", v)
		}
	})

	t.Run("close-reason", func(t *testing.T) {
		stub := newStub()
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx,
			chromedp.Navigate(srv.URL+"/connections/c1"),
			chromedp.WaitVisible(chromedp.CSS("#close-connection-btn")),
			chromedp.Click(chromedp.CSS("#close-connection-btn")),
			chromedp.WaitVisible(chromedp.CSS("#close-reason")),
		)
		setFieldValue(t, ctx, "close-reason", scriptHostile+imgHostile)
		assertInert(t, ctx, "#modal-root")
	})

	t.Run("channel-detail", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) {
			// connection stays "c1" so the composite id routes; the hostile
			// values ride the other rendered fields.
			s.channelItems = []map[string]any{{
				"number": 1, "connection": "c1", "user": scriptHostile, "vhost": imgHostile, "state": imgHostile,
				"consumers": 0, "unacked": 0, "prefetch_count": 0, "confirm": false, "transactional": false, "stats_partial": false,
			}}
		})
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/channels/c1:1"), chromedp.WaitVisible(chromedp.CSS("#content .props")))
		assertInert(t, ctx, "#content")
		txt := evalStr(t, ctx, `document.getElementById('content').textContent`)
		if !strings.Contains(txt, "<img") {
			t.Fatalf("hostile channel value did not render as inert text: %q", txt)
		}
	})
}

// TestUI_CSRF_CallerHeaderCannotOverride proves a caller-supplied CSRF header
// (in any case) can never survive: it is dropped, the canonical token is set
// from the session, and with no session token no CSRF header is sent at all.
func TestUI_CSRF_CallerHeaderCannotOverride(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#nav")))

	raw := mustRun(t, ctx, chromedp.Evaluate[string](`(async function(){
	  var real = window.fetch;
	  var cap = [];
	  window.fetch = function(u, o){
	    cap.push({ url: u, headers: o.headers });
	    return Promise.resolve({ status: 200, ok: true,
	      headers: { get: function(){ return null; } },
	      text: function(){ return Promise.resolve('{}'); } });
	  };
	  try {
	    window.SQ.state.csrf = null;
	    await window.SQ.api('/api/probe1', { method: 'POST', headers: { 'x-csrf-token': 'evil', 'X-CSRF-Token': 'evil2', 'X-Reason': 'r' } });
	    window.SQ.state.csrf = 'good-token';
	    await window.SQ.api('/api/probe2', { method: 'POST', headers: { 'x-csrf-token': 'evil3' } });
	  } finally { window.fetch = real; }
	  return JSON.stringify(cap);
	})()`, chromedp.EvalAwaitPromise))

	var probes []struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal([]byte(raw), &probes); err != nil {
		t.Fatalf("capture not JSON: %v (%s)", err, raw)
	}
	if len(probes) != 2 {
		t.Fatalf("want 2 captured probes, got %d (%s)", len(probes), raw)
	}

	csrfKeys := func(h map[string]string) []string {
		var out []string
		for k := range h {
			if strings.EqualFold(k, "x-csrf-token") {
				out = append(out, k)
			}
		}
		return out
	}

	// Probe 1: no session token — the caller's CSRF headers must all be gone.
	if keys := csrfKeys(probes[0].Headers); len(keys) != 0 {
		t.Fatalf("caller CSRF header survived with no session token: %v", keys)
	}
	if probes[0].Headers["X-Reason"] != "r" {
		t.Fatalf("the caller's non-CSRF header was dropped: %v", probes[0].Headers)
	}
	// Probe 2: a session token — exactly one CSRF header, carrying the token.
	if keys := csrfKeys(probes[1].Headers); len(keys) != 1 || !strings.EqualFold(keys[0], "X-CSRF-Token") {
		t.Fatalf("want exactly one canonical CSRF header, got %v", keys)
	}
	if v := probes[1].Headers["X-CSRF-Token"]; v != "good-token" {
		t.Fatalf("canonical CSRF header = %q, want the session token", v)
	}
}

// TestUI_About_BindingsRowsCurrent proves the About capability explanation and
// divergence table no longer describe bindings as unimplemented (BOB-4).
func TestUI_About_BindingsRowsCurrent(t *testing.T) {
	js := string(readAsset(t, "app.js"))
	if strings.Contains(js, "aggregate binding listing (BOB-4)") {
		t.Error("app.js still describes bindings as an unimplemented BOB-4 feature")
	}
	if !strings.Contains(js, "DIV-bindings") {
		t.Error("app.js divergence table lacks a DIV-bindings row")
	}
}
