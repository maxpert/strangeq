package web

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// bindingRows is the aggregate binding set the S10 tests install: two queue
// destinations (one from amq.topic, one from a custom source exchange) and one
// exchange destination.
func bindingRows() []map[string]any {
	return []map[string]any{
		{"source": "amq.topic", "destination": "orders", "destination_type": "queue", "routing_key": "orders.#", "properties_key": "orders.#", "arguments": map[string]any{}},
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
	if !strings.Contains(cell, "orders.#") {
		t.Fatalf("properties_key cell = %q, want it to render the synthesised key", cell)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#create-binding-btn').length`); n != 1 {
		t.Fatalf("bindings screen has no create control (found %d)", n)
	}
}

// TestUI_S10_BindingsCreate proves the create dialog POSTs the canonical path
// with the routing key and arguments and carries the CSRF token.
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
	setFieldValue(t, ctx, "binding-dest-type", "q")
	setFieldValue(t, ctx, "binding-destination", "orders2")
	setFieldValue(t, ctx, "binding-routing-key", "orders2.#")
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
	if csrf := stub.csrfSeen(); csrf != "csrf-token-abc" {
		t.Fatalf("binding create did not carry the CSRF token, got %q", csrf)
	}
}

// TestUI_S10_BindingsDelete proves the per-row delete is a confirmed
// destructive action: Confirm stays disabled until the acknowledgement
// checkbox, and the DELETE addresses the properties_key.
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
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var b = document.querySelector('#content tbody tr td[data-col="actions"] button');
	  if (!b) return 'missing';
	  b.click();
	  return 'ok';
	})()`))
	mustDo(t, ctx, chromedp.WaitVisible(chromedp.CSS("#binding-delete-ack")))
	if !evalBool(t, ctx, `document.getElementById('modal-confirm').disabled`) {
		t.Fatal("binding delete Confirm must be disabled before the acknowledgement")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#binding-delete-ack")))
	if evalBool(t, ctx, `document.getElementById('modal-confirm').disabled`) {
		t.Fatal("binding delete Confirm must enable once acknowledged")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))

	if !waitFor(t, 3*time.Second, func() bool { return len(stub.bindingDeletePaths()) > 0 }) {
		t.Fatal("delete binding never reached the API")
	}
	got, _ := url.PathUnescape(stub.bindingDeletePaths()[0])
	if !strings.Contains(got, "/e/amq.topic/q/orders/orders.#") {
		t.Fatalf("binding delete path = %q, want it to address the properties_key", got)
	}
	if csrf := stub.csrfSeen(); csrf != "csrf-token-abc" {
		t.Fatalf("binding delete did not carry the CSRF token, got %q", csrf)
	}
}

// TestUI_S10_QueueBindingsSection proves the queue detail screen lists the
// queue's bindings from GET /api/queues/{vhost}/{name}/bindings instead of the
// pre-BOB-4 "not available" notice.
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
	txt := evalStr(t, ctx, `document.getElementById('queue-bindings').textContent`)
	if !strings.Contains(txt, "amq.topic") {
		t.Fatalf("queue bindings table = %q, want the source exchange", txt)
	}
	if strings.Contains(txt, "not available") || strings.Contains(txt, "not implemented") {
		t.Fatalf("queue bindings still render the stale unsupported notice: %q", txt)
	}
}

// TestUI_S10_ExchangeBindingsSection proves the exchange detail screen renders
// both the source and destination binding lists.
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
// capabilities.bindings false the screen renders a notice and never an enabled
// create control or a data table.
func TestUI_S10_BindingsCapabilityOff(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = false })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/bindings"), chromedp.WaitVisible(chromedp.CSS("#content .notice")))
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
