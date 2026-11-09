package web

import (
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// versionFetches counts the GET /api/strangeq/version (capabilities) requests
// the stub has seen. A capability re-render must never re-fire this fetch, so a
// test can pin the count stable across refreshes/renders.
func versionFetches(stub *stubAPI) int {
	n := 0
	for _, p := range stub.pathsFor("strangeq") {
		if p == "/api/strangeq/version" {
			n++
		}
	}
	return n
}

// TestUI_CloseConnectionWaitsForCapability is the connections-gate sibling of
// TestUI_S10_BindingsCreateWaitsForCapability: closeConnectionState() returns
// "unknown" until state.capabilities lands, and #close-connection-btn is
// rendered disabled with the unknown-capability tooltip. The version probe
// fails on the first pass, so the detail screen paints while the capability is
// unknown; when a later probe finally delivers close_connection, the already
// rendered control must become enabled without a navigation.
func TestUI_CloseConnectionWaitsForCapability(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.versionFail = true }) // capabilities unknown on the first paint
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/connections/c1"),
		chromedp.WaitVisible(chromedp.CSS("#close-connection-btn")),
	)
	if !evalBool(t, ctx, `document.getElementById('close-connection-btn').disabled`) {
		t.Fatal("precondition: the Close control must render disabled while close_connection is unknown")
	}
	if title := evalStr(t, ctx, `document.getElementById('close-connection-btn').title`); !strings.Contains(strings.ToLower(title), "unknown") {
		t.Fatalf("precondition: the disabled control must carry the unknown-capability tooltip, got %q", title)
	}

	// The capability arrives on a later probe (delayed, so it lands after the
	// screen is already on screen). The rendered control must pick it up.
	stub.set(func(s *stubAPI) {
		s.versionFail = false
		s.capsDelay = 600 * time.Millisecond
	})
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){ window.SQ.refreshNow(); return 'ok'; })()`))
	mustDo(t, ctx, chromedp.WaitEnabled(chromedp.CSS("#close-connection-btn")))
}

// TestUI_CapabilityArrivalDoesNotRefetch pins the no-render-loop property: the
// re-render that delivers late capabilities calls render(), which must not
// re-fire loadVersion(). The bindings boot performs exactly one capability
// fetch; several refreshes and an explicit render must leave that count
// unchanged.
func TestUI_CapabilityArrivalDoesNotRefetch(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.caps["bindings"] = true
		s.capsDelay = 400 * time.Millisecond
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#create-binding-btn")),
		chromedp.WaitEnabled(chromedp.CSS("#create-binding-btn")),
	)

	base := versionFetches(stub)
	if base != 1 {
		t.Fatalf("bindings boot fetched capabilities %d times, want exactly 1 (a re-render must not re-fire loadVersion)", base)
	}
	for i := 0; i < 3; i++ {
		mustRun(t, ctx, refreshAwait(`(async function(){ await window.SQ.refreshNow(); return 'ok'; })()`))
	}
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){ window.SQ.render(); return 'ok'; })()`))
	if after := versionFetches(stub); after != base {
		t.Fatalf("capability fetch count grew from %d to %d across refreshes/renders (render loop)", base, after)
	}
}

// TestUI_S10_CreateBindingDialogSurvivesCapabilityRefresh proves the capability
// refresh that delivers changed capabilities does not close or clobber an open
// dialog. The dialog is opened with capabilities present, typed into, and then
// a CHANGED capability report is delivered while it is open: the dialog must
// stay open with its in-progress input intact, and still work.
func TestUI_S10_CreateBindingDialogSurvivesCapabilityRefresh(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/bindings"),
		chromedp.WaitVisible(chromedp.CSS("#create-binding-btn")),
		chromedp.WaitEnabled(chromedp.CSS("#create-binding-btn")),
		chromedp.Click(chromedp.CSS("#create-binding-btn")),
		chromedp.WaitVisible(chromedp.CSS("#binding-source")),
	)
	// In-progress operator input in the dialog.
	setFieldValue(t, ctx, "binding-source", "amq.topic")

	// A CHANGED capability report arrives late, while the dialog is open.
	stub.set(func(s *stubAPI) {
		s.caps["queue_message_totals"] = false // changed => the gates re-apply
		s.capsDelay = 300 * time.Millisecond
	})
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){ window.SQ.refreshCapabilities(); return 'ok'; })()`))

	// Wait for the response to land: the changed value is observable in state.
	if !waitFor(t, 10*time.Second, func() bool {
		v, err := evalStrOK(ctx, `String(window.SQ.state.capabilities.queue_message_totals === false)`)
		return err == nil && v == "true"
	}) {
		t.Fatal("the late capability report never reached the SPA")
	}
	// The dialog was neither closed nor clobbered, and the in-progress value
	// survived the capability refresh.
	if v := evalStr(t, ctx, `(function(){ var el = document.getElementById('binding-source'); return el ? el.value : '<missing>'; })()`); v != "amq.topic" {
		t.Fatalf("the open dialog was clobbered by the capability refresh (source=%q)", v)
	}
	// And it still works end to end: a valid destination lets Confirm create.
	setFieldValue(t, ctx, "binding-destination", "orders")
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))
	if !waitFor(t, 3*time.Second, func() bool { return len(stub.bindingCreatePaths()) > 0 }) {
		t.Fatal("the surviving dialog could not create a binding")
	}
}
