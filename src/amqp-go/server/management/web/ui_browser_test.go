package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

/* ------------------------------------------------------------------ setup -- */

var (
	browserCtx    context.Context
	browserCancel context.CancelFunc
	browserErr    error
)

func chromeExecPath() string {
	if p := os.Getenv("CHROME_BIN"); p != "" {
		return p
	}
	candidates := []string{
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"/Applications/Chromium.app/Contents/MacOS/Chromium",
		"/usr/bin/google-chrome",
		"/usr/bin/google-chrome-stable",
		"/usr/bin/chromium",
		"/usr/bin/chromium-browser",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

// TestMain starts one headless browser for the whole package. A missing or
// unstartable browser is not a failure: every UI test skips with a clear
// message (CI installs Chrome and runs them).
func TestMain(m *testing.M) {
	if os.Getenv("SQ_NO_BROWSER") == "" {
		opts := slices.Clone(chromedp.DefaultExecAllocatorOptions[:])
		opts = append(opts, chromedp.NoSandbox, chromedp.DisableGPU)
		if p := chromeExecPath(); p != "" {
			opts = append(opts, chromedp.ExecPath(p))
		}
		allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
		startCtx, cancelStart := chromedp.NewContext(allocCtx)
		if err := chromedp.Do(startCtx); err != nil {
			browserErr = err
		} else {
			browserCtx = startCtx
			browserCancel = func() {
				cancelStart()
				cancelAlloc()
			}
		}
	} else {
		browserErr = context.Canceled
	}
	code := m.Run()
	if browserCancel != nil {
		browserCancel()
	}
	os.Exit(code)
}

// newTab opens a fresh tab that shares the package browser. Every test gets an
// explicit 30 s deadline so no test can hang.
func newTab(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	if browserErr != nil {
		t.Skipf("headless browser unavailable: %v", browserErr)
	}
	tabCtx, cancelTab := chromedp.NewContext(browserCtx)
	if err := chromedp.Do(tabCtx); err != nil {
		cancelTab()
		t.Skipf("headless browser tab unavailable: %v", err)
	}
	ctx, cancelTimeout := context.WithTimeout(tabCtx, 30*time.Second)
	return ctx, func() { cancelTimeout(); cancelTab() }
}

// appServer composes the SPA asset handler with the stub API on one origin,
// exactly as the production management mux composes web.Handler with the API.
func appServer(t *testing.T, s *stubAPI) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/api/", s)
	mux.Handle("/", Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func mustDo(t *testing.T, ctx context.Context, steps ...chromedp.Action[chromedp.Void]) {
	t.Helper()
	if err := chromedp.Do(ctx, steps...); err != nil {
		t.Fatalf("chromedp: %v", err)
	}
}

func mustRun[T any](t *testing.T, ctx context.Context, a chromedp.Action[T]) T {
	t.Helper()
	v, err := chromedp.Run(ctx, a)
	if err != nil {
		t.Fatalf("chromedp: %v", err)
	}
	return v
}

// refreshAwait runs SQ.refreshNow() and returns the current scheduled delay.
func refreshAwait(expr string) chromedp.Action[string] {
	return chromedp.Evaluate[string](expr, chromedp.EvalAwaitPromise)
}

// login drives the real login form (FR-UI-002).
func login(t *testing.T, ctx context.Context, url string) {
	t.Helper()
	mustDo(t, ctx,
		chromedp.Navigate(url+"/"),
		chromedp.WaitVisible(chromedp.CSS("#login-form")),
		chromedp.Click(chromedp.CSS("#login-username")),
		chromedp.SendKeys(chromedp.CSS("#login-username"), "guest"),
		chromedp.Click(chromedp.CSS("#login-password")),
		chromedp.SendKeys(chromedp.CSS("#login-password"), "s3cret"),
		chromedp.Click(chromedp.CSS("#login-submit")),
		chromedp.WaitVisible(chromedp.CSS("#nav")),
	)
}

func waitFor(t *testing.T, deadline time.Duration, cond func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// onNewDocument injects a script that runs before any page script in each new
// document of this tab. Tests use it to seed window.__SQ_CONFIG__.
func onNewDocument(script string) chromedp.Action[chromedp.Void] {
	return chromedp.Func(func(ctx context.Context, tgt *chromedp.Target) error {
		_, err := cdp.Call(ctx, tgt, page.AddScriptToEvaluateOnNewDocument, page.AddScriptToEvaluateOnNewDocumentParams{Source: script})
		return err
	})
}

/* -------------------------------------------------------------- SPA tests -- */

// TestUI_Login_NoCredentialInStorage (FR-UI-002/MIL-011): after login nothing
// credential-bearing ends up in localStorage, sessionStorage or document.cookie.
func TestUI_Login_NoCredentialInStorage(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.requireLogin = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	login(t, ctx, srv.URL)

	type storage struct {
		Local   string `json:"local"`
		Session string `json:"session"`
		Cookie  string `json:"cookie"`
	}
	got := mustRun(t, ctx, chromedp.Evaluate[storage](`(function(){
	  return { local: JSON.stringify(localStorage), session: JSON.stringify(sessionStorage), cookie: document.cookie };
	})()`))

	combined := got.Local + got.Session + got.Cookie
	if strings.Contains(combined, "s3cret") {
		t.Errorf("password leaked into browser storage: %q", combined)
	}
	if strings.Contains(got.Cookie, "SQ_SESSION") {
		t.Errorf("SQ_SESSION cookie is readable from JS: %q", got.Cookie)
	}
	if got.Local != "{}" || got.Session != "{}" {
		t.Errorf("expected empty web storage, got local=%q session=%q", got.Local, got.Session)
	}
}

// TestUI_CSRF_MutationTokenAttached (FR-UI-003/MIL-012): a declare carries
// X-CSRF-Token; stripping it yields 403 and the UI shows the error state.
func TestUI_CSRF_MutationTokenAttached(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.requireLogin = true; s.wantCSRF = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	login(t, ctx, srv.URL)

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/exchanges"),
		chromedp.WaitVisible(chromedp.CSS("#declare-exchange-btn")),
		chromedp.Click(chromedp.CSS("#declare-exchange-btn")),
		chromedp.WaitVisible(chromedp.CSS("#declare-name")),
	)
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var el=document.getElementById('declare-name'); el.value='myex'; el.dispatchEvent(new Event('input',{bubbles:true})); return 'ok';
	})()`))
	mustDo(t, ctx,
		chromedp.Click(chromedp.CSS("#modal-confirm")),
		chromedp.WaitVisible(chromedp.CSS(".toast")),
	)

	if !waitFor(t, 3*time.Second, func() bool { return len(stub.declarations()) > 0 }) {
		t.Fatalf("declare never reached the API")
	}
	if got := stub.csrfSeen(); got != "csrf-token-abc" {
		t.Fatalf("mutation did not carry the CSRF token, got %q", got)
	}

	// Strip the header on the way out and repeat: the API must 403 and the UI
	// must surface the failure instead of silently succeeding.
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var real = window.fetch;
	  window.fetch = function(u,o){ if (o && o.headers) { delete o.headers['X-CSRF-Token']; } return real(u,o); };
	  return 'ok';
	})()`))
	mustDo(t, ctx,
		chromedp.Click(chromedp.CSS("#declare-exchange-btn")),
		chromedp.WaitVisible(chromedp.CSS("#declare-name")),
	)
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var el=document.getElementById('declare-name'); el.value='myex2'; el.dispatchEvent(new Event('input',{bubbles:true})); return 'ok';
	})()`))
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))
	if !waitFor(t, 3*time.Second, func() bool {
		txt, err := chromedp.Run(ctx, chromedp.Text(chromedp.CSS("#declare-error")))
		return err == nil && strings.Contains(txt, "access_refused")
	}) {
		t.Fatal("UI did not render the CSRF 403 error state")
	}
}

type rect struct {
	Found  bool    `json:"found"`
	Top    float64 `json:"top"`
	Bottom float64 `json:"bottom"`
}

type foldResult struct {
	Viewport float64 `json:"viewport"`
	ScrollH  float64 `json:"scrollH"`
	Health   rect    `json:"health"`
	Cards    rect    `json:"cards"`
	QWM      rect    `json:"qwm"`
	Table    rect    `json:"table"`
}

// TestUI_S1_AboveFold (FR-UI-020/FR-UI-007/MIL-008): at 1280x720 the headline
// elements are all inside the viewport and the page does not scroll.
func TestUI_S1_AboveFold(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.EmulateViewport(1280, 720),
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#top-queues")),
	)

	got := mustRun(t, ctx, chromedp.Evaluate[foldResult](`(function(){
	  var vh = window.innerHeight;
	  function box(sel){ var el=document.querySelector(sel); if(!el) return {found:false}; var r=el.getBoundingClientRect(); return {found:true, top:r.top, bottom:r.bottom}; }
	  var tableEl = document.querySelector('#top-queues');
	  var scroller = tableEl ? tableEl.closest('.table-scroll') : null;
	  var tableBox = {found:false};
	  if (scroller) { var r = scroller.getBoundingClientRect(); tableBox = {found:true, top:r.top, bottom:r.bottom}; }
	  return { viewport: vh, scrollH: document.documentElement.scrollHeight,
	    health: box('#health'), cards: box('.cards'), qwm: box('#queues-with-messages'), table: tableBox };
	})()`))

	if got.ScrollH > got.Viewport+1 {
		t.Errorf("page scrolls at 1280x720: scrollHeight=%v viewport=%v", got.ScrollH, got.Viewport)
	}
	for name, r := range map[string]rect{"health": got.Health, "cards": got.Cards, "queues_with_messages": got.QWM, "top-queues": got.Table} {
		if !r.Found {
			t.Errorf("%s not found above the fold", name)
			continue
		}
		if r.Top < 0 || r.Bottom > got.Viewport+1 {
			t.Errorf("%s is outside the viewport: top=%v bottom=%v viewport=%v", name, r.Top, r.Bottom, got.Viewport)
		}
	}
}

// TestUI_S1_NoFlickerOn304 (FR-UI-111): a 304 never re-renders the table.
func TestUI_S1_NoFlickerOn304(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#top-queues tbody tr")),
	)
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  window.__mut = 0;
	  var tb = document.querySelector('#top-queues tbody');
	  new MutationObserver(function(){ window.__mut++; }).observe(tb, { childList: true, subtree: true });
	  return 'ok';
	})()`))

	for i := 0; i < 2; i++ {
		mustRun(t, ctx, refreshAwait(`(async function(){ await window.SQ.refreshNow(); return 'ok'; })()`))
	}
	if got := mustRun(t, ctx, chromedp.Evaluate[int](`window.__mut`)); got != 0 {
		t.Fatalf("304 refresh re-rendered the table %d times", got)
	}
}

// TestUI_S1_HeadlineFromOverview (FR-UI-020): queues_with_messages comes from
// /api/overview, never client-side aggregation of /api/queues.
func TestUI_S1_HeadlineFromOverview(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.overview["queue_totals"] = map[string]any{"messages_ready": 12, "messages_unacknowledged": 1, "messages": 13, "queues_with_messages": 7}
		s.queues = s.queues[:2]
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#queues-with-messages")))
	if got := mustRun(t, ctx, chromedp.Text(chromedp.CSS("#queues-with-messages"))); strings.TrimSpace(got) != "7" {
		t.Fatalf("headline = %q, want 7 (must come from overview.queue_totals)", got)
	}
}

// TestUI_NullRendersDashNotZero (FR-UI-005/MIL-005): null stats render an
// em-dash, never a fabricated 0.
func TestUI_NullRendersDashNotZero(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.queues = []map[string]any{{
			"name": "only", "vhost": "/", "type": "classic", "durable": true, "auto_delete": false,
			"exclusive": false, "arguments": map[string]any{}, "state": "unknown", "consumers": nil,
			"messages_ready": nil, "messages_unacknowledged": nil, "messages": nil, "stats_partial": true,
		}}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
	got := mustRun(t, ctx, chromedp.Text(chromedp.CSS(`#content tbody tr td[data-col="messages_ready"]`)))
	if strings.TrimSpace(got) != "\u2014" {
		t.Fatalf("null stat rendered %q, want an em-dash", got)
	}
}

// TestUI_UnsupportedState_Bindings (FR-IA-041/FR-IA-082): a 501 renders the
// capability notice, not an empty list.
func TestUI_UnsupportedState_Bindings(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["bindings"] = true; s.bindings501 = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/bindings"), chromedp.WaitVisible(chromedp.CSS("#content .notice")))
	txt := mustRun(t, ctx, chromedp.Text(chromedp.CSS("#content .notice")))
	if !strings.Contains(txt, "BOB-4") && !strings.Contains(txt, "not implemented") {
		t.Fatalf("bindings notice did not name the missing aggregate listing: %q", txt)
	}
	if n := mustRun(t, ctx, chromedp.Evaluate[int](`document.querySelectorAll('#content .data-table').length`)); n != 0 {
		t.Fatalf("unsupported state rendered a data table (%d)", n)
	}
}

// TestUI_PermissionDeniedState (AUTH-019/FR-IA-042): a 403 renders the
// required-permission explanation, not an empty list.
func TestUI_PermissionDeniedState(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.users403 = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/users"), chromedp.WaitVisible(chromedp.CSS("#content .notice.permission")))
	txt := mustRun(t, ctx, chromedp.Text(chromedp.CSS("#content .notice.permission")))
	if !strings.Contains(txt, "access_refused") {
		t.Fatalf("permission-denied state must surface the server reason (access_refused): %q", txt)
	}
}

// TestUI_DeletedDeepLink (FR-IA-013): a deep link to a missing queue renders
// the not-found state with a back link.
func TestUI_DeletedDeepLink(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues/missing"), chromedp.WaitVisible(chromedp.CSS("#content .notice.empty")))
	if n := mustRun(t, ctx, chromedp.Evaluate[int](`document.querySelectorAll('#content .notice.empty a[href="/queues"]').length`)); n != 1 {
		t.Fatalf("not-found state has no back link to /queues (%d)", n)
	}
}

// TestUI_ConfirmGestures (FR-UI-105/MIL-027): purge needs the checkbox and
// delete needs the exact name before Confirm enables.
func TestUI_ConfirmGestures(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/queues/orders"),
		chromedp.WaitVisible(chromedp.CSS("#purge-btn")),
		chromedp.Click(chromedp.CSS("#purge-btn")),
		chromedp.WaitVisible(chromedp.CSS("#modal-confirm")),
	)
	if disabled := mustRun(t, ctx, chromedp.Evaluate[bool](`document.getElementById('modal-confirm').disabled`)); !disabled {
		t.Fatal("purge Confirm must be disabled before the acknowledgement checkbox")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#purge-ack")))
	if disabled := mustRun(t, ctx, chromedp.Evaluate[bool](`document.getElementById('modal-confirm').disabled`)); disabled {
		t.Fatal("purge Confirm must enable once acknowledged")
	}
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-cancel")), chromedp.WaitNotPresent(chromedp.CSS("#modal-confirm")))

	mustDo(t, ctx,
		chromedp.Click(chromedp.CSS("#delete-btn")),
		chromedp.WaitVisible(chromedp.CSS("#delete-confirm-input")),
	)
	if disabled := mustRun(t, ctx, chromedp.Evaluate[bool](`document.getElementById('modal-confirm').disabled`)); !disabled {
		t.Fatal("delete Confirm must be disabled until the name is typed")
	}
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var el=document.getElementById('delete-confirm-input'); el.value='orders'; el.dispatchEvent(new Event('input',{bubbles:true})); return 'ok';
	})()`))
	if disabled := mustRun(t, ctx, chromedp.Evaluate[bool](`document.getElementById('modal-confirm').disabled`)); disabled {
		t.Fatal("delete Confirm must enable once the exact name is typed")
	}
}

// TestUI_XSS_QueueNameScript (SEC-030/MIL-011): a hostile queue name renders
// inert.
func TestUI_XSS_QueueNameScript(t *testing.T) {
	const hostile = `<script>window.__pwned=1</script>`
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.queues = []map[string]any{{
			"name": hostile, "vhost": "/", "type": "classic", "durable": true, "auto_delete": false,
			"exclusive": false, "arguments": map[string]any{}, "state": "running", "consumers": 0,
			"messages_ready": 0, "messages_unacknowledged": 0, "messages": 0, "stats_partial": false,
		}}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
	if pwned := mustRun(t, ctx, chromedp.Evaluate[string](`String(window.__pwned)`)); pwned != "undefined" {
		t.Fatalf("script tag executed: window.__pwned = %s", pwned)
	}
	cell := mustRun(t, ctx, chromedp.Text(chromedp.CSS(`#content tbody tr td[data-col="name"]`)))
	if !strings.Contains(cell, hostile) {
		t.Fatalf("hostile name did not render as inert text: %q", cell)
	}
}

// TestUI_A11y_FocusTrap (FR-UI-008/FR-IA-061): closing a destructive dialog
// with Esc restores focus to the trigger.
func TestUI_A11y_FocusTrap(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/queues/orders"),
		chromedp.WaitVisible(chromedp.CSS("#purge-btn")),
	)
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  document.getElementById('purge-btn').focus();
	  document.getElementById('purge-btn').click();
	  return 'ok';
	})()`))
	mustDo(t, ctx, chromedp.WaitVisible(chromedp.CSS("#modal-confirm")))
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
	  return 'ok';
	})()`))
	mustDo(t, ctx, chromedp.WaitNotPresent(chromedp.CSS("#modal-confirm")))
	if active := mustRun(t, ctx, chromedp.Evaluate[string](`(document.activeElement && document.activeElement.id) || ''`)); active != "purge-btn" {
		t.Fatalf("focus not restored to the trigger, activeElement id = %q", active)
	}
}

// TestUI_TabHiddenPausesRefresh (FR-UI-113): a hidden document stops fetching.
func TestUI_TabHiddenPausesRefresh(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#top-queues tbody tr")))

	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  clearTimeout(window.SQ.state.refresh.timer);
	  Object.defineProperty(document, 'hidden', { configurable: true, get: function(){ return true; } });
	  Object.defineProperty(document, 'visibilityState', { configurable: true, get: function(){ return 'hidden'; } });
	  document.dispatchEvent(new Event('visibilitychange'));
	  return 'ok';
	})()`))
	base := stub.hitCount()
	for i := 0; i < 3; i++ {
		mustRun(t, ctx, refreshAwait(`(async function(){ await window.SQ.refreshNow(); return 'ok'; })()`))
	}
	if after := stub.hitCount(); after != base {
		t.Fatalf("hidden document still fetched: hits %d -> %d", base, after)
	}

	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  Object.defineProperty(document, 'hidden', { configurable: true, get: function(){ return false; } });
	  Object.defineProperty(document, 'visibilityState', { configurable: true, get: function(){ return 'visible'; } });
	  document.dispatchEvent(new Event('visibilitychange'));
	  return 'ok';
	})()`))
	if !waitFor(t, 3*time.Second, func() bool { return stub.hitCount() > base }) {
		t.Fatal("visible document did not resume fetching")
	}
}

// TestUI_BackoffOnError (FR-UI-112): the retry interval grows 2-4-8-30 s and
// the table is never cleared.
func TestUI_BackoffOnError(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#top-queues tbody tr")))
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){ clearTimeout(window.SQ.state.refresh.timer); return 'ok'; })()`))
	stub.set(func(s *stubAPI) { s.readFail = true })

	var delays []string
	for i := 0; i < 4; i++ {
		d := mustRun(t, ctx, refreshAwait(`(async function(){
		  await window.SQ.refreshNow();
		  return document.getElementById('refresh-status').dataset.delayMs;
		})()`))
		delays = append(delays, d)
	}
	want := []string{"2000", "4000", "8000", "30000"}
	if strings.Join(delays, ",") != strings.Join(want, ",") {
		t.Fatalf("backoff ladder = %v, want %v", delays, want)
	}
	if rows := mustRun(t, ctx, chromedp.Evaluate[int](`document.querySelectorAll('#top-queues tbody tr').length`)); rows != 3 {
		t.Fatalf("refresh error cleared the table (%d rows remain)", rows)
	}
}

// TestUI_AuthChallengeDoesNotBlockShell (FR-UI-002) proves the SPA never hangs
// when the API's 401 carries WWW-Authenticate: Basic: the bounded auth probe
// times out and the login form renders. This is the production header (spec 01
// §5.2); Chromium defers a credentials-carrying fetch on it waiting for the
// native prompt, so the SPA must not depend on the fetch ever settling.
func TestUI_AuthChallengeDoesNotBlockShell(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.requireLogin = true; s.challenge = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		onNewDocument(`window.__SQ_CONFIG__ = { authTimeoutMs: 400 };`),
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#login-form")),
	)
}

// TestUI_CapabilitiesHideUnsupported (COMPAT-043): the Bindings screen renders
// the unsupported state when the capability is false and the controls when the
// capability is true. The unsupported state must name the missing feature and
// must not render a data table.
func TestUI_CapabilitiesHideUnsupported(t *testing.T) {
	t.Run("unsupported", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) { s.caps["bindings"] = false; s.bindings501 = true })
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/bindings"), chromedp.WaitVisible(chromedp.CSS("#content .notice")))
		txt := mustRun(t, ctx, chromedp.Text(chromedp.CSS("#content .notice")))
		if !strings.Contains(txt, "not implemented") && !strings.Contains(txt, "BOB-4") {
			t.Fatalf("unsupported notice did not name the missing feature: %q", txt)
		}
		if n := mustRun(t, ctx, chromedp.Evaluate[int](`document.querySelectorAll('#content .data-table').length`)); n != 0 {
			t.Fatalf("unsupported state rendered a data table (%d)", n)
		}
	})
	t.Run("supported", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) { s.caps["bindings"] = true })
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/bindings"), chromedp.WaitVisible(chromedp.CSS("#content .data-table")))
	})
}
