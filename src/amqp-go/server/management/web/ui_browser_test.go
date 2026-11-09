package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

/* ------------------------------------------------------------------ setup -- */

// Per-tab deadline policy. A UI test opens one fresh tab from the package
// browser; every tab operation is bounded so no test can hang, but the bound is
// derived from the go-test budget instead of a hard-coded 30 s so a slow host
// cannot turn "the tab needed longer" into a bare "context deadline exceeded".
const (
	// defaultBrowserDeadline is the generous per-tab budget used when the
	// go-test budget is unknown or larger than this.
	defaultBrowserDeadline = 90 * time.Second
	// minBrowserDeadline is the hard floor for the built-in default: a cold tab
	// start plus a real page load cannot finish reliably below it. It is not
	// applied to an explicit override, and it yields to a smaller go-test
	// budget so a tab can never outlive the test binary.
	minBrowserDeadline = 30 * time.Second
	// browserDeadlineMargin is reserved out of the go-test budget so the tab
	// bound fires first. The failure is then attributed to the tab stage with a
	// full diagnosis instead of the test binary timing out mid-message.
	browserDeadlineMargin = 5 * time.Second
)

// browserDeadlineBudget is the pure budget arithmetic behind newTab's timeout.
// envOverride is the raw SQ_BROWSER_DEADLINE value (empty when unset); it
// returns an error only for a malformed override so a CI typo fails loudly
// rather than silently falling back.
//
// Policy:
//   - start from the env override, or the default (floored at minBrowserDeadline)
//     when unset;
//   - clamp to (go-test deadline - browserDeadlineMargin) whenever that is
//     smaller, so a tab can never outlive the go-test binary;
//   - never return zero or negative: an already-spent budget yields the
//     smallest positive bound so the tab fails fast and diagnosably.
func browserDeadlineBudget(now, deadline time.Time, hasDeadline bool, envOverride string) (time.Duration, error) {
	d := max(defaultBrowserDeadline, minBrowserDeadline)
	if envOverride != "" {
		parsed, err := time.ParseDuration(envOverride)
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("SQ_BROWSER_DEADLINE=%q is not a positive Go duration (e.g. 90s or 2m)", envOverride)
		}
		// An explicit override is honoured exactly (CI may want a shorter
		// bound); it is still clamped to the go-test budget below.
		d = parsed
	}
	if hasDeadline {
		if limit := deadline.Sub(now) - browserDeadlineMargin; limit < d {
			d = limit
		}
	}
	if d <= 0 {
		d = time.Millisecond
	}
	return d, nil
}

// browserDeadline resolves the per-tab bound for t from its go-test budget and
// the optional SQ_BROWSER_DEADLINE override.
func browserDeadline(t *testing.T) time.Duration {
	t.Helper()
	dl, has := t.Deadline()
	d, err := browserDeadlineBudget(time.Now(), dl, has, os.Getenv("SQ_BROWSER_DEADLINE"))
	if err != nil {
		t.Fatalf("browser harness: %v", err)
	}
	return d
}

var (
	browserCtx    context.Context
	browserCancel context.CancelFunc
	browserErr    error
)

// browserTabKey carries the per-tab bound and the stage currently in flight so
// a timeout can name the test, the stage and the bound.
type browserTabKey struct{}

// browserTab is the diagnostic state attached to a tab's context.
type browserTab struct {
	bound   time.Duration
	started time.Time

	mu    sync.Mutex
	stage string
}

func (b *browserTab) setStage(s string) {
	b.mu.Lock()
	b.stage = s
	b.mu.Unlock()
}

func (b *browserTab) stageNow() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stage == "" {
		return "tab setup"
	}
	return b.stage
}

func tabFrom(ctx context.Context) *browserTab {
	b, _ := ctx.Value(browserTabKey{}).(*browserTab)
	return b
}

// callerSite names the short file:line of the caller skip frames up, so a
// timeout points at the exact harness call that hung.
func callerSite(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

// tracedSteps wraps each action so the tab records the stage in flight. The
// wrapper is transparent: it records the index and forwards to the action, so
// execution semantics and assertions are unchanged.
func tracedSteps(b *browserTab, site string, steps []chromedp.Action[chromedp.Void]) []chromedp.Action[chromedp.Void] {
	if b == nil || len(steps) == 0 {
		return steps
	}
	out := make([]chromedp.Action[chromedp.Void], len(steps))
	for i, step := range steps {
		i, step := i, step
		out[i] = func(ctx context.Context, tgt *chromedp.Target) (chromedp.Void, error) {
			b.setStage(fmt.Sprintf("%s step %d/%d", site, i+1, len(steps)))
			return step(ctx, tgt)
		}
	}
	return out
}

// tabFailure renders the diagnosis appended to a chromedp failure: the test,
// the tab stage in flight, the bound, the elapsed time and the go-test budget
// left. It never changes the outcome, only the message.
func tabFailure(ctx context.Context, t *testing.T, err error) string {
	parts := []string{fmt.Sprintf("test %s", t.Name())}
	if b := tabFrom(ctx); b != nil {
		parts = append(parts,
			fmt.Sprintf("stage %s", b.stageNow()),
			fmt.Sprintf("tab bound %s", b.bound),
			fmt.Sprintf("elapsed %s", time.Since(b.started).Round(10*time.Millisecond)),
		)
	}
	if dl, ok := t.Deadline(); ok {
		parts = append(parts, fmt.Sprintf("go-test budget left %s", time.Until(dl).Round(10*time.Millisecond)))
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		parts = append(parts,
			"tab deadline exceeded: raise SQ_BROWSER_DEADLINE or the go-test -timeout so the tab bound stays ahead of the budget")
	}
	return " [" + strings.Join(parts, "; ") + "]"
}

// newTab opens a fresh tab that shares the package browser. Every test gets a
// tab deadline derived from its go-test budget (see browserDeadlineBudget), so
// no test can hang and a slow host is attributed to the tab, not the binary.
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
	bound := browserDeadline(t)
	ctx, cancelTimeout := context.WithTimeout(tabCtx, bound)
	ctx = context.WithValue(ctx, browserTabKey{}, &browserTab{bound: bound, started: time.Now()})
	return ctx, func() { cancelTimeout(); cancelTab() }
}

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
	if err := chromedp.Do(ctx, tracedSteps(tabFrom(ctx), callerSite(2), steps)...); err != nil {
		t.Fatalf("chromedp: %v%s", err, tabFailure(ctx, t, err))
	}
}

func mustRun[T any](t *testing.T, ctx context.Context, a chromedp.Action[T]) T {
	t.Helper()
	if b := tabFrom(ctx); b != nil {
		inner, site := a, callerSite(2)
		a = func(ctx context.Context, tgt *chromedp.Target) (T, error) {
			b.setStage(site)
			return inner(ctx, tgt)
		}
	}
	v, err := chromedp.Run(ctx, a)
	if err != nil {
		t.Fatalf("chromedp: %v%s", err, tabFailure(ctx, t, err))
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

/* ------------------------------------------------------- harness self-test -- */

// TestUI_BrowserDeadlineBudget pins the tab-deadline arithmetic: the bound is
// generous by default, honours SQ_BROWSER_DEADLINE, never drops below the hard
// floor unless the go-test budget forces it, and never outlives the budget.
func TestUI_BrowserDeadlineBudget(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const far = 10 * time.Minute

	cases := []struct {
		name     string
		deadline time.Time
		hasDead  bool
		override string
		want     time.Duration
		wantErr  bool
	}{
		{name: "default without a known go-test deadline", hasDead: false, want: defaultBrowserDeadline},
		{name: "generous budget keeps the default", deadline: now.Add(far), hasDead: true, want: defaultBrowserDeadline},
		{name: "tight budget clamps below the floor", deadline: now.Add(40 * time.Second), hasDead: true, want: 35 * time.Second},
		{name: "override wins under a generous budget", deadline: now.Add(far), hasDead: true, override: "2m", want: 2 * time.Minute},
		{name: "override is still clamped by the budget", deadline: now.Add(40 * time.Second), hasDead: true, override: "2m", want: 35 * time.Second},
		{name: "short override is honoured below the floor", hasDead: false, override: "10s", want: 10 * time.Second},
		{name: "spent budget yields a positive bound", deadline: now.Add(2 * time.Second), hasDead: true, want: time.Millisecond},
		{name: "malformed override", hasDead: false, override: "banana", wantErr: true},
		{name: "non-positive override", hasDead: false, override: "0s", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := browserDeadlineBudget(now, tc.deadline, tc.hasDead, tc.override)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("browserDeadlineBudget(%q) = %s, want an error", tc.override, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("browserDeadlineBudget(%q): %v", tc.override, err)
			}
			if got != tc.want {
				t.Fatalf("browserDeadlineBudget(%q) = %s, want %s", tc.override, got, tc.want)
			}
		})
	}
	if defaultBrowserDeadline < minBrowserDeadline {
		t.Fatalf("default %s is below the hard floor %s", defaultBrowserDeadline, minBrowserDeadline)
	}
}

// TestUI_BrowserTabDeadlineIsBudgetAware proves newTab derives its bound from
// the go-test budget: the returned context is always bounded, matches the
// resolver's value, and never outlives the go-test binary.
func TestUI_BrowserTabDeadlineIsBudgetAware(t *testing.T) {
	t.Setenv("SQ_BROWSER_DEADLINE", "")
	ctx, cancel := newTab(t)
	defer cancel()

	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("newTab returned an unbounded tab context")
	}
	budget, ok := t.Deadline()
	if !ok {
		t.Fatal("go test reported no deadline; the harness relies on one")
	}
	want, err := browserDeadlineBudget(time.Now(), budget, true, "")
	if err != nil {
		t.Fatalf("browserDeadlineBudget: %v", err)
	}
	got := time.Until(dl)
	if delta := got - want; delta > time.Second || delta < -time.Second {
		t.Fatalf("tab bound = %s, want %s (within 1s of the go-test budget)", got.Round(time.Millisecond), want)
	}
	if got > time.Until(budget)-browserDeadlineMargin+time.Second {
		t.Fatalf("tab bound %s outlives the go-test budget %s", got.Round(time.Millisecond), time.Until(budget).Round(time.Millisecond))
	}
}

// TestUI_BrowserTabFailureDiagnosis proves a timed-out tab stage produces a
// message naming the test, the stage and the bound instead of a bare
// "context deadline exceeded".
func TestUI_BrowserTabFailureDiagnosis(t *testing.T) {
	base, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-base.Done()

	b := &browserTab{bound: 42 * time.Second, started: time.Now().Add(-43 * time.Second)}
	b.setStage("ui_browser_test.go:999 step 2/3")
	ctx := context.WithValue(base, browserTabKey{}, b)

	msg := tabFailure(ctx, t, context.DeadlineExceeded)
	for _, want := range []string{
		"test " + t.Name(),
		"stage ui_browser_test.go:999 step 2/3",
		"tab bound 42s",
		"tab deadline exceeded",
		"SQ_BROWSER_DEADLINE",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("diagnosis %q does not contain %q", msg, want)
		}
	}
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
