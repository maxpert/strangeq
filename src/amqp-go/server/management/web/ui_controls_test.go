package web

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// makeQueues builds n synthetic queue rows so a list screen can be exercised
// with a full page.
func makeQueues(n int) []map[string]any {
	out := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, map[string]any{
			"name": fmt.Sprintf("q-%02d", i), "vhost": "/", "type": "classic", "durable": true,
			"auto_delete": false, "exclusive": false, "arguments": map[string]any{},
			"state": "running", "consumers": 0, "messages_ready": i,
			"messages_unacknowledged": 0, "messages": i, "stats_partial": false,
		})
	}
	return out
}

// TestUI_QueuesPagination (FR-UI-100) proves the queues list renders the shared
// pagination controls: the 25/50/100/250/500 page-size selector, the
// total-or-more indicator and Prev/Next. Clicking Next must put the canonical
// params on a clone of the current route query and re-fetch page 2.
func TestUI_QueuesPagination(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.queues = makeQueues(50)
		s.queueTotal = 120
		s.queuePageCount = 3
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/queues"),
		chromedp.WaitVisible(chromedp.CSS("#page-size")),
	)

	opts := mustRun(t, ctx, chromedp.Evaluate[[]string](
		`Array.prototype.map.call(document.querySelectorAll('#page-size option'), function(o){return o.value;})`))
	wantOpts := []string{"25", "50", "100", "250", "500"}
	if strings.Join(opts, ",") != strings.Join(wantOpts, ",") {
		t.Fatalf("page-size options = %v, want %v", opts, wantOpts)
	}

	ind := mustRun(t, ctx, chromedp.Text(chromedp.CSS(".pagination .pagination-total")))
	if !strings.Contains(ind, "showing 50 of 120") {
		t.Fatalf("pagination indicator = %q, want it to contain %q", ind, "showing 50 of 120")
	}

	filters := mustRun(t, ctx, chromedp.Evaluate[[]string](
		`Array.prototype.map.call(document.querySelectorAll('.toolbar [data-filter]'), function(o){return o.dataset.filter;})`))
	for _, f := range []string{"name", "durable", "has_consumers", "non_empty"} {
		if !slices.Contains(filters, f) {
			t.Errorf("queues toolbar missing filter %q (got %v)", f, filters)
		}
	}

	// Column sort: clicking a header sets the canonical sort params and
	// re-queries page 1 (no optimistic reordering of the DOM).
	before := stub.queuePageRequests(1)
	mustDo(t, ctx, chromedp.Click(chromedp.CSS(`#content th[data-col="messages_ready"]`)))
	if !waitFor(t, 3*time.Second, func() bool {
		got, err := chromedp.Run(ctx, chromedp.Evaluate[string](`location.search`))
		return err == nil && strings.Contains(got, "sort=messages_ready")
	}) {
		t.Fatal("clicking a column header did not set the sort param")
	}
	if !waitFor(t, 3*time.Second, func() bool { return stub.queuePageRequests(1) > before }) {
		t.Fatal("clicking a column header did not re-query page 1")
	}

	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#page-next")))
	if !waitFor(t, 3*time.Second, func() bool {
		got, err := chromedp.Run(ctx, chromedp.Evaluate[string](`location.search`))
		return err == nil && strings.Contains(got, "page=2")
	}) {
		t.Fatal("clicking Next did not navigate to page=2")
	}
	if !waitFor(t, 3*time.Second, func() bool { return stub.queuePageSeen(2) }) {
		t.Fatal("the API never saw a queues request with page=2")
	}
}

// TestUI_PaginationUnknownTotal (FR-UI-005/FR-UI-100) proves an envelope whose
// total_count is absent renders an em-dash / "unknown", never a fabricated 0.
func TestUI_PaginationUnknownTotal(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.queues = makeQueues(50)
		s.queueTotal = 120
		s.omitTotal = true
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS(".pagination .pagination-total")))
	got := mustRun(t, ctx, chromedp.Text(chromedp.CSS(".pagination .pagination-total")))
	if !strings.Contains(got, "unknown") || !strings.Contains(got, "\u2014") {
		t.Fatalf("missing total_count must render an em-dash and %q, got %q", "unknown", got)
	}
	if strings.Contains(got, "of 0") {
		t.Fatalf("missing total_count rendered as 0: %q", got)
	}
}

// TestUI_PaginationFailureShowsError (FR-UI-100) proves a failed page request
// renders the error state rather than leaving an optimistic or stale table.
func TestUI_PaginationFailureShowsError(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.queues = makeQueues(50)
		s.queueTotal = 120
		s.queuePageCount = 3
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#page-size")))
	stub.set(func(s *stubAPI) { s.readFail = true })
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#page-next")))
	if !waitFor(t, 3*time.Second, func() bool {
		n, err := chromedp.Run(ctx, chromedp.Evaluate[int](`document.querySelectorAll('#content .notice').length`))
		return err == nil && n > 0
	}) {
		t.Fatal("a failed page request did not render the error state")
	}
}

// TestUI_ListScreenBackoffLadder (FR-UI-112/F9) pins the list-screen retry
// ladder to the fixed 2/4/8/30 s schedule, independent of the healthy cadence,
// and proves a refresh error does not clear the table.
func TestUI_ListScreenBackoffLadder(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
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
		t.Fatalf("list-screen backoff ladder = %v, want %v", delays, want)
	}
	if rows := mustRun(t, ctx, chromedp.Evaluate[int](`document.querySelectorAll('#content tbody tr').length`)); rows == 0 {
		t.Fatal("refresh error cleared the list table")
	}
}

// TestUI_CloseConnectionCapabilityOff (F3) proves capabilities.close_connection
// === false disables the close button with a tooltip naming the capability.
func TestUI_CloseConnectionCapabilityOff(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.caps["close_connection"] = false })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections/c1"), chromedp.WaitVisible(chromedp.CSS("#close-connection-btn")))
	if disabled := mustRun(t, ctx, chromedp.Evaluate[bool](`document.getElementById('close-connection-btn').disabled`)); !disabled {
		t.Fatal("close-connection button must be disabled when capabilities.close_connection is false")
	}
	title := mustRun(t, ctx, chromedp.Evaluate[string](`document.getElementById('close-connection-btn').title`))
	if !strings.Contains(strings.ToLower(title), "close_connection") {
		t.Fatalf("close-connection tooltip must name the capability, got %q", title)
	}
}

// TestUI_QueueStatsPartialMarker (F6) proves a queue with stats_partial renders
// the shared stat-partial marker (the stub's "tasks" queue).
func TestUI_QueueStatsPartialMarker(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
	marker := mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var tr = Array.prototype.find.call(document.querySelectorAll('#content tbody tr'), function(r){ return r.dataset.key === 'tasks'; });
	  if (!tr) return '<no tasks row>';
	  var m = tr.querySelector('.stat-partial');
	  return m ? m.textContent : '<no marker>';
	})()`))
	if !strings.Contains(strings.TrimSpace(marker), "partial") {
		t.Fatalf("tasks row must render a stat-partial marker, got %q", marker)
	}
}

// TestUI_AboutCapabilityStates (F7) proves About derives a distinct status for
// every capability value shape: true, [] , 501 and false.
func TestUI_AboutCapabilityStates(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.caps = map[string]any{
			"a_supported": true,
			"b_empty":     []any{},
			"c_501":       501,
			"d_blocked":   false,
		}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/about"), chromedp.WaitVisible(chromedp.CSS("#content .data-table")))
	txt := strings.ToLower(mustRun(t, ctx, chromedp.Text(chromedp.CSS("#content .data-table"))))
	for _, want := range []string{"supported", "empty", "not implemented", "blocked"} {
		if !strings.Contains(txt, want) {
			t.Errorf("About capability table lacks the %q state:\n%s", want, txt)
		}
	}
}

// TestUI_DegradedDashNotZero (F8) proves a null discarded_artifacts renders an
// em-dash in the degraded banner, never a fabricated 0.
func TestUI_DegradedDashNotZero(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.overview["unsafe_recovery"] = map[string]any{"degraded": true, "discarded_artifacts": nil}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#alarm-banner")))
	txt := mustRun(t, ctx, chromedp.Text(chromedp.CSS("#alarm-banner")))
	if !strings.Contains(txt, "\u2014") {
		t.Fatalf("degraded banner must render an em-dash for a null discarded_artifacts: %q", txt)
	}
	if strings.Contains(txt, "0 artifacts") {
		t.Fatalf("null discarded_artifacts rendered as 0: %q", txt)
	}
}

// TestUI_HealthAlarmFlip (F11) proves the banner flips to alarm within a
// refresh interval and clears again when the alarm clears; the two banner
// sources (health and degraded) must merge ordering-safely.
func TestUI_HealthAlarmFlip(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		onNewDocument(`window.__SQ_CONFIG__ = { refreshInterval: 200, s1RefreshInterval: 200 };`),
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#top-queues tbody tr")),
	)

	stub.set(func(s *stubAPI) { s.healthStatus = "alarm"; s.healthReason = "memory resource alarm active" })
	if !waitFor(t, 5*time.Second, func() bool {
		txt, err := chromedp.Run(ctx, chromedp.Text(chromedp.CSS("#alarm-banner")))
		return err == nil && strings.Contains(txt, "Alarm active")
	}) {
		t.Fatal("banner did not flip to alarm within a refresh interval")
	}

	stub.set(func(s *stubAPI) { s.healthStatus = "ok"; s.healthReason = "" })
	if !waitFor(t, 5*time.Second, func() bool {
		hidden, err := chromedp.Run(ctx, chromedp.Evaluate[bool](`document.getElementById('alarm-banner').hidden`))
		return err == nil && hidden
	}) {
		t.Fatal("banner did not clear after the alarm cleared")
	}
}

// TestUI_QueueEditorArgsAndNameValidation (S6/S8) proves the declare-queue
// editor accepts only x-* argument keys, validates the queue name, and carries
// the arguments to the API.
func TestUI_QueueEditorArgsAndNameValidation(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/queues"),
		chromedp.WaitVisible(chromedp.CSS("#declare-queue-btn")),
		chromedp.Click(chromedp.CSS("#declare-queue-btn")),
		chromedp.WaitVisible(chromedp.CSS("#declare-queue-arguments")),
	)
	setField := func(id, value string) {
		mustRun(t, ctx, chromedp.Evaluate[string](fmt.Sprintf(`(function(){
		  var el=document.getElementById(%q); el.value=%q; el.dispatchEvent(new Event('input',{bubbles:true})); return 'ok';
		})()`, id, value)))
	}

	// A non-x-* argument key is refused client side before any request.
	setField("declare-queue-name", "argsq")
	setField("declare-queue-arguments", "notx=1")
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")))
	if !waitFor(t, 2*time.Second, func() bool {
		txt, err := chromedp.Run(ctx, chromedp.Evaluate[string](`(document.getElementById('declare-error') || {}).textContent || ''`))
		return err == nil && strings.Contains(txt, "x-")
	}) {
		got, _ := chromedp.Run(ctx, chromedp.Evaluate[string](`JSON.stringify({
		  err: (document.getElementById('declare-error') || {}).textContent || '',
		  modal: !!document.getElementById('modal-confirm'),
		  name: (document.getElementById('declare-queue-name') || {}).value || '',
		  args: (document.getElementById('declare-queue-arguments') || {}).value || ''
		})`))
		t.Fatalf("a non-x-* argument key was not refused: %s", got)
	}

	// A name with whitespace keeps Confirm disabled.
	setField("declare-queue-arguments", "x-message-ttl=60000")
	setField("declare-queue-name", "bad name")
	if disabled := mustRun(t, ctx, chromedp.Evaluate[bool](`document.getElementById('modal-confirm').disabled`)); !disabled {
		t.Fatal("a queue name containing whitespace must keep Confirm disabled")
	}

	setField("declare-queue-name", "argsq")
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-confirm")), chromedp.WaitVisible(chromedp.CSS(".toast")))
	if !waitFor(t, 3*time.Second, func() bool {
		for _, d := range stub.declarations() {
			if d == "queue:argsq" {
				return true
			}
		}
		return false
	}) {
		t.Fatal("the declare never carried a valid x-* argument to the API")
	}
	if body := stub.queueDeclareBody(); body == nil {
		t.Fatal("the declaration body was empty")
	} else if args, ok := body["arguments"].(map[string]any); !ok || args["x-message-ttl"] == nil {
		t.Fatalf("the x-message-ttl argument was not sent: %v", body)
	}
}

// TestUI_MiscControls (S2/S7 + copy affordances) proves the remaining cheap
// SHOULD features exist: a publish properties editor, per-message and
// deep-link copy affordances, and a per-row Close column. (The /metrics link is
// config-driven and covered by TestUI_MetricsLink.)
func TestUI_MiscControls(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	// S2: a per-row Close column on the connections list.
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
	if n := mustRun(t, ctx, chromedp.Evaluate[int](`document.querySelectorAll('#content tbody tr td[data-col="actions"] button').length`)); n != 1 {
		t.Errorf("connections list must render a per-row Close column, found %d", n)
	}

	// S7: a properties/headers editor in the publish modal.
	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/exchanges/custom"),
		chromedp.WaitVisible(chromedp.CSS("#publish-btn")),
		chromedp.Click(chromedp.CSS("#publish-btn")),
		chromedp.WaitVisible(chromedp.CSS("#publish-properties")),
	)
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#modal-cancel")))

	// deep-link copy affordance on a detail page.
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues/orders"), chromedp.WaitVisible(chromedp.CSS("#copy-link-btn")))

	// Get-body copy affordance after fetching a message.
	mustDo(t, ctx,
		chromedp.Click(chromedp.CSS("#get-btn")),
		chromedp.WaitVisible(chromedp.CSS("#get-count")),
		chromedp.Click(chromedp.CSS("#modal-confirm")),
		chromedp.WaitVisible(chromedp.CSS(".copy-btn")),
	)
}
