package web

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

/* ------------------------------------------------------------- helpers ---- */

func evalStr(t *testing.T, ctx context.Context, expr string) string {
	t.Helper()
	return mustRun(t, ctx, chromedp.Evaluate[string](expr))
}

func evalBool(t *testing.T, ctx context.Context, expr string) bool {
	t.Helper()
	return mustRun(t, ctx, chromedp.Evaluate[bool](expr))
}

func evalInt(t *testing.T, ctx context.Context, expr string) int {
	t.Helper()
	return mustRun(t, ctx, chromedp.Evaluate[int](expr))
}

// evalStrOK evaluates without failing, for polling conditions.
func evalStrOK(ctx context.Context, expr string) (string, error) {
	return chromedp.Run(ctx, chromedp.Evaluate[string](expr))
}

func makeUsers(n int) []map[string]any {
	out := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, map[string]any{"name": fmt.Sprintf("u-%02d", i), "tags": []string{"monitoring"}})
	}
	return out
}

/* ------------------------------------------------- finding 1: sort wiring -- */

// TestUI_ListSortWiring (round-2 #1) proves every list screen tells the truth
// about sorting: the five endpoints that accept sort/sort_reverse send the
// canonical params and render the server's (reversed) order, while the three
// bare-array endpoints advertise no sorting at all.
func TestUI_ListSortWiring(t *testing.T) {
	sortable := []struct {
		name, path, kind, thKey, firstCol, wantFirst string
	}{
		{"queues", "/queues", "queues", "name", "name", "tasks"},
		{"exchanges", "/exchanges", "exchanges", "name", "name", "custom"},
		{"connections", "/connections", "connections", "name", "name", "c2"},
		{"channels", "/channels", "channels", "number", "number", "2"},
		{"consumers", "/consumers", "consumers", "consumer_tag", "consumer_tag", "ctag-2"},
	}
	for _, tc := range sortable {
		t.Run(tc.name, func(t *testing.T) {
			stub := newStub()
			stub.set(func(s *stubAPI) {
				switch tc.kind {
				case "connections":
					s.connectionItems = []map[string]any{
						{"name": "c1", "user": "guest", "vhost": "/", "state": "running", "channels": 1, "peer_host": "127.0.0.1", "peer_port": 5000},
						{"name": "c2", "user": "guest", "vhost": "/", "state": "running", "channels": 1, "peer_host": "127.0.0.1", "peer_port": 5001},
					}
				case "channels":
					s.channelItems = []map[string]any{
						{"number": 1, "connection": "c1", "user": "guest", "vhost": "/", "state": "running", "consumers": 1},
						{"number": 2, "connection": "c2", "user": "guest", "vhost": "/", "state": "running", "consumers": 0},
					}
				case "consumers":
					s.consumerItems = []map[string]any{
						{"consumer_tag": "ctag-1", "queue": "orders", "ack_required": true, "exclusive": false, "prefetch_count": 10},
						{"consumer_tag": "ctag-2", "queue": "events", "ack_required": true, "exclusive": false, "prefetch_count": 5},
					}
				}
			})
			srv := appServer(t, stub)
			ctx, cancel := newTab(t)
			defer cancel()

			mustDo(t, ctx,
				chromedp.Navigate(srv.URL+tc.path),
				chromedp.WaitVisible(chromedp.CSS(`#content th[data-col="`+tc.thKey+`"]`)),
			)
			mustDo(t, ctx, chromedp.Click(chromedp.CSS(`#content th[data-col="`+tc.thKey+`"]`)))
			if !waitFor(t, 3*time.Second, func() bool { return stub.querySeen(tc.kind, "sort="+tc.thKey) }) {
				t.Fatalf("%s: clicking a header did not send sort=%s; sent %v", tc.name, tc.thKey, stub.queriesFor(tc.kind))
			}
			if !stub.querySeen(tc.kind, "sort_reverse=true") {
				t.Fatalf("%s: header click did not send sort_reverse=true; sent %v", tc.name, stub.queriesFor(tc.kind))
			}
			cellExpr := fmt.Sprintf(`(function(){
			  var tr = document.querySelector('#content tbody tr');
			  if (!tr) return '';
			  var td = tr.querySelector('td[data-col="%s"]');
			  return td ? td.textContent : '';
			})()`, tc.firstCol)
			if !waitFor(t, 3*time.Second, func() bool {
				got, err := evalStrOK(ctx, cellExpr)
				return err == nil && strings.Contains(got, tc.wantFirst)
			}) {
				got, _ := evalStrOK(ctx, cellExpr)
				t.Fatalf("%s: first row %q does not match the reversed set (want %q)", tc.name, got, tc.wantFirst)
			}
		})
	}

	unsortable := []struct{ name, path, kind string }{
		{"nodes", "/nodes", "nodes"},
		{"users", "/users", "users"},
		{"permissions", "/permissions", "permissions"},
	}
	for _, tc := range unsortable {
		t.Run(tc.name+"_unsortable", func(t *testing.T) {
			stub := newStub()
			srv := appServer(t, stub)
			ctx, cancel := newTab(t)
			defer cancel()

			mustDo(t, ctx,
				chromedp.Navigate(srv.URL+tc.path),
				chromedp.WaitVisible(chromedp.CSS("#content table.data-table")),
			)
			if n := evalInt(t, ctx, `document.querySelectorAll('#content th.sortable').length`); n != 0 {
				t.Fatalf("%s: %d headers advertise sorting but the endpoint does not accept it", tc.name, n)
			}
			if n := evalInt(t, ctx, `document.querySelectorAll('#content th[aria-sort]').length`); n != 0 {
				t.Fatalf("%s: %d headers carry aria-sort but the endpoint does not accept it", tc.name, n)
			}
			before := stub.queryCount(tc.kind)
			beforeSearch := evalStr(t, ctx, `location.search`)
			evalStr(t, ctx, `(function(){ var th=document.querySelector('#content th'); if (th) th.click(); return 'ok'; })()`)
			if after := evalStr(t, ctx, `location.search`); after != beforeSearch {
				t.Fatalf("%s: clicking a non-sortable header changed location.search (%q -> %q)", tc.name, beforeSearch, after)
			}
			if after := stub.queryCount(tc.kind); after != before {
				t.Fatalf("%s: clicking a non-sortable header issued a new request (%d -> %d)", tc.name, before, after)
			}
		})
	}
}

/* ------------------------------------ finding 2: bare arrays are not paged -- */

// TestUI_BareArrayListsNotPaged (round-2 #2) proves a bare-array endpoint never
// invents a page: /users?page=2 shows no "page 2" and sends no page=, and a
// 60-row users list renders a read-only count with no pager.
func TestUI_BareArrayListsNotPaged(t *testing.T) {
	t.Run("page param ignored", func(t *testing.T) {
		stub := newStub()
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		mustDo(t, ctx,
			chromedp.Navigate(srv.URL+"/users?page=2"),
			chromedp.WaitVisible(chromedp.CSS("#content .pagination")),
		)
		txt := evalStr(t, ctx, `document.querySelector('#content .pagination').textContent`)
		if strings.Contains(txt, "page 2") {
			t.Fatalf("a bare-array endpoint invented a page number: %q", txt)
		}
		if !strings.Contains(txt, "rows") {
			t.Fatalf("bare-array pagination must render a read-only row count, got %q", txt)
		}
		if n := evalInt(t, ctx, `document.querySelectorAll('#page-size').length`); n != 0 {
			t.Fatalf("bare-array endpoint rendered a page-size selector (%d)", n)
		}
		if n := evalInt(t, ctx, `document.querySelectorAll('#page-next').length`); n != 0 {
			t.Fatalf("bare-array endpoint rendered Next (%d)", n)
		}
		if stub.querySeen("users", "page=") {
			t.Fatalf("bare-array endpoint sent a page param: %v", stub.queriesFor("users"))
		}
	})

	t.Run("sixty rows stay unpaged", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) { s.userItems = makeUsers(60) })
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/users"), chromedp.WaitVisible(chromedp.CSS("#content .pagination")))
		if !waitFor(t, 3*time.Second, func() bool {
			txt, err := evalStrOK(ctx, `document.querySelector('#content .pagination').textContent`)
			return err == nil && strings.Contains(txt, "60 rows")
		}) {
			got, _ := evalStrOK(ctx, `document.querySelector('#content .pagination').textContent`)
			t.Fatalf("want a read-only \"60 rows\" count, got %q", got)
		}
		if n := evalInt(t, ctx, `document.querySelectorAll('#page-next').length`); n != 0 {
			t.Fatalf("60-row bare array rendered a Next control (%d)", n)
		}
		before := stub.queryCount("users")
		evalStr(t, ctx, `(function(){ var b=document.querySelector('#page-next'); if (b) b.click(); return 'ok'; })()`)
		if after := stub.queryCount("users"); after != before {
			t.Fatalf("clicking a nonexistent pager issued a request (%d -> %d)", before, after)
		}
		if stub.querySeen("users", "page=") {
			t.Fatalf("bare-array endpoint sent a page param: %v", stub.queriesFor("users"))
		}
	})
}

/* ------------------------------- finding 3: alarm clears into the degraded -- */

// TestUI_HealthAlarmThenDegraded (round-2 #3) proves the merged banner is
// ordering-safe: with a degraded boot AND an active alarm the alarm wins, and
// when the alarm clears the banner must fall back to the degraded warning.
// Off the overview screen only refreshHealth drives the banner, so a pre-fix
// applyHealth leaves the stale "Alarm active" text forever.
func TestUI_HealthAlarmThenDegraded(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.overview["unsafe_recovery"] = map[string]any{"degraded": true, "discarded_artifacts": 5}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		onNewDocument(`window.__SQ_CONFIG__ = { refreshInterval: 200, s1RefreshInterval: 200 };`),
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#top-queues tbody tr")),
	)
	bannerHas := func(want string) func() bool {
		return func() bool {
			txt, err := evalStrOK(ctx, `document.getElementById('alarm-banner').textContent`)
			return err == nil && strings.Contains(txt, want)
		}
	}
	if !waitFor(t, 5*time.Second, bannerHas("broker booted degraded")) {
		t.Fatal("degraded banner never rendered")
	}

	// Leave overview so applyDegraded no longer runs and only refreshHealth
	// feeds the banner. Navigate within the SPA so the in-memory degraded
	// state survives.
	evalStr(t, ctx, `(function(){ window.SQ.navigate('/queues'); return 'ok'; })()`)
	mustDo(t, ctx, chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))
	stub.set(func(s *stubAPI) { s.healthStatus = "alarm"; s.healthReason = "memory resource alarm active" })
	if !waitFor(t, 5*time.Second, bannerHas("Alarm active")) {
		t.Fatal("banner did not flip to alarm")
	}
	if txt, _ := evalStrOK(ctx, `document.getElementById('alarm-banner').textContent`); strings.Contains(txt, "degraded") {
		t.Fatalf("alarm must take precedence over the degraded warning: %q", txt)
	}
	stub.set(func(s *stubAPI) { s.healthStatus = "ok"; s.healthReason = "" })
	if !waitFor(t, 5*time.Second, bannerHas("broker booted degraded")) {
		txt, _ := evalStrOK(ctx, `document.getElementById('alarm-banner').textContent`)
		t.Fatalf("after the alarm cleared the banner stayed %q; it must fall back to the degraded warning", txt)
	}
	if txt, _ := evalStrOK(ctx, `document.getElementById('alarm-banner').textContent`); strings.Contains(txt, "Alarm active") {
		t.Fatalf("the cleared alarm must not linger in the banner: %q", txt)
	}
}

/* --------------------------------------- finding 4: toolbar handlers pinned -- */

// TestUI_ToolbarControls (round-2 #4) pins the Apply submit, the page-size
// change and the Prev/Next bounds.
func TestUI_ToolbarControls(t *testing.T) {
	paged := func(s *stubAPI) {
		s.queues = makeQueues(50)
		s.queueTotal = 120
		s.queuePageCount = 3
	}

	t.Run("apply-name-filter", func(t *testing.T) {
		stub := newStub()
		stub.set(paged)
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#sort-select")))
		// The toolbar must report the loader's effective default sort, so an
		// Apply does not silently re-sort (round-2 #7, round-3 #2).
		if v := evalStr(t, ctx, `document.getElementById('sort-select').value`); v != "messages_ready" {
			t.Fatalf("#sort-select reports %q, want messages_ready", v)
		}
		if !evalBool(t, ctx, `document.getElementById('sort-reverse').checked`) {
			t.Fatal("#sort-reverse must be checked for the default descending sort")
		}
		before := stub.queryCount("queues")
		evalStr(t, ctx, `(function(){
		  var inp = document.querySelector('.toolbar [data-filter="name"]');
		  inp.value = 'orders'; inp.dispatchEvent(new Event('input', {bubbles:true}));
		  inp.closest('form').requestSubmit();
		  return 'ok';
		})()`)
		if !waitFor(t, 3*time.Second, func() bool {
			return stub.queryCount("queues") > before && stub.querySeen("queues", "name=orders")
		}) {
			t.Fatalf("Apply did not request name=orders; sent %v", stub.queriesFor("queues"))
		}
		if !stub.querySeen("queues", "page=1") {
			t.Fatalf("Apply must reset to page 1; sent %v", stub.queriesFor("queues"))
		}
	})

	t.Run("page-size-change", func(t *testing.T) {
		stub := newStub()
		stub.set(paged)
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#page-size")))
		before := stub.queryCount("queues")
		evalStr(t, ctx, `(function(){
		  var s = document.getElementById('page-size');
		  s.value = '25'; s.dispatchEvent(new Event('change', {bubbles:true}));
		  return 'ok';
		})()`)
		if !waitFor(t, 3*time.Second, func() bool {
			return stub.queryCount("queues") > before && stub.querySeen("queues", "page_size=25")
		}) {
			t.Fatalf("page-size change did not request page_size=25; sent %v", stub.queriesFor("queues"))
		}
		if !stub.querySeen("queues", "page=1") {
			t.Fatalf("page-size change must reset to page 1; sent %v", stub.queriesFor("queues"))
		}
	})

	t.Run("prev-next-bounds", func(t *testing.T) {
		stub := newStub()
		stub.set(paged)
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues?page=2"), chromedp.WaitVisible(chromedp.CSS("#page-prev")))
		if evalBool(t, ctx, `document.getElementById('page-prev').disabled`) {
			t.Fatal("Prev must be enabled on page 2")
		}
		mustDo(t, ctx, chromedp.Click(chromedp.CSS("#page-prev")))
		if !waitFor(t, 3*time.Second, func() bool {
			s, err := evalStrOK(ctx, `location.search`)
			return err == nil && strings.Contains(s, "page=1")
		}) {
			t.Fatal("Prev did not navigate to page=1")
		}
		if !waitFor(t, 3*time.Second, func() bool {
			disabled, err := chromedp.Run(ctx, chromedp.Evaluate[bool](`(function(){ var b=document.getElementById('page-prev'); return b ? b.disabled : false; })()`))
			return err == nil && disabled
		}) {
			t.Fatal("Prev must be disabled on page 1")
		}

		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues?page=3"), chromedp.WaitVisible(chromedp.CSS("#page-next")))
		if !evalBool(t, ctx, `document.getElementById('page-next').disabled`) {
			t.Fatal("Next must be disabled on the last page")
		}
	})
}

/* ------------------------------- finding 7: the toolbar reports the sort --- */

// TestUI_QueuesDefaultSort (round-2 #7) proves the queues toolbar shows the
// sort the loader actually requests (messages_ready descending), not the first
// alphabetical column.
func TestUI_QueuesDefaultSort(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.queues = makeQueues(50)
		s.queueTotal = 120
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/queues"), chromedp.WaitVisible(chromedp.CSS("#sort-select")))
	if v := evalStr(t, ctx, `document.getElementById('sort-select').value`); v != "messages_ready" {
		t.Fatalf("#sort-select reports %q, want messages_ready", v)
	}
	if !evalBool(t, ctx, `document.getElementById('sort-reverse').checked`) {
		t.Fatal("#sort-reverse must be checked for the default descending sort")
	}
	// The server's queue sort allowlist includes auto_delete/exclusive, which
	// the UI renders as columns, so both headers must be sortable (round-3 #5a).
	for _, col := range []string{"auto_delete", "exclusive"} {
		if n := evalInt(t, ctx, fmt.Sprintf(`document.querySelectorAll('#content th[data-col="%s"].sortable').length`, col)); n != 1 {
			t.Fatalf("queues header %q must be sortable (the server accepts it)", col)
		}
	}
	if !waitFor(t, 3*time.Second, func() bool {
		return stub.querySeen("queues", "sort=messages_ready") && stub.querySeen("queues", "sort_reverse=true")
	}) {
		t.Fatalf("the loader did not request the default sort; sent %v", stub.queriesFor("queues"))
	}

	// First click on the already-default-sorted column must toggle direction,
	// not re-send the default: the route query is the source, falling back to
	// the screen's declared default (round-3 #4).
	mustDo(t, ctx, chromedp.Click(chromedp.CSS(`#content th[data-col="messages_ready"]`)))
	if !waitFor(t, 3*time.Second, func() bool {
		s, err := evalStrOK(ctx, `location.search`)
		return err == nil && strings.Contains(s, "sort_reverse=false")
	}) {
		got, _ := evalStrOK(ctx, `location.search`)
		t.Fatalf("first click on the default sort column did not flip direction: %q", got)
	}
}

/* --------------------------- finding 8: close capability is three-state --- */

// TestUI_CloseConnectionCapabilityUnknown (round-2 #8) proves an unknown
// close_connection capability (missing, or a failed version lookup) disables
// close with an explicit "capability unknown" tooltip instead of looking
// enabled.
func TestUI_CloseConnectionCapabilityUnknown(t *testing.T) {
	t.Run("capability missing", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) { s.caps = map[string]any{"channels": true} })
		assertCloseUnknown(t, stub)
	})
	t.Run("version lookup failed", func(t *testing.T) {
		stub := newStub()
		stub.set(func(s *stubAPI) { s.versionFail = true })
		assertCloseUnknown(t, stub)
	})
}

func assertCloseUnknown(t *testing.T, stub *stubAPI) {
	t.Helper()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections/c1"), chromedp.WaitVisible(chromedp.CSS("#close-connection-btn")))
	if !evalBool(t, ctx, `document.getElementById('close-connection-btn').disabled`) {
		t.Fatal("close must be disabled when the close_connection capability is unknown")
	}
	title := evalStr(t, ctx, `document.getElementById('close-connection-btn').title`)
	if !strings.Contains(strings.ToLower(title), "capability unknown") {
		t.Fatalf("unknown-capability tooltip = %q, want it to name the unknown capability", title)
	}
}

/* ----------------------------------- finding 9: /metrics URL from config -- */

// TestUI_MetricsLink (round-2 #9) proves the metrics link is emitted from
// window.__SQ_CONFIG__.metricsUrl and hidden when unset (the endpoint may live
// on a different listener).
func TestUI_MetricsLink(t *testing.T) {
	t.Run("hidden when unset", func(t *testing.T) {
		stub := newStub()
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#top-queues")))
		// No link at all: not by id, and not any new-tab link whose href ends
		// in /metrics (the pre-round-2 widget had no id).
		if n := evalInt(t, ctx, `document.querySelectorAll('a[target="_blank"][href$="/metrics"], #metrics-link').length`); n != 0 {
			t.Fatalf("metrics link rendered without a configured metricsUrl (%d)", n)
		}
	})
	t.Run("configured href", func(t *testing.T) {
		stub := newStub()
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		mustDo(t, ctx,
			onNewDocument(`window.__SQ_CONFIG__ = { metricsUrl: "/prom/metrics" };`),
			chromedp.Navigate(srv.URL+"/"),
			chromedp.WaitVisible(chromedp.CSS("#metrics-link")),
		)
		if href := evalStr(t, ctx, `document.getElementById('metrics-link').getAttribute('href')`); href != "/prom/metrics" {
			t.Fatalf("metrics link href = %q, want /prom/metrics", href)
		}
		if target := evalStr(t, ctx, `document.getElementById('metrics-link').getAttribute('target')`); target != "_blank" {
			t.Fatalf("metrics link target = %q, want _blank", target)
		}
	})
}
