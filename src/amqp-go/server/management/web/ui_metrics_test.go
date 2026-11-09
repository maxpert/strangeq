package web

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

/* ------------------------------------------------------------- helpers ---- */

// evalIntOK evaluates an int expression without failing, for polling.
func evalIntOK(ctx context.Context, expr string) int {
	v, err := chromedp.Run(ctx, chromedp.Evaluate[int](expr))
	if err != nil {
		return -1
	}
	return v
}

// evalBoolOK evaluates a bool expression without failing, for polling.
func evalBoolOK(ctx context.Context, expr string) bool {
	v, err := chromedp.Run(ctx, chromedp.Evaluate[bool](expr))
	if err != nil {
		return false
	}
	return v
}

// metricFixture joins Prometheus text lines into a scrape body.
func metricFixture(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func scalarFixture(name, typ, help, value string) string {
	return metricFixture(
		"# HELP "+name+" "+help,
		"# TYPE "+name+" "+typ,
		name+" "+value,
	)
}

// openMetrics navigates to /metrics with an optional pre-document config script
// and waits until at least one family has rendered (the first parse finished).
func openMetrics(t *testing.T, ctx context.Context, url string, hasFamily bool, preDoc string) {
	t.Helper()
	steps := []chromedp.Action[chromedp.Void]{}
	if preDoc != "" {
		steps = append(steps, onNewDocument(preDoc))
	}
	steps = append(steps, chromedp.Navigate(url+"/metrics"))
	if hasFamily {
		steps = append(steps, chromedp.WaitVisible(chromedp.CSS(".metric-family")))
	} else {
		steps = append(steps, chromedp.WaitVisible(chromedp.CSS("#metrics-screen")))
	}
	mustDo(t, ctx, steps...)
}

func tileText(t *testing.T, ctx context.Context, id string) string {
	t.Helper()
	return strings.TrimSpace(evalStr(t, ctx, `document.getElementById('metrics-tile-`+id+`').textContent`))
}

func tileSeverity(t *testing.T, ctx context.Context, id string) string {
	t.Helper()
	return evalStr(t, ctx, `document.querySelector('.card[data-tile="`+id+`"]').dataset.severity`)
}

/* ----------------------------------------------------- present families ---- */

// TestUI_MetricsTab_RendersMatrix (spec test 19): the nav is active, three
// families render, each shows its HELP, type badge and series count, and the
// labelled family renders a table while the unlabelled one renders a single
// value.
func TestUI_MetricsTab_RendersMatrix(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if n := evalInt(t, ctx, `document.querySelectorAll('a[data-nav="metrics"][aria-current="page"]').length`); n != 1 {
		t.Fatalf("metrics nav is not aria-current (%d)", n)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family').length`); n != 3 {
		t.Fatalf("metric-family count = %d, want 3", n)
	}
	alpha := evalStr(t, ctx, `document.querySelector('.metric-family[data-family="amqp_alpha_total"]').textContent`)
	if !strings.Contains(alpha, "Alpha counter") || !strings.Contains(alpha, "counter") || !strings.Contains(alpha, "series") {
		t.Fatalf("alpha family header incomplete: %q", alpha)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family[data-family="amqp_queue_messages_ready"] table.data-table').length`); n != 1 {
		t.Fatalf("labelled family must render a table (%d)", n)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family[data-family="amqp_alpha_total"] .metric-single-value').length`); n != 1 {
		t.Fatalf("unlabelled family must render a single value (%d)", n)
	}
}

// TestUI_MetricsNoFabricatedFamily (spec test 20): a family the endpoint never
// emitted appears nowhere on the page.
func TestUI_MetricsNoFabricatedFamily(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if n := evalInt(t, ctx, `document.querySelectorAll('#metrics-families [data-family="amqp_beta_total"]').length`); n != 0 {
		t.Fatalf("a fabricated family rendered (%d)", n)
	}
	txt := evalStr(t, ctx, `document.getElementById('metrics-families').textContent`)
	if strings.Contains(txt, "amqp_beta_total") {
		t.Fatal("a family absent from the response text appears on the page")
	}
}

// TestUI_MetricsMissingFamilyRendersDash (spec test 21): an absent family's
// tile is an em-dash with severity unknown — never a fabricated zero.
func TestUI_MetricsMissingFamilyRendersDash(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if got := tileText(t, ctx, "wal_fsync_errors"); got != "\u2014" {
		t.Fatalf("missing-family tile = %q, want an em-dash", got)
	}
	if got := tileSeverity(t, ctx, "wal_fsync_errors"); got != "unknown" {
		t.Fatalf("missing-family severity = %q, want unknown", got)
	}
	// Spec D3/R3: an unknown tile carries title="not emitted by this broker".
	if got := evalStr(t, ctx, `document.getElementById('metrics-tile-wal_fsync_errors').title`); got != "not emitted by this broker" {
		t.Fatalf("unknown tile title = %q, want %q", got, "not emitted by this broker")
	}
	// A present tile must NOT carry that title (it was emitted).
	if got := evalStr(t, ctx, `document.getElementById('metrics-tile-wal_write_errors').title`); got != "" {
		t.Fatalf("present tile title = %q, want empty", got)
	}
}

// TestUI_MetricsPresentZeroRendersZero (spec test 22): a family present with
// value 0 renders 0 (ok), the pair to test 21.
func TestUI_MetricsPresentZeroRendersZero(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if got := tileText(t, ctx, "wal_write_errors"); got != "0" {
		t.Fatalf("present-zero tile = %q, want 0", got)
	}
	if got := tileSeverity(t, ctx, "wal_write_errors"); got != "ok" {
		t.Fatalf("present-zero severity = %q, want ok", got)
	}
}

/* ------------------------------------------------------- severity tiers ---- */

// TestUI_MetricsBadTileHighlights (spec test 23): a bad counter drives the
// danger banner and names its tile.
func TestUI_MetricsBadTileHighlights(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = scalarFixture("amqp_wal_write_errors_total", "counter", "WAL write failures", "3")
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if got := tileSeverity(t, ctx, "wal_write_errors"); got != "bad" {
		t.Fatalf("severity = %q, want bad", got)
	}
	notice := evalStr(t, ctx, `document.getElementById('metrics-notice').textContent`)
	if !regexp.MustCompile(`1 vital signal`).MatchString(notice) {
		t.Fatalf("banner does not count 1 bad signal: %q", notice)
	}
	if !strings.Contains(notice, "WAL write errors") {
		t.Fatalf("banner does not name the bad tile: %q", notice)
	}
}

// TestUI_MetricsSeverityClassification (spec test 24 / R6 + audit LOW 14):
// redelivered is warn (noise-grade) and unroutable is warn too — a mandatory
// publish with no matching binding is a normal caller outcome, not a broker
// fault — so the banner counts bad only. A WAL full flush alarms only when it
// fires on the traffic path: in barrier mode the boot/close sites are allowed
// to fire legitimately, and the anchored site exclusion keeps them out of the
// bad count.
func TestUI_MetricsSeverityClassification(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_messages_redelivered_total Redelivered messages",
			"# TYPE amqp_messages_redelivered_total counter",
			"amqp_messages_redelivered_total 7",
			"# HELP amqp_messages_unroutable_total Unroutable messages",
			"# TYPE amqp_messages_unroutable_total counter",
			"amqp_messages_unroutable_total 2",
			"# HELP amqp_wal_full_flush_total WAL full flushes",
			"# TYPE amqp_wal_full_flush_total counter",
			`amqp_wal_full_flush_total{site="group_commit"} 1`,
			`amqp_wal_full_flush_total{site="boot_tail"} 1`,
			`amqp_wal_full_flush_total{site="boot_mkdir"} 1`,
			`amqp_wal_full_flush_total{site="close"} 1`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if got := tileSeverity(t, ctx, "redelivered"); got != "warn" {
		t.Fatalf("redelivered severity = %q, want warn", got)
	}
	if got := tileSeverity(t, ctx, "unroutable"); got != "warn" {
		t.Fatalf("unroutable severity = %q, want warn (a no-route mandatory publish is a caller outcome)", got)
	}
	// Only the traffic-path full flush counts: group_commit is 1 and the boot
	// and close sites are excluded, so the tile reads 1, not 4.
	if got := tileText(t, ctx, "wal_full_flush"); got != "1" {
		t.Fatalf("wal_full_flush = %q, want 1 (only the traffic-path site)", got)
	}
	if got := tileSeverity(t, ctx, "wal_full_flush"); got != "bad" {
		t.Fatalf("wal_full_flush severity = %q, want bad", got)
	}
	notice := evalStr(t, ctx, `document.getElementById('metrics-notice').textContent`)
	if !regexp.MustCompile(`1 vital signal`).MatchString(notice) {
		t.Fatalf("banner must count exactly 1 bad signal: %q", notice)
	}
	if !strings.Contains(notice, "WAL full flushes") {
		t.Fatalf("banner does not name the bad tile: %q", notice)
	}
	if strings.Contains(notice, "Unroutable") {
		t.Fatalf("a warn tile must not be named in the bad banner: %q", notice)
	}

	// A full flush at a boot/close site is legitimate off the traffic path: the
	// tile renders a present zero (ok), never bad and never an unknown dash.
	for _, site := range []string{"boot_tail", "boot_mkdir", "close"} {
		t.Run(site, func(t *testing.T) {
			s2 := newStub()
			s2.set(func(s *stubAPI) {
				s.metricsText = metricFixture(
					"# HELP amqp_wal_full_flush_total WAL full flushes",
					"# TYPE amqp_wal_full_flush_total counter",
					`amqp_wal_full_flush_total{site="`+site+`"} 1`,
				)
			})
			srv2 := appServer(t, s2)
			ctx2, cancel2 := newTab(t)
			defer cancel2()
			openMetrics(t, ctx2, srv2.URL, true, "")
			if got := tileText(t, ctx2, "wal_full_flush"); got != "0" {
				t.Fatalf("a %s flush must not count as a traffic-path flush: tile = %q, want 0", site, got)
			}
			if got := tileSeverity(t, ctx2, "wal_full_flush"); got != "ok" {
				t.Fatalf("a %s flush is legitimate: severity = %q, want ok", site, got)
			}
		})
	}

	// The site matcher is anchored (Prometheus regex semantics): segment_close
	// is a traffic-path flush, so the bare "close" in the exclusion must NOT
	// swallow it.
	t.Run("segment_close is on the traffic path", func(t *testing.T) {
		s3 := newStub()
		s3.set(func(s *stubAPI) {
			s.metricsText = metricFixture(
				"# HELP amqp_wal_full_flush_total WAL full flushes",
				"# TYPE amqp_wal_full_flush_total counter",
				`amqp_wal_full_flush_total{site="segment_close"} 1`,
			)
		})
		srv3 := appServer(t, s3)
		ctx3, cancel3 := newTab(t)
		defer cancel3()
		openMetrics(t, ctx3, srv3.URL, true, "")
		if got := tileText(t, ctx3, "wal_full_flush"); got != "1" {
			t.Fatalf("segment_close must not be excluded by the anchored site matcher: tile = %q, want 1", got)
		}
		if got := tileSeverity(t, ctx3, "wal_full_flush"); got != "bad" {
			t.Fatalf("a segment_close full flush is a traffic-path flush: severity = %q, want bad", got)
		}
	})
}

/* ------------------------------------------------------------- rates ------- */

// TestUI_MetricsRatesFromTwoSamples (spec test 25): with an injected clock the
// displayed rate is exactly Δcounter/Δt, deterministic and sleep-free.
func TestUI_MetricsRatesFromTwoSamples(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = scalarFixture("amqp_messages_published_total", "counter", "Published messages", "100")
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	const clock = `window.__SQ_CONFIG__ = { nowMs: (function(){var t=0;return function(){t+=2000;return t;};})() };`
	openMetrics(t, ctx, srv.URL, true, clock)

	if got := tileText(t, ctx, "pub_rate"); got != "\u2014" {
		t.Fatalf("one-sample rate = %q, want an em-dash", got)
	}
	stub.set(func(s *stubAPI) {
		s.metricsText = scalarFixture("amqp_messages_published_total", "counter", "Published messages", "150")
	})
	for i := 0; i < 2; i++ {
		mustRun(t, ctx, refreshAwait(`(async function(){ await window.SQ.refreshNow(); return 'ok'; })()`))
	}
	if got := tileText(t, ctx, "pub_rate"); got != "25.00/s" {
		t.Fatalf("rate = %q, want 25.00/s", got)
	}
}

// TestUI_MetricsRateUnknownWithOneSample (spec test 26): a single scrape never
// yields 0/s; the rate is unknown.
func TestUI_MetricsRateUnknownWithOneSample(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = scalarFixture("amqp_messages_published_total", "counter", "Published messages", "100")
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	const clock = `window.__SQ_CONFIG__ = { nowMs: (function(){var t=0;return function(){t+=2000;return t;};})() };`
	openMetrics(t, ctx, srv.URL, true, clock)

	if got := tileText(t, ctx, "pub_rate"); got != "\u2014" {
		t.Fatalf("single-sample rate = %q, want an em-dash (never 0/s)", got)
	}
}

/* ------------------------------------------------------- non-finite ------- */

// TestUI_MetricsNonFiniteValue (spec test 27): a non-finite value is rendered
// verbatim in the family table and as an em-dash in a tile.
func TestUI_MetricsNonFiniteValue(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_alpha_total Alpha counter",
			"# TYPE amqp_alpha_total counter",
			"amqp_alpha_total +Inf",
			"# HELP amqp_goroutines_total Goroutines",
			"# TYPE amqp_goroutines_total gauge",
			"amqp_goroutines_total +Inf",
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	cell := evalStr(t, ctx, `document.querySelector('.metric-family[data-family="amqp_alpha_total"] .metric-single-value').textContent`)
	if cell != "+Inf" {
		t.Fatalf("non-finite family value = %q, want +Inf verbatim", cell)
	}
	if got := tileText(t, ctx, "goroutines"); got != "\u2014" {
		t.Fatalf("non-finite tile = %q, want an em-dash", got)
	}
}

/* ------------------------------------------------------------ caps --------- */

// manySeriesFixture builds a labelled gauge family with n series, each in its
// own table row, so the total-series cap can be exercised.
func manySeriesFixture(prefix string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP %s_metric many series\n# TYPE %s_metric gauge\n", prefix, prefix)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%s_metric{queue=\"q%04d\"} %d\n", prefix, i, i)
	}
	return b.String()
}

// TestUI_MetricsTruncationNotice (spec test 28 + B3 LOW #4): over the family
// cap the render is capped at 400; and the series cap raises the notice whenever
// it actually bit, including an overshoot on the final family.
func TestUI_MetricsTruncationNotice(t *testing.T) {
	t.Run("family cap", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < 450; i++ {
			fmt.Fprintf(&b, "# HELP amqp_fam_%03d family %d\n# TYPE amqp_fam_%03d counter\namqp_fam_%03d %d\n", i, i, i, i, i)
		}
		stub := newStub()
		stub.set(func(s *stubAPI) { s.metricsText = b.String() })
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		openMetrics(t, ctx, srv.URL, true, "")

		if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family').length`); n != 400 {
			t.Fatalf("rendered families = %d, want 400", n)
		}
		if hidden := evalBool(t, ctx, `document.getElementById('metrics-truncation').hidden`); hidden {
			t.Fatal("truncation notice is present but hidden")
		}
		if txt := evalStr(t, ctx, `document.getElementById('metrics-truncation').textContent`); !strings.Contains(txt, "400") {
			t.Fatalf("truncation notice does not name the cap: %q", txt)
		}
	})

	// 9x200 + 50 + 200 = 2050 series over 11 families. The final family pushes
	// the running total from 1850 past 2000, so the old family-boundary check
	// alone would miss it; the notice must still appear.
	t.Run("series overshoot", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < 9; i++ {
			b.WriteString(manySeriesFixture(fmt.Sprintf("amqp_a%02d", i), 200))
		}
		b.WriteString(manySeriesFixture("amqp_b00", 50))
		b.WriteString(manySeriesFixture("amqp_c00", 200))
		stub := newStub()
		stub.set(func(s *stubAPI) { s.metricsText = b.String() })
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()

		openMetrics(t, ctx, srv.URL, true, "")

		if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family').length`); n != 11 {
			t.Fatalf("rendered families = %d, want 11", n)
		}
		if hidden := evalBool(t, ctx, `document.getElementById('metrics-truncation').hidden`); hidden {
			t.Fatal("series-cap overshoot did not raise the truncation notice")
		}
		if txt := evalStr(t, ctx, `document.getElementById('metrics-truncation').textContent`); !strings.Contains(txt, "2000") {
			t.Fatalf("truncation notice does not name the series cap: %q", txt)
		}
	})
}

/* ------------------------------------------------------- raw / filter ------ */

// TestUI_MetricsRawView (spec test 29): the toggle makes the raw view
// shareable and shows the verbatim text.
func TestUI_MetricsRawView(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if n := evalInt(t, ctx, `document.querySelectorAll('#metrics-raw pre.payload').length`); n != 1 {
		t.Fatalf("raw <pre> missing (%d)", n)
	}
	mustDo(t, ctx,
		chromedp.Click(chromedp.CSS("#metrics-view-toggle")),
		chromedp.WaitVisible(chromedp.CSS("#metrics-raw pre.payload")),
	)
	if !strings.Contains(evalStr(t, ctx, `location.search`), "view=raw") {
		t.Fatalf("raw view did not set ?view=raw: %q", evalStr(t, ctx, `location.search`))
	}
	raw := evalStr(t, ctx, `document.querySelector('#metrics-raw pre.payload').textContent`)
	if !strings.Contains(raw, "# HELP amqp_alpha_total Alpha counter") {
		t.Fatalf("raw view does not contain the fixture's HELP line: %q", raw)
	}
	if hidden := evalBool(t, ctx, `document.getElementById('metrics-families').hidden`); !hidden {
		t.Fatal("the matrix is not hidden in the raw view")
	}
}

// TestUI_MetricsFilter (spec test 30): a local filter narrows the family list
// and the count names shown/total.
func TestUI_MetricsFilter(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	mustDo(t, ctx,
		chromedp.Click(chromedp.CSS("#metrics-filter")),
		chromedp.SendKeys(chromedp.CSS("#metrics-filter"), "wal"),
	)
	if !waitFor(t, 3*time.Second, func() bool {
		return evalIntOK(ctx, `document.querySelectorAll('.metric-family').length`) == 1
	}) {
		t.Fatalf("filter did not narrow to 1 family (got %d)", evalIntOK(ctx, `document.querySelectorAll('.metric-family').length`))
	}
	if fam := evalStr(t, ctx, `document.querySelector('.metric-family').dataset.family`); !strings.Contains(fam, "wal") {
		t.Fatalf("remaining family = %q, want one containing wal", fam)
	}
	if cnt := evalStr(t, ctx, `document.getElementById('metrics-filter-count').textContent`); !strings.Contains(cnt, "1 of 3") {
		t.Fatalf("filter count = %q, want 1 of 3", cnt)
	}
}

// TestUI_MetricsFilterFocusSurvivesRefresh (spec test 31 / R7): the stable
// skeleton keeps the filter input's identity and focus across a refresh.
func TestUI_MetricsFilterFocusSurvivesRefresh(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var n = document.getElementById('metrics-filter'); n.__t = 1; n.focus(); return 'ok';
	})()`))
	mustRun(t, ctx, refreshAwait(`(async function(){ await window.SQ.refreshNow(); return 'ok'; })()`))

	if got := evalInt(t, ctx, `(document.activeElement && document.activeElement.__t) || 0`); got != 1 {
		t.Fatalf("filter lost focus across refresh (activeElement.__t = %d)", got)
	}
	if same := evalBool(t, ctx, `document.activeElement === document.getElementById('metrics-filter')`); !same {
		t.Fatal("filter input node identity changed across refresh")
	}
}

/* ------------------------------------------------- unavailable / error ---- */

// TestUI_MetricsTelemetryDisabled (spec test 32): a 501 renders the truthful
// telemetry notice and zero families.
func TestUI_MetricsTelemetryDisabled(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.metricsStatus = 501 })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, false, "")

	if !waitFor(t, 4*time.Second, func() bool {
		txt, err := evalStrOK(ctx, `document.getElementById('metrics-notice').textContent`)
		return err == nil && (strings.Contains(txt, "telemetry is disabled") || strings.Contains(txt, "not implemented"))
	}) {
		t.Fatalf("telemetry notice not rendered: %q", evalStr(t, ctx, `document.getElementById('metrics-notice').textContent`))
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family').length`); n != 0 {
		t.Fatalf("a 501 rendered %d families", n)
	}
}

// TestUI_MetricsScrapeError (spec test 33 / R9): a 503 keeps the skeleton,
// renders an error notice, and the header goes stale on the bounded ladder.
func TestUI_MetricsScrapeError(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.metricsStatus = 503 })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, false, "")

	if !waitFor(t, 6*time.Second, func() bool {
		return evalBoolOK(ctx, `document.getElementById('refresh-status').dataset.stale === "true"`)
	}) {
		t.Fatal("refresh status did not go stale after a 503")
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('.metric-family').length`); n != 0 {
		t.Fatalf("a 503 rendered %d families", n)
	}
	if txt, err := evalStrOK(ctx, `document.getElementById('metrics-notice').textContent`); err != nil || strings.TrimSpace(txt) == "" {
		t.Fatalf("503 error notice missing: %q (%v)", txt, err)
	}
}

/* ----------------------------------------------- deep link / pause / a11y -- */

// TestUI_MetricsDeepLinkAndPause (spec test 34): a deep link renders the screen
// and a hidden document stops fetching.
func TestUI_MetricsDeepLinkAndPause(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")
	if n := evalInt(t, ctx, `document.querySelectorAll('a[data-nav="metrics"][aria-current="page"]').length`); n != 1 {
		t.Fatalf("deep link did not mark the nav active (%d)", n)
	}

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
		t.Fatalf("hidden document still fetched: %d -> %d", base, after)
	}

	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  Object.defineProperty(document, 'hidden', { configurable: true, get: function(){ return false; } });
	  Object.defineProperty(document, 'visibilityState', { configurable: true, get: function(){ return 'visible'; } });
	  document.dispatchEvent(new Event('visibilitychange'));
	  return 'ok';
	})()`))
	if !waitFor(t, 4*time.Second, func() bool { return stub.hitCount() > base }) {
		t.Fatal("visible document did not resume fetching")
	}
}

// TestUI_MetricsA11yStateNotColourAlone (spec test 35 / FR-UI-009): every tile
// carries a textual severity, so state is never conveyed by colour alone.
func TestUI_MetricsA11yStateNotColourAlone(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	cards := evalInt(t, ctx, `document.querySelectorAll('.card[data-tile]').length`)
	sevs := evalInt(t, ctx, `document.querySelectorAll('.card[data-tile] .card-sev').length`)
	if cards == 0 || cards != sevs {
		t.Fatalf("cards=%d severity spans=%d; every tile needs a textual severity", cards, sevs)
	}
	bad := evalInt(t, ctx, `Array.prototype.filter.call(document.querySelectorAll('.card[data-tile] .card-sev'), function(s){ return ['ok','warn','bad','\u2014'].indexOf(s.textContent) < 0; }).length`)
	if bad != 0 {
		t.Fatalf("%d tiles carry a severity text outside ok/warn/bad/em-dash", bad)
	}
}

/* ----------------------------------------------------- B3 review LOWs ------ */

// TestUI_MetricsNominalNoticeCountsUnknown (B3 LOW #2): the "no bad signals"
// banner must not overstate health — it names how many tiles are unknown. The
// default fixture emits two tile families (ready=3 and WAL write errors=0), so
// 43 of 45 signals are unknown.
func TestUI_MetricsNominalNoticeCountsUnknown(t *testing.T) {
	stub := newStub()
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	notice := evalStr(t, ctx, `document.getElementById('metrics-notice').textContent`)
	if !strings.Contains(notice, "unknown") {
		t.Fatalf("nominal banner does not disclose unknown signals: %q", notice)
	}
	if !strings.Contains(notice, "43 of 45") {
		t.Fatalf("nominal banner unknown count = %q, want 43 of 45", notice)
	}
	if strings.Contains(notice, "nominal") {
		t.Fatalf("nominal banner still claims everything is nominal: %q", notice)
	}
}

// TestUI_MetricsRawEndpointGated (B3 LOW #5): the raw-endpoint shortcut appears
// only when metricsUrl is configured, and is a plain new-tab link.
func TestUI_MetricsRawEndpointGated(t *testing.T) {
	t.Run("present with metricsUrl", func(t *testing.T) {
		stub := newStub()
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		openMetrics(t, ctx, srv.URL, true, `window.__SQ_CONFIG__ = { metricsUrl: "http://127.0.0.1:9999/metrics" };`)
		if n := evalInt(t, ctx, `document.querySelectorAll('#metrics-raw-endpoint').length`); n != 1 {
			t.Fatalf("raw endpoint link count = %d, want 1", n)
		}
		if got := evalStr(t, ctx, `document.getElementById('metrics-raw-endpoint').getAttribute('href')`); got != "http://127.0.0.1:9999/metrics" {
			t.Fatalf("raw endpoint href = %q", got)
		}
		if got := evalStr(t, ctx, `document.getElementById('metrics-raw-endpoint').getAttribute('target')`); got != "_blank" {
			t.Fatalf("raw endpoint target = %q, want _blank", got)
		}
	})
	t.Run("absent without metricsUrl", func(t *testing.T) {
		stub := newStub()
		srv := appServer(t, stub)
		ctx, cancel := newTab(t)
		defer cancel()
		openMetrics(t, ctx, srv.URL, true, "")
		if n := evalInt(t, ctx, `document.querySelectorAll('#metrics-raw-endpoint, a[target="_blank"][href$="/metrics"]').length`); n != 0 {
			t.Fatalf("a raw /metrics link rendered without metricsUrl (%d)", n)
		}
	})
}

// TestUI_MetricsInvalidPatternStaysUnknown (review MEDIUM): a malformed or
// match-everything regex must degrade the tile to unknown, never fabricate a
// present zero. evalTile reports invalid:true and the rendered tile keeps the
// em-dash, severity unknown, with a title naming the offending pattern.
func TestUI_MetricsInvalidPatternStaysUnknown(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_wal_full_flush_total WAL full flushes",
			"# TYPE amqp_wal_full_flush_total counter",
			`amqp_wal_full_flush_total{site="group_commit"} 5`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	// evalTile is the pure seam: a malformed pattern is a parse-time fact.
	got := mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  var ev = window.SQ.evalTile(window.SQ.state.metrics.model, 'sum(amqp_wal_full_flush_total{site!~"boot["})', {prev:{}, curr:{}});
	  return JSON.stringify({found: ev.found, invalid: !!ev.invalid, pattern: ev.pattern || ''});
	})()`))
	if !strings.Contains(got, `"found":false`) || !strings.Contains(got, `"invalid":true`) || !strings.Contains(got, "boot[") {
		t.Fatalf("evalTile on a malformed pattern = %s, want found:false, invalid:true, pattern boot[", got)
	}

	// Swap the shipped selector for the malformed one, then force a fresh
	// scrape (a changed body defeats the 304) so the real tile re-renders.
	mustRun(t, ctx, chromedp.Evaluate[string](`(function(){
	  for (var i=0;i<window.SQ.VITALS.length;i++){
	    if (window.SQ.VITALS[i].id === 'wal_full_flush') {
	      window.SQ.VITALS[i].sel = 'sum(amqp_wal_full_flush_total{site!~"boot["})';
	      break;
	    }
	  }
	  return 'ok';
	})()`))
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_wal_full_flush_total WAL full flushes",
			"# TYPE amqp_wal_full_flush_total counter",
			`amqp_wal_full_flush_total{site="group_commit"} 6`,
		)
	})
	mustRun(t, ctx, refreshAwait(`(async function(){ await window.SQ.refreshNow(); return 'ok'; })()`))

	if got := tileText(t, ctx, "wal_full_flush"); got != "\u2014" {
		t.Fatalf("invalid-pattern tile = %q, want an em-dash (never a fabricated 0)", got)
	}
	if got := tileSeverity(t, ctx, "wal_full_flush"); got != "unknown" {
		t.Fatalf("invalid-pattern severity = %q, want unknown", got)
	}
	if got := evalStr(t, ctx, `document.getElementById('metrics-tile-wal_full_flush').title`); !strings.Contains(got, "boot[") {
		t.Fatalf("invalid-pattern title = %q, want it to name the pattern", got)
	}
}

// TestUI_MetricsWalFullFlushDurabilityFlush (review MEDIUM): under
// durability: flush every confirm is a full drive-cache flush, so the
// group_commit site is expected to climb and must not alarm. The number still
// renders (nothing is hidden); only the alarm is scoped to barrier mode. The
// barrier default keeps the strict alarm — TestUI_MetricsSeverityClassification
// pins that direction.
func TestUI_MetricsWalFullFlushDurabilityFlush(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_wal_full_flush_total WAL full flushes",
			"# TYPE amqp_wal_full_flush_total counter",
			`amqp_wal_full_flush_total{site="group_commit"} 5`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, `window.__SQ_CONFIG__ = { durability: "flush" };`)

	if got := tileText(t, ctx, "wal_full_flush"); got != "5" {
		t.Fatalf("flush-mode wal_full_flush = %q, want 5 (the count must still render)", got)
	}
	if got := tileSeverity(t, ctx, "wal_full_flush"); got != "ok" {
		t.Fatalf("flush-mode wal_full_flush severity = %q, want ok (a flush-mode confirm is expected)", got)
	}
	notice := evalStr(t, ctx, `document.getElementById('metrics-notice').textContent`)
	if strings.Contains(notice, "WAL full flushes") {
		t.Fatalf("flush mode still alarms on WAL full flushes: %q", notice)
	}
}

// TestUI_MetricsFilteredSumEmptyMatch (review LOW): the tile rule has a third
// outcome — a family present whose label matcher selects none of its series
// renders a present 0 (ok), because a sum over an empty set is a defined zero.
// It must not render the absent-family em-dash, which would be a false claim.
func TestUI_MetricsFilteredSumEmptyMatch(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_management_requests_total Management requests",
			"# TYPE amqp_management_requests_total counter",
			`amqp_management_requests_total{status_class="2"} 3`,
			"# HELP amqp_recovery_faults_total Recovery faults",
			"# TYPE amqp_recovery_faults_total counter",
			`amqp_recovery_faults_total{outcome="benign"} 3`,
			"# HELP amqp_wal_full_flush_total WAL full flushes",
			"# TYPE amqp_wal_full_flush_total counter",
			`amqp_wal_full_flush_total{site="boot_tail"} 5`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	for _, id := range []string{"mgmt_5xx", "recovery_faults", "wal_full_flush"} {
		if got := tileText(t, ctx, id); got != "0" {
			t.Errorf("%s = %q, want 0 (family present, matcher selects no series)", id, got)
		}
		if got := tileSeverity(t, ctx, id); got != "ok" {
			t.Errorf("%s severity = %q, want ok", id, got)
		}
		if got := evalStr(t, ctx, `document.getElementById('metrics-tile-`+id+`').title`); got != "" {
			t.Errorf("%s title = %q, want empty (the family is emitted)", id, got)
		}
	}
}

// TestUI_MetricsFilteredSumAbsentFamilyStaysUnknown (review LOW) is the pair to
// the empty-match case: when the family is absent there is nothing to sum, so
// the tile stays an em-dash with the "not emitted by this broker" title and
// never fabricates a 0.
func TestUI_MetricsFilteredSumAbsentFamilyStaysUnknown(t *testing.T) {
	stub := newStub() // default fixture: mgmt/recovery/wal_full_flush are absent
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	for _, id := range []string{"mgmt_5xx", "recovery_faults", "wal_full_flush"} {
		if got := tileText(t, ctx, id); got != "\u2014" {
			t.Errorf("%s = %q, want an em-dash (an absent family must never fabricate 0)", id, got)
		}
		if got := tileSeverity(t, ctx, id); got != "unknown" {
			t.Errorf("%s severity = %q, want unknown", id, got)
		}
		if got := evalStr(t, ctx, `document.getElementById('metrics-tile-`+id+`').title`); got != "not emitted by this broker" {
			t.Errorf("%s title = %q, want %q", id, got, "not emitted by this broker")
		}
	}
}

// TestUI_MetricsFilteredNonSumStaysUnknown (review LOW) pins the other half of
// the empty-match rule: the present-0 shortcut is gated on a filtered SUM
// (sel.agg === "sum" && sel.filter), so it applies to `sum(<family>{<label>…})`
// only. The shipped shutdown_incomplete tile is a bare metric name plus a
// matcher — a family present whose matcher selects none of its series has no
// aggregate value defined, so the tile stays an em-dash with an unknown
// severity and the "not emitted by this broker" title. This is the case the
// docs previously described as a fabricated-0; the runtime is what it is, and
// this test keeps the prose tied to it.
func TestUI_MetricsFilteredNonSumStaysUnknown(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_shutdown_total Shutdowns",
			"# TYPE amqp_shutdown_total counter",
			`amqp_shutdown_total{result="clean"} 1`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	// The family IS present, but the tile is a bare name + matcher, not a sum.
	if got := tileText(t, ctx, "shutdown_incomplete"); got != "\u2014" {
		t.Fatalf("shutdown_incomplete = %q, want an em-dash (a non-sum selector has no defined aggregate over an empty set)", got)
	}
	if got := tileSeverity(t, ctx, "shutdown_incomplete"); got != "unknown" {
		t.Fatalf("shutdown_incomplete severity = %q, want unknown", got)
	}
	if got := evalStr(t, ctx, `document.getElementById('metrics-tile-shutdown_incomplete').title`); got != "not emitted by this broker" {
		t.Fatalf("shutdown_incomplete title = %q, want %q", got, "not emitted by this broker")
	}
}

// TestUI_MetricsHostileTextIsInert (B3 LOW #5 / SEC-030): a HELP line and a
// label value carrying markup render as inert text — no element from the
// payload is created and no event handler runs.
func TestUI_MetricsHostileTextIsInert(t *testing.T) {
	hostile := "\"><img src=x onerror=window.__pwned=1>"
	labelVal := strings.ReplaceAll(hostile, "\"", "\\\"")
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.metricsText = metricFixture(
			"# HELP amqp_hostile_total "+hostile,
			"# TYPE amqp_hostile_total gauge",
			`amqp_hostile_total{queue="`+labelVal+`"} 1`,
		)
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	openMetrics(t, ctx, srv.URL, true, "")

	if n := evalInt(t, ctx, `document.querySelectorAll('#content img, #content script').length`); n != 0 {
		t.Fatalf("hostile payload created %d element(s)", n)
	}
	if got := evalStr(t, ctx, `String(window.__pwned)`); got != "undefined" {
		t.Fatalf("hostile onerror handler ran: window.__pwned = %s", got)
	}
	txt := evalStr(t, ctx, `document.getElementById('metrics-families').textContent`)
	if !strings.Contains(txt, "onerror=window.__pwned=1") {
		t.Fatalf("hostile text did not render inert: %q", txt)
	}
}
