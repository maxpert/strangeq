package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chromedp/chromedp"
)

// TestUI_MetricsLinkFromInjectedMeta (FR-UI-024) proves the SPA renders the
// /metrics link from the server-injected <meta name="sq-config"> element, with
// no window.__SQ_CONFIG__ seam at all: the shell is served by
// HandlerWithOptions, exactly as the composed management listener serves it.
func TestUI_MetricsLinkFromInjectedMeta(t *testing.T) {
	stub := newStub()
	mux := http.NewServeMux()
	mux.Handle("/api/", stub)
	mux.Handle("/", HandlerWithOptions(Options{MetricsURL: "http://127.0.0.1:9999/prom/metrics"}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := newTab(t)
	defer cancel()
	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#metrics-link")),
	)
	if href := evalStr(t, ctx, `document.getElementById('metrics-link').getAttribute('href')`); href != "http://127.0.0.1:9999/prom/metrics" {
		t.Fatalf("metrics link href = %q, want the injected absolute URL", href)
	}
	if target := evalStr(t, ctx, `document.getElementById('metrics-link').getAttribute('target')`); target != "_blank" {
		t.Fatalf("metrics link target = %q, want _blank", target)
	}
}

// TestUI_MetricsLinkRejectsNonHTTPScheme (FR-UI-024) proves a metricsUrl that is
// not an absolute http(s) URL is never promoted into an href: a hostile
// javascript: value (or a scheme-relative //evil.example) yields no link.
func TestUI_MetricsLinkRejectsNonHTTPScheme(t *testing.T) {
	for _, bad := range []string{"javascript:alert(1)", "data:text/html,<script>", "//evil.example/metrics", "/metrics"} {
		t.Run(bad, func(t *testing.T) {
			stub := newStub()
			mux := http.NewServeMux()
			mux.Handle("/api/", stub)
			mux.Handle("/", HandlerWithOptions(Options{MetricsURL: bad}))
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			ctx, cancel := newTab(t)
			defer cancel()
			mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#top-queues")))
			if n := evalInt(t, ctx, `document.querySelectorAll('#metrics-link, a[target="_blank"][href$="/metrics"]').length`); n != 0 {
				t.Fatalf("a non-http(s) metricsUrl (%q) rendered a link (%d)", bad, n)
			}
		})
	}
}

// TestUI_NoMetricsLinkWithoutConfig proves the link is absent when no
// metricsUrl is configured on either path (the endpoint may live on a
// different listener), so the injection never manufactures a dead link.
func TestUI_NoMetricsLinkWithoutConfig(t *testing.T) {
	stub := newStub()
	mux := http.NewServeMux()
	mux.Handle("/api/", stub)
	mux.Handle("/", Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := newTab(t)
	defer cancel()
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/"), chromedp.WaitVisible(chromedp.CSS("#top-queues")))
	if n := evalInt(t, ctx, `document.querySelectorAll('#metrics-link, a[target="_blank"][href$="/metrics"]').length`); n != 0 {
		t.Fatalf("metrics link rendered without configuration (%d)", n)
	}
}
