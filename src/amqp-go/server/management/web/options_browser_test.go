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
	mux.Handle("/", HandlerWithOptions(Options{MetricsURL: "/prom/metrics"}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := newTab(t)
	defer cancel()
	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/"),
		chromedp.WaitVisible(chromedp.CSS("#metrics-link")),
	)
	if href := evalStr(t, ctx, `document.getElementById('metrics-link').getAttribute('href')`); href != "/prom/metrics" {
		t.Fatalf("metrics link href = %q, want /prom/metrics (from the injected meta)", href)
	}
	if target := evalStr(t, ctx, `document.getElementById('metrics-link').getAttribute('target')`); target != "_blank" {
		t.Fatalf("metrics link target = %q, want _blank", target)
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
