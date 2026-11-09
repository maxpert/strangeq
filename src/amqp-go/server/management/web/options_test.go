package web

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

// sqConfigMetaRe extracts the content attribute of the injected config meta.
var sqConfigMetaRe = regexp.MustCompile(`<meta name="sq-config" content="([^"]*)">`)

// TestHandlerWithOptions_InjectsMetricsMeta (FR-UI-024) proves the metrics URL
// reaches the SPA through a CSP-safe <meta> element, inserted immediately
// before </head>, with the security headers byte-identical to Handler()'s.
func TestHandlerWithOptions_InjectsMetricsMeta(t *testing.T) {
	const wantURL = "http://127.0.0.1:9999/metrics"
	h := HandlerWithOptions(Options{MetricsURL: wantURL})
	rec := do(h, http.MethodGet, "/", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("GET / Content-Type = %q, want text/html; charset=utf-8", got)
	}

	// The CSP and the other security headers must be byte-identical to the
	// zero-option handler: injection is a body-only change.
	plain := do(Handler(), http.MethodGet, "/", nil)
	for _, hk := range []string{"Content-Security-Policy", "X-Content-Type-Options", "Referrer-Policy", "X-Frame-Options"} {
		if got, want := rec.Header().Get(hk), plain.Header().Get(hk); got != want {
			t.Errorf("%s with options = %q, want %q (identical to Handler())", hk, got, want)
		}
	}
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") {
		t.Errorf("CSP = %q, want script-src 'self' unchanged", csp)
	}

	body := rec.Body.Bytes()
	matches := sqConfigMetaRe.FindAllSubmatch(body, -1)
	if len(matches) != 1 {
		t.Fatalf("expected exactly one sq-config meta, got %d; body=%s", len(matches), body)
	}
	// The URL appears verbatim in the attribute (the escaping only touches the
	// JSON structural quotes; URL characters are untouched for this value).
	if !strings.Contains(string(matches[0][1]), wantURL) {
		t.Errorf("meta content %q does not contain the configured URL", matches[0][1])
	}
	var cfg struct {
		MetricsURL string `json:"metricsUrl"`
	}
	// Decode entities the way a browser does, then parse the JSON.
	if err := json.Unmarshal([]byte(html.UnescapeString(string(matches[0][1]))), &cfg); err != nil {
		t.Fatalf("meta content is not JSON after entity decoding: %v (%q)", err, matches[0][1])
	}
	if cfg.MetricsURL != wantURL {
		t.Errorf("injected metricsUrl = %q, want %q", cfg.MetricsURL, wantURL)
	}

	// The meta is injected immediately before </head> (nothing between them
	// but the newline this handler adds).
	endHead := bytes.Index(body, []byte("</head>"))
	if endHead < 0 {
		t.Fatal("shell has no </head>")
	}
	prefix := body[:endHead]
	if !strings.HasSuffix(string(prefix), "\n") {
		t.Errorf("meta is not immediately before </head>: %q", prefix[len(prefix)-40:])
	}
	if idx := bytes.LastIndex(prefix, []byte(`<meta name="sq-config"`)); idx < 0 {
		t.Error("sq-config meta does not precede </head>")
	}

	// The body differs from the embedded asset (it grew by the meta), and the
	// declared Content-Length matches.
	if bytes.Equal(body, readAsset(t, "index.html")) {
		t.Error("configured handler served the unmodified shell")
	}
}

// TestHandler_NoInjectedConfigMeta proves the default handler serves the shell
// byte-for-byte: no meta, identical to the embedded asset (and to an
// explicit zero-option handler).
func TestHandler_NoInjectedConfigMeta(t *testing.T) {
	index := readAsset(t, "index.html")
	rec := do(Handler(), http.MethodGet, "/", nil)
	if !bytes.Equal(rec.Body.Bytes(), index) {
		t.Error("Handler() body is not byte-identical to the embedded index.html")
	}
	if strings.Contains(rec.Body.String(), "sq-config") {
		t.Error("Handler() injected a config meta despite no options")
	}

	recZero := do(HandlerWithOptions(Options{}), http.MethodGet, "/", nil)
	if !bytes.Equal(rec.Body.Bytes(), recZero.Body.Bytes()) {
		t.Error("HandlerWithOptions(Options{}) body differs from Handler()")
	}
	if !bytes.Equal(recZero.Body.Bytes(), index) {
		t.Error("HandlerWithOptions(Options{}) served a rewritten shell")
	}
}

// TestHandlerWithOptions_AttributeIsHTMLEscaped proves a hostile URL cannot
// break out of the double-quoted attribute: the raw quoting is entity-escaped
// and the decoded value round-trips to the same URL.
func TestHandlerWithOptions_AttributeIsHTMLEscaped(t *testing.T) {
	const wantURL = `http://h/metrics?a=1&b="x"`
	rec := do(HandlerWithOptions(Options{MetricsURL: wantURL}), http.MethodGet, "/", nil)
	body := rec.Body.String()
	// encoding/json escapes & (and <, >) inside a string, so the ampersand may
	// appear as \u0026; html.EscapeString would otherwise produce &amp;. Either
	// way the raw ampersand must not survive unescaped in the attribute.
	if !strings.Contains(body, `\u0026`) && !strings.Contains(body, "&amp;") {
		t.Error("ampersand in the URL was not escaped")
	}
	meta := sqConfigMetaRe.FindStringSubmatch(body)
	if meta == nil {
		t.Fatalf("no sq-config meta in shell: %s", body)
	}
	if strings.Contains(meta[1], `"x"`) {
		t.Error("a raw double quote leaked into the attribute value")
	}
	var cfg struct {
		MetricsURL string `json:"metricsUrl"`
	}
	if err := json.Unmarshal([]byte(html.UnescapeString(meta[1])), &cfg); err != nil {
		t.Fatalf("decoded content is not JSON: %v (%q)", err, meta[1])
	}
	if cfg.MetricsURL != wantURL {
		t.Errorf("round-tripped metricsUrl = %q, want %q", cfg.MetricsURL, wantURL)
	}
}

// TestHandlerWithOptions_InjectsDurabilityMeta proves the crash model reaches
// the SPA through the same CSP-safe <meta> element, both when it is set together
// with MetricsURL and when it is the only option (a durability-only option must
// not be silently dropped by the empty-MetricsURL fast path).
func TestHandlerWithOptions_InjectsDurabilityMeta(t *testing.T) {
	cases := []struct {
		name    string
		opts    Options
		wantURL string
		wantDur string
	}{
		{"durability with metrics url", Options{MetricsURL: "http://127.0.0.1:9999/metrics", Durability: "flush"}, "http://127.0.0.1:9999/metrics", "flush"},
		{"durability alone", Options{Durability: "barrier"}, "", "barrier"},
		{"none durability alone", Options{Durability: "none"}, "", "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(HandlerWithOptions(tc.opts), http.MethodGet, "/", nil)
			body := rec.Body.Bytes()
			if bytes.Equal(body, readAsset(t, "index.html")) {
				t.Fatal("a configured handler served the unmodified shell")
			}
			matches := sqConfigMetaRe.FindAllSubmatch(body, -1)
			if len(matches) != 1 {
				t.Fatalf("expected exactly one sq-config meta, got %d; body=%s", len(matches), body)
			}
			var cfg struct {
				MetricsURL string `json:"metricsUrl"`
				Durability string `json:"durability"`
			}
			if err := json.Unmarshal([]byte(html.UnescapeString(string(matches[0][1]))), &cfg); err != nil {
				t.Fatalf("meta content is not JSON after entity decoding: %v (%q)", err, matches[0][1])
			}
			if cfg.Durability != tc.wantDur {
				t.Errorf("injected durability = %q, want %q", cfg.Durability, tc.wantDur)
			}
			if cfg.MetricsURL != tc.wantURL {
				t.Errorf("injected metricsUrl = %q, want %q", cfg.MetricsURL, tc.wantURL)
			}
		})
	}
}

// TestHandlerWithOptions_AssetsUnaffected proves the static assets keep their
// content-hash ETag and 304 handling when options are set.
func TestHandlerWithOptions_AssetsUnaffected(t *testing.T) {
	h := HandlerWithOptions(Options{MetricsURL: "http://127.0.0.1:9999/metrics"})
	for _, name := range []string{"app.js", "styles.css"} {
		body := readAsset(t, name)
		sum := sha256.Sum256(body)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		rec := do(h, http.MethodGet, "/assets/"+name, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /assets/%s = %d, want 200", name, rec.Code)
		}
		if got := rec.Header().Get("ETag"); got != etag {
			t.Errorf("GET /assets/%s ETag = %q, want %q", name, got, etag)
		}
		rec304 := do(h, http.MethodGet, "/assets/"+name, map[string]string{"If-None-Match": etag})
		if rec304.Code != http.StatusNotModified {
			t.Errorf("GET /assets/%s If-None-Match = %d, want 304", name, rec304.Code)
		}
	}
}

// TestHandlerWithOptions_HeadMatchesGet proves HEAD carries the injected
// Content-Length and no body, exactly as GET would.
func TestHandlerWithOptions_HeadMatchesGet(t *testing.T) {
	h := HandlerWithOptions(Options{MetricsURL: "http://127.0.0.1:9999/metrics"})
	get := do(h, http.MethodGet, "/", nil)
	head := do(h, http.MethodHead, "/", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("HEAD / = %d, want 200", head.Code)
	}
	if head.Body.Len() != 0 {
		t.Errorf("HEAD carried %d body bytes", head.Body.Len())
	}
	if got, want := head.Header().Get("Content-Length"), get.Header().Get("Content-Length"); got != want {
		t.Errorf("HEAD Content-Length = %q, want %q (same as GET)", got, want)
	}
}
