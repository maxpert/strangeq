package web

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// assetNames are the three files the UI bundle must contain (spec 02 §1.1).
var assetNames = []string{"index.html", "app.js", "styles.css"}

func readAsset(t *testing.T, name string) []byte {
	t.Helper()
	b, err := fs.ReadFile(assetsFS, "assets/"+name)
	if err != nil {
		t.Fatalf("embedded asset assets/%s missing: %v", name, err)
	}
	return b
}

func do(h http.Handler, method, target string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestAssets_EmbeddedAndOffline (FR-UI-001/SEC-033, R-11) proves the bundle is
// present, within the 220 KiB budget, and references no external origin.
func TestAssets_EmbeddedAndOffline(t *testing.T) {
	const maxBytes = 220 * 1024
	total := 0
	for _, name := range assetNames {
		b := readAsset(t, name)
		if len(b) == 0 {
			t.Fatalf("asset %s is empty", name)
		}
		total += len(b)
		for _, bad := range []string{"http://", "https://", "//", "ws://", "wss://", "@import", "@font-face"} {
			if bytes.Contains(b, []byte(bad)) {
				t.Errorf("asset %s contains forbidden external reference %q", name, bad)
			}
		}
	}
	if total > maxBytes {
		t.Errorf("bundle size %d bytes exceeds budget %d bytes", total, maxBytes)
	}

	// index.html may only reference same-origin (root-relative) URLs.
	attrRe := regexp.MustCompile(`(?i)(?:src|href)\s*=\s*"([^"]*)"`)
	for _, m := range attrRe.FindAllStringSubmatch(string(readAsset(t, "index.html")), -1) {
		if !strings.HasPrefix(m[1], "/") {
			t.Errorf("index.html references non-same-origin URL %q", m[1])
		}
	}
}

// TestAssets_NoInlineScriptOrStyle (SEC-031) proves the shell has no inline
// <script>/<style> body and no inline style/event attributes, which the CSP
// (script-src 'self') would otherwise block.
func TestAssets_NoInlineScriptOrStyle(t *testing.T) {
	idx := string(readAsset(t, "index.html"))
	scriptTagRe := regexp.MustCompile(`(?is)<script\b[^>]*>`)
	srcRe := regexp.MustCompile(`(?i)\bsrc\s*=`)
	for _, tag := range scriptTagRe.FindAllString(idx, -1) {
		if !srcRe.MatchString(tag) {
			t.Errorf("index.html contains an inline <script> without src: %s", tag)
		}
	}
	if strings.Contains(strings.ToLower(idx), "<style") {
		t.Error("index.html contains an inline <style> block")
	}
	if regexp.MustCompile(`(?i)\sstyle\s*=`).MatchString(idx) {
		t.Error("index.html contains an inline style= attribute")
	}
	if regexp.MustCompile(`(?i)\son[a-z]+\s*=`).MatchString(idx) {
		t.Error("index.html contains an inline event handler attribute")
	}
}

// TestAssets_ContentTypes checks the per-extension content types from §1.2.
func TestAssets_ContentTypes(t *testing.T) {
	cases := map[string]string{
		"index.html": "text/html; charset=utf-8",
		"app.js":     "text/javascript; charset=utf-8",
		"styles.css": "text/css; charset=utf-8",
	}
	h := Handler()
	for name, want := range cases {
		rec := do(h, http.MethodGet, "/assets/"+name, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /assets/%s = %d, want 200", name, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); got != want {
			t.Errorf("GET /assets/%s Content-Type = %q, want %q", name, got, want)
		}
	}
	// The shell is served as text/html.
	rec := do(h, http.MethodGet, "/", nil)
	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("GET / Content-Type = %q, want text/html; charset=utf-8", got)
	}
}

// TestAssets_ETagAnd304 (FR-UI-111/FR-CONV-064) proves a strong content-hash
// ETag and a 304 on If-None-Match for the static assets.
func TestAssets_ETagAnd304(t *testing.T) {
	h := Handler()
	for _, name := range []string{"app.js", "styles.css"} {
		body := readAsset(t, name)
		sum := sha256.Sum256(body)
		wantETag := `"` + hex.EncodeToString(sum[:]) + `"`

		rec := do(h, http.MethodGet, "/assets/"+name, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /assets/%s = %d, want 200", name, rec.Code)
		}
		etag := rec.Header().Get("ETag")
		if etag != wantETag {
			t.Errorf("GET /assets/%s ETag = %q, want %q", name, etag, wantETag)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "public, max-age=0, must-revalidate" {
			t.Errorf("GET /assets/%s Cache-Control = %q", name, cc)
		}
		if !bytes.Equal(rec.Body.Bytes(), body) {
			t.Errorf("GET /assets/%s body does not match embedded bytes", name)
		}

		rec304 := do(h, http.MethodGet, "/assets/"+name, map[string]string{"If-None-Match": etag})
		if rec304.Code != http.StatusNotModified {
			t.Errorf("GET /assets/%s If-None-Match = %d, want 304", name, rec304.Code)
		}
		if rec304.Body.Len() != 0 {
			t.Errorf("304 for %s carried a body of %d bytes", name, rec304.Body.Len())
		}

		// A stale validator must not yield a 304.
		stale := do(h, http.MethodGet, "/assets/"+name, map[string]string{"If-None-Match": `"deadbeef"`})
		if stale.Code != http.StatusOK {
			t.Errorf("GET /assets/%s with stale If-None-Match = %d, want 200", name, stale.Code)
		}
	}
}

// TestAssets_ShellNoStore proves the shell is never cached so a redeploy is
// not masked (no strong stale validator on index.html).
func TestAssets_ShellNoStore(t *testing.T) {
	rec := do(Handler(), http.MethodGet, "/", nil)
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("shell Cache-Control = %q, want no-store", cc)
	}
	if rec.Header().Get("ETag") != "" {
		t.Error("shell must not carry an ETag")
	}
}

// TestAssets_SPAFallback (FR-IA-014) proves unknown non-/api paths survive a
// hard reload by returning the shell.
func TestAssets_SPAFallback(t *testing.T) {
	h := Handler()
	index := readAsset(t, "index.html")
	for _, target := range []string{"/", "/queues", "/queues/orders", "/connections/x/y", "/about"} {
		rec := do(h, http.MethodGet, target, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", target, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
			t.Errorf("GET %s Content-Type = %q, want text/html", target, got)
		}
		if !bytes.Equal(rec.Body.Bytes(), index) {
			t.Errorf("GET %s body is not the shell", target)
		}
	}
}

// TestAssets_APIRequestsNotServed proves the handler never answers /api/* (the
// API mux owns it); it must not fall back to the SPA shell.
func TestAssets_APIRequestsNotServed(t *testing.T) {
	h := Handler()
	for _, target := range []string{"/api/overview", "/api/queues", "/api"} {
		rec := do(h, http.MethodGet, target, nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404 (must not serve the shell)", target, rec.Code)
		}
		if bytes.Contains(rec.Body.Bytes(), []byte("<html")) {
			t.Errorf("GET %s leaked the SPA shell", target)
		}
	}
}

// TestAssets_MethodNotAllowed (SEC-023) proves non-GET/HEAD verbs are refused
// with an Allow header.
func TestAssets_MethodNotAllowed(t *testing.T) {
	h := Handler()
	for _, target := range []string{"/assets/app.js", "/", "/queues/orders"} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			rec := do(h, method, target, nil)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", method, target, rec.Code)
			}
			if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
				t.Errorf("%s %s Allow = %q, want %q", method, target, allow, "GET, HEAD")
			}
		}
	}
}

// TestAssets_UnknownAssetNotFound proves unknown /assets/ paths are a 404, not
// a shell fallback.
func TestAssets_UnknownAssetNotFound(t *testing.T) {
	rec := do(Handler(), http.MethodGet, "/assets/nope.js", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /assets/nope.js = %d, want 404", rec.Code)
	}
}

// TestAssets_SecurityHeaders (MIL-011/SEC-031/032/035) proves the security
// headers are present on asset, shell and error responses.
func TestAssets_SecurityHeaders(t *testing.T) {
	h := Handler()
	type tc struct {
		method, target string
	}
	for _, c := range []tc{
		{http.MethodGet, "/assets/app.js"},
		{http.MethodGet, "/"},
		{http.MethodPost, "/assets/app.js"},
	} {
		rec := do(h, c.method, c.target, nil)
		hdr := rec.Header()
		if got := hdr.Get("Content-Security-Policy"); !strings.Contains(got, "default-src 'self'") ||
			!strings.Contains(got, "script-src 'self'") || !strings.Contains(got, "frame-ancestors 'none'") {
			t.Errorf("%s %s CSP = %q", c.method, c.target, got)
		}
		if got := hdr.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s %s X-Content-Type-Options = %q", c.method, c.target, got)
		}
		if got := hdr.Get("Referrer-Policy"); got != "no-referrer" {
			t.Errorf("%s %s Referrer-Policy = %q", c.method, c.target, got)
		}
		if got := hdr.Get("X-Frame-Options"); got != "DENY" {
			t.Errorf("%s %s X-Frame-Options = %q", c.method, c.target, got)
		}
	}
}

// TestAssets_HEAD proves HEAD mirrors GET without a body.
func TestAssets_HEAD(t *testing.T) {
	h := Handler()
	body := readAsset(t, "app.js")
	rec := do(h, http.MethodHead, "/assets/app.js", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD /assets/app.js = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD carried a body of %d bytes", rec.Body.Len())
	}
	if cl := rec.Header().Get("Content-Length"); cl != strconv.Itoa(len(body)) {
		t.Errorf("HEAD Content-Length = %q, want %d", cl, len(body))
	}
	head := do(h, http.MethodHead, "/", nil)
	if head.Code != http.StatusOK || head.Body.Len() != 0 {
		t.Errorf("HEAD / = %d with %d body bytes, want 200/0", head.Code, head.Body.Len())
	}
}

// TestAssets_NoFilesystemRead (SEC-091) proves serving is satisfied entirely
// from the embed.FS: it succeeds with an unreadable working directory.
func TestAssets_NoFilesystemRead(t *testing.T) {
	t.Chdir(t.TempDir())
	rec := do(Handler(), http.MethodGet, "/assets/app.js", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /assets/app.js with empty cwd = %d, want 200", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), readAsset(t, "app.js")) {
		t.Error("served bytes differ from the embedded asset")
	}
}

// TestFS_ExposesAssets proves FS() exposes the embedded bundle.
func TestFS_ExposesAssets(t *testing.T) {
	for _, name := range assetNames {
		if _, err := fs.Stat(FS(), "assets/"+name); err != nil {
			t.Errorf("FS() missing assets/%s: %v", name, err)
		}
	}
}
