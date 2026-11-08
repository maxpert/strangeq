// Package web serves the StrangeQ management single-page application from an
// embedded, framework-free asset bundle.
//
// The bundle is deliberately self-contained: three files (index.html, app.js,
// styles.css) are compiled into the binary with go:embed, so the management
// surface works air-gapped and in a distroless container with no runtime file
// reads. The SPA talks to the management API over same-origin /api/* at
// runtime; it never imports or links the API package.
//
// Serving contract (spec 02 §1.2):
//   - GET and HEAD only; any other verb is 405 with "Allow: GET, HEAD".
//   - GET /assets/{file} serves the embedded file with a strong content-hash
//     ETag and must-revalidate caching; If-None-Match yields 304.
//   - Every other non-/api path serves the shell with Cache-Control: no-store
//     so SPA deep links survive a hard reload (FR-IA-014).
//   - /api/* is never answered here; it 404s so the API mux (registered more
//     specifically) owns those routes in the composed server.
package web

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"html"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
)

//go:embed assets
var assetsFS embed.FS

// FS returns the embedded UI asset filesystem. It exposes the same bytes that
// Handler serves; callers must not assume the files are readable from disk.
func FS() fs.FS { return assetsFS }

const (
	assetsPathPrefix = "/assets"
	cspPolicy        = "default-src 'self'; script-src 'self'; style-src 'self'; " +
		"object-src 'none'; base-uri 'none'; frame-ancestors 'none'; connect-src 'self'"
)

// asset is one preloaded, pre-hashed embedded file.
type asset struct {
	name        string
	contentType string
	body        []byte
	etag        string
}

// bundle holds the embedded files keyed by their path under assets/.
type bundle struct {
	files map[string]*asset
	index *asset
}

var embedded = loadBundle()

func loadBundle() *bundle {
	b := &bundle{files: make(map[string]*asset, 3)}
	types := map[string]string{
		"index.html": "text/html; charset=utf-8",
		"app.js":     "text/javascript; charset=utf-8",
		"styles.css": "text/css; charset=utf-8",
	}
	for name, ct := range types {
		body, err := fs.ReadFile(assetsFS, "assets/"+name)
		if err != nil {
			// A missing embedded file is a build-time programming error.
			panic("web: embedded asset assets/" + name + " missing: " + err.Error())
		}
		sum := sha256.Sum256(body)
		b.files[name] = &asset{
			name:        name,
			contentType: ct,
			body:        body,
			etag:        `"` + hex.EncodeToString(sum[:]) + `"`,
		}
	}
	b.index = b.files["index.html"]
	return b
}

// Options carries the runtime configuration injected into the SPA shell. The
// zero value serves the shell byte-for-byte as embedded.
type Options struct {
	// MetricsURL, when non-empty, is injected into <head> as a
	// <meta name="sq-config"> element so the SPA can render the /metrics link
	// without an inline <script> (the CSP is script-src 'self').
	MetricsURL string
}

// Handler returns the http.Handler that serves the embedded SPA.
func Handler() http.Handler { return HandlerWithOptions(Options{}) }

// HandlerWithOptions returns the SPA handler with opts applied. The only
// option today injects the metrics URL as a CSP-safe <meta> element; an empty
// option set returns a handler that serves the embedded bytes unchanged.
func HandlerWithOptions(opts Options) http.Handler {
	return &spaHandler{shell: shellForOptions(opts)}
}

// spaHandler serves the (optionally rewritten) shell plus the immutable assets.
type spaHandler struct {
	shell []byte
}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handle(h.shell, w, r)
}

// shellForOptions renders index.html with the injected config, or the embedded
// bytes verbatim when nothing is configured.
func shellForOptions(opts Options) []byte {
	if opts.MetricsURL == "" {
		return embedded.index.body
	}
	cfg, err := json.Marshal(map[string]string{"metricsUrl": opts.MetricsURL})
	if err != nil {
		return embedded.index.body
	}
	// The attribute value is HTML-escaped so a URL (or a future option) cannot
	// break out of the double-quoted attribute; the browser decodes the
	// entities before app.js reads the attribute, so JSON.parse sees the JSON.
	meta := `<meta name="sq-config" content="` + html.EscapeString(string(cfg)) + `">` + "\n"
	// index.html embeds exactly one </head>; the SPA shell is the only thing
	// this rewrites, so a missing marker degrades to the embedded bytes.
	if !bytes.Contains(embedded.index.body, []byte("</head>")) {
		return embedded.index.body
	}
	return bytes.Replace(embedded.index.body, []byte("</head>"), []byte(meta+"</head>"), 1)
}

// AssetHandler is an alias of Handler retained for the frozen interface named
// in spec 02 §1.2/§9.
func AssetHandler() http.Handler { return Handler() }

func handle(shell []byte, w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	head := r.Method == http.MethodHead
	upath := path.Clean(r.URL.Path)
	if upath == "." {
		upath = "/"
	}

	// A request aimed at /assets/ that normalises out of the prefix (e.g.
	// /assets/../secrets) must not fall through to the SPA shell: it is a 404.
	rawAssets := r.URL.Path == assetsPathPrefix || strings.HasPrefix(r.URL.Path, assetsPathPrefix+"/")
	if rawAssets && upath != assetsPathPrefix && !strings.HasPrefix(upath, assetsPathPrefix+"/") {
		http.NotFound(w, r)
		return
	}

	switch {
	case upath == assetsPathPrefix || strings.HasPrefix(upath, assetsPathPrefix+"/"):
		serveAsset(w, r, head, strings.TrimPrefix(upath, assetsPathPrefix+"/"))
	case upath == "/api" || strings.HasPrefix(upath, "/api/"):
		// The API mux owns /api/*; never answer it with the SPA shell.
		http.NotFound(w, r)
	default:
		// SPA fallback: any other path renders the shell so deep links reload.
		serveShell(w, head, shell)
	}
}

func serveAsset(w http.ResponseWriter, r *http.Request, head bool, name string) {
	if !fs.ValidPath(name) || strings.Contains(name, "/") {
		http.NotFound(w, r)
		return
	}
	a, ok := embedded.files[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", a.contentType)
	h.Set("ETag", a.etag)
	h.Set("Cache-Control", "public, max-age=0, must-revalidate")
	h.Set("Content-Length", strconv.Itoa(len(a.body)))
	if etagMatches(r.Header.Get("If-None-Match"), a.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	if !head {
		_, _ = w.Write(a.body)
	}
}

func serveShell(w http.ResponseWriter, head bool, shell []byte) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("Content-Length", strconv.Itoa(len(shell)))
	w.WriteHeader(http.StatusOK)
	if !head {
		_, _ = w.Write(shell)
	}
}

// etagMatches implements the If-None-Match comparison (RFC 9110 §13.1.2) for a
// single strong ETag validator, including the "*" wildcard and weak forms.
func etagMatches(ifNoneMatch, etag string) bool {
	if ifNoneMatch == "" {
		return false
	}
	for _, part := range strings.Split(ifNoneMatch, ",") {
		part = strings.TrimSpace(part)
		if part == "*" {
			return true
		}
		if strings.TrimPrefix(part, "W/") == etag {
			return true
		}
	}
	return false
}

func setSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Security-Policy", cspPolicy)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Frame-Options", "DENY")
}
