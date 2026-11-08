package web

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestUI_NoFrameworkOrCDN (FR-UI-001) proves app.js is a dependency-free
// vanilla script: no ES module import from a URL, no dynamic import of a URL,
// no CommonJS require, and no external origin.
func TestUI_NoFrameworkOrCDN(t *testing.T) {
	js := string(readAsset(t, "app.js"))
	for _, bad := range []string{"import(", "require(", "http://", "https://", "cdn.", "@import"} {
		if strings.Contains(js, bad) {
			t.Errorf("app.js contains forbidden dependency marker %q", bad)
		}
	}
	fromRe := regexp.MustCompile(`(?m)\bimport\s+[^\n;]*\bfrom\s*["'][^"']*["']`)
	if fromRe.MatchString(js) {
		t.Error("app.js imports from a module specifier")
	}
}

// TestUI_StubContract proves the SPA still references every API route it
// consumes and that the stub it is developed against answers the documented
// top-level shape. It is a self-consistency check of the UI against its stub,
// not a contract with the real API: the real-mux cross-check is
// TestUI_RoutesMatchAPI plus the lead's composed-server integration test.
func TestUI_StubContract(t *testing.T) {
	routes := []string{
		"/whoami", "/strangeq/session", "/strangeq/version", "/health", "/overview",
		"/queues", "/exchanges", "/connections", "/channels", "/consumers",
		"/bindings", "/nodes", "/users", "/permissions", "/definitions",
	}
	js := string(readAsset(t, "app.js"))
	for _, r := range routes {
		if !strings.Contains(js, `"`+r+`"`) {
			t.Errorf("app.js no longer references the API route %q", r)
		}
	}

	stub := newStub()
	srv := httptest.NewServer(stub)
	defer srv.Close()

	type check struct {
		path    string
		status  int
		keys    []string
		isArray bool
	}
	cases := []check{
		{path: "/api/whoami", status: 200, keys: []string{"name", "capabilities"}},
		{path: "/api/health", status: 200, keys: []string{"status"}},
		{path: "/api/strangeq/version", status: 200, keys: []string{"capabilities", "api_compatibility"}},
		{path: "/api/overview", status: 200, keys: []string{"object_totals", "queue_totals"}},
		{path: "/api/queues", status: 200, keys: []string{"items"}},
		{path: "/api/exchanges", status: 200, keys: []string{"items"}},
		{path: "/api/connections", status: 200, keys: []string{"items"}},
		{path: "/api/channels", status: 200, keys: []string{"items"}},
		{path: "/api/consumers", status: 200, keys: []string{"items"}},
		{path: "/api/nodes", status: 200, isArray: true},
		{path: "/api/users", status: 200, isArray: true},
		{path: "/api/permissions", status: 200, isArray: true},
		{path: "/api/definitions", status: 200, keys: []string{"vhosts", "exchanges", "queues"}},
		{path: "/api/bindings", status: 501, keys: []string{"error", "reason"}},
	}
	for _, c := range cases {
		resp, err := http.Get(srv.URL + c.path)
		if err != nil {
			t.Fatalf("GET %s: %v", c.path, err)
		}
		if resp.StatusCode != c.status {
			resp.Body.Close()
			t.Errorf("GET %s = %d, want %d", c.path, resp.StatusCode, c.status)
			continue
		}
		var body any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			t.Errorf("GET %s: invalid JSON: %v", c.path, err)
			continue
		}
		resp.Body.Close()
		if c.isArray {
			if _, ok := body.([]any); !ok {
				t.Errorf("GET %s: want a JSON array", c.path)
			}
			continue
		}
		obj, ok := body.(map[string]any)
		if !ok {
			t.Errorf("GET %s: want a JSON object", c.path)
			continue
		}
		for _, k := range c.keys {
			if _, present := obj[k]; !present {
				t.Errorf("GET %s: missing key %q", c.path, k)
			}
		}
	}
}

// webConsumedRoutes is the canonical set of API path templates the SPA calls,
// written in the same {placeholder} form the API route table uses. The UI never
// imports the API package, so this list is the UI-side half of the contract
// that TestUI_RoutesMatchAPI checks against the API route-table source.
var webConsumedRoutes = []string{
	"/api/whoami",
	"/api/strangeq/session",
	"/api/strangeq/version",
	"/api/health",
	"/api/overview",
	"/api/queues",
	"/api/queues/{vhost}/{name}",
	"/api/queues/{vhost}/{name}/contents",
	"/api/queues/{vhost}/{name}/get",
	"/api/exchanges",
	"/api/exchanges/{vhost}/{name}",
	"/api/exchanges/{vhost}/{name}/publish",
	"/api/connections",
	"/api/connections/{name}",
	"/api/connections/{name}/channels",
	"/api/channels",
	"/api/consumers",
	"/api/bindings",
	"/api/nodes",
	"/api/nodes/{name}",
	"/api/users",
	"/api/permissions",
	"/api/definitions",
}

// scanAPIRouteSources globs dir/*.go, parses each file, and returns the union
// of string literals beginning "/api/" that appear as arguments to a route
// registration call (get/post/put/del/add/addPublic). Restricting the scan to
// registration call arguments means a stale top-level literal such as
// `var legacy = "/api/queues"` cannot mask a removed registration.
//
// files is the number of .go files matched: zero means the API package is not
// present at all, so the caller may skip; a non-zero files with an empty routes
// set means the route table was renamed/refactored and the contract must be
// updated (the caller fails).
func scanAPIRouteSources(dir string) (routes map[string]bool, files int, err error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, 0, err
	}
	routes = map[string]bool{}
	for _, m := range matches {
		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, m, nil, 0)
		if perr != nil {
			return nil, files, perr
		}
		files++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isRouteRegistration(call.Fun) {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				s, uerr := strconv.Unquote(lit.Value)
				if uerr != nil {
					continue
				}
				if strings.HasPrefix(s, "/api/") {
					routes[s] = true
				}
			}
			return true
		})
	}
	return routes, files, nil
}

// isRouteRegistration reports whether fn names one of the route-registration
// helpers used by buildRouter (a bare ident or an rt.add-style selector).
func isRouteRegistration(fn ast.Expr) bool {
	names := map[string]bool{"get": true, "post": true, "put": true, "del": true, "add": true, "addPublic": true}
	switch f := fn.(type) {
	case *ast.Ident:
		return names[f.Name]
	case *ast.SelectorExpr:
		return names[f.Sel.Name]
	}
	return false
}

// TestUI_RoutesMatchAPI parses every .go file in the parent API package and
// asserts every path template the SPA calls is still registered there, so a
// rename on either side breaks a test. A missing package (no .go at all) skips;
// a present package whose route registrations are gone fails rather than
// silently skipping. The composed real-mux assertion is a lead integration
// item.
func TestUI_RoutesMatchAPI(t *testing.T) {
	routes, files, err := scanAPIRouteSources("..")
	if err != nil {
		t.Fatalf("scan ../*.go: %v", err)
	}
	if skip, cerr := routeContractError(routes, files); cerr != nil {
		t.Fatal(cerr)
	} else if skip {
		t.Skip("management API package not present in this worktree (owned by a separate stream)")
	}
	for _, r := range webConsumedRoutes {
		if !routes[r] {
			t.Errorf("the SPA consumes %q but the API route table no longer registers it", r)
		}
	}
}

// routeContractError decides the outcome of TestUI_RoutesMatchAPI from the scan
// result: no .go files at all means the API package is absent, so skip; a
// present package with no /api registrations means the route table was renamed
// or refactored, so fail; otherwise the contract applies.
func routeContractError(routes map[string]bool, files int) (skip bool, err error) {
	if files == 0 {
		return true, nil
	}
	if len(routes) == 0 {
		return false, fmt.Errorf("%d .go files parsed but no /api route registrations were found: the route table was renamed or refactored and this contract must be updated", files)
	}
	return false, nil
}

// TestScanAPIRouteSources pins the parser's behaviour with temp-dir fixtures:
// an empty directory yields no files (skip), a differently named file with
// registrations is found, a file with no /api literals yields nothing (fatal),
// and a stale non-registration literal does not count as a route.
func TestScanAPIRouteSources(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		routes, files, err := scanAPIRouteSources(t.TempDir())
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if files != 0 || len(routes) != 0 {
			t.Fatalf("want 0 files / 0 routes, got %d / %d", files, len(routes))
		}
		if skip, cerr := routeContractError(routes, files); !skip || cerr != nil {
			t.Fatalf("an empty package must skip: skip=%v err=%v", skip, cerr)
		}
	})
	t.Run("renamed file registers routes", func(t *testing.T) {
		dir := t.TempDir()
		writeGoFixture(t, dir, "zz_custom_routes.go", `package fake

func wire() {
	get("/api/queues")
	rt.add("GET", "/api/exchanges/{vhost}/{name}")
}
`)
		routes, files, err := scanAPIRouteSources(dir)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if files != 1 {
			t.Fatalf("want 1 file, got %d", files)
		}
		if !routes["/api/queues"] || !routes["/api/exchanges/{vhost}/{name}"] {
			t.Fatalf("registrations not found: %v", routes)
		}
		if skip, cerr := routeContractError(routes, files); skip || cerr != nil {
			t.Fatalf("a present package must be checked: skip=%v err=%v", skip, cerr)
		}
	})
	t.Run("no api literals", func(t *testing.T) {
		dir := t.TempDir()
		writeGoFixture(t, dir, "x.go", `package fake

var nope = "/not-api"
`)
		routes, files, err := scanAPIRouteSources(dir)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if files != 1 || len(routes) != 0 {
			t.Fatalf("want 1 file / 0 routes, got %d / %d", files, len(routes))
		}
		if skip, cerr := routeContractError(routes, files); skip || cerr == nil {
			t.Fatalf("a present package with no routes must fail, not skip: skip=%v err=%v", skip, cerr)
		}
	})
	t.Run("stale literal is not a route", func(t *testing.T) {
		dir := t.TempDir()
		writeGoFixture(t, dir, "r.go", `package fake

var legacy = "/api/queues"

func wire() {
	get("/api/exchanges")
}
`)
		routes, _, err := scanAPIRouteSources(dir)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if routes["/api/queues"] {
			t.Fatal("a stale non-registration literal must not count as a registered route")
		}
		if !routes["/api/exchanges"] {
			t.Fatalf("the real registration was missed: %v", routes)
		}
	})
}

func writeGoFixture(t *testing.T, dir, name, src string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
}
