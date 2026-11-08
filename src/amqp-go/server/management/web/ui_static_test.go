package web

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
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

// TestUI_RoutesMatchAPI parses the real management API route table source
// (../routes.go) with go/parser and asserts every path template the SPA calls
// still exists there, so a rename on either side breaks a test. The API package
// is owned by a separate stream and may not be present in every worktree; when
// the route-table source is absent the test skips rather than fabricating a
// contract. The composed real-mux assertion is a lead integration item.
func TestUI_RoutesMatchAPI(t *testing.T) {
	const routesSrc = "../routes.go"
	if _, err := os.Stat(routesSrc); err != nil {
		t.Skipf("management API route table %s not present in this worktree (owned by a separate stream): %v", routesSrc, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, routesSrc, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", routesSrc, err)
	}
	have := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		if strings.HasPrefix(s, "/api/") {
			have[s] = true
		}
		return true
	})
	if len(have) == 0 {
		t.Fatalf("%s parsed but no /api route templates were found", routesSrc)
	}
	for _, r := range webConsumedRoutes {
		if !have[r] {
			t.Errorf("the SPA consumes %q but the API route table no longer registers it", r)
		}
	}
}
