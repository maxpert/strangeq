package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
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

// TestUI_EndpointMapMatchesSpec guards the UI against API drift: every route
// the SPA consumes must still be present, and the API must answer with the
// documented top-level shape. The real management mux belongs to spec 01 and is
// not part of this package, so the shape check runs against the same stub the
// browser tests use; a rename on either side breaks this test.
func TestUI_EndpointMapMatchesSpec(t *testing.T) {
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
