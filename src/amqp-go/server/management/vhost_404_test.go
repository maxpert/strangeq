package management

import (
	"net/http"
	"testing"
)

// TestUnknownVHost_404BeforeResourceGate (LOW-7, FR-CONV-003/spec §11 test 31):
// an unknown vhost on an entity route must be 404 "unknown vhost" even for a
// principal that holds no permission on it — the route's vhost existence check
// runs before the resource gate, which would otherwise answer 403.
func TestUnknownVHost_404BeforeResourceGate(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"mon","password_hash":"`+secretHash(t)+`",
		"tags":["monitoring"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")}

	for _, target := range []string{
		"/api/queues/other",
		"/api/queues/other/q",
		"/api/exchanges/other/x",
		"/api/queues/other/q/bindings",
		"/api/bindings/other",
	} {
		rec := e.do(http.MethodGet, target, nil, hdr)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GET %s = %d, want 404 (%s)", target, rec.Code, rec.Body.String())
		}
		env := decode[errorEnvelope](t, rec)
		if env.Reason != "unknown vhost" {
			t.Fatalf("GET %s reason = %q, want unknown vhost", target, env.Reason)
		}
	}

	// A route WITHOUT a {vhost} capture still hits the resource gate: "/" is
	// the only vhost the principal has, and /api/queues is a vhost-"/" read.
	ok := e.do(http.MethodGet, "/api/queues", nil, hdr)
	if ok.Code != http.StatusOK {
		t.Fatalf("GET /api/queues = %d, want 200 (%s)", ok.Code, ok.Body.String())
	}
}
