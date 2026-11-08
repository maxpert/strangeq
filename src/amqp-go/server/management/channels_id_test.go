package management

import (
	"net/http"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// TestChannelByID_DeterministicWithDuplicateNumbers pins WS4 review finding 5:
// GET /api/channels/{id} must be deterministic when two connections each own a
// channel numbered 1. The bare integer is accepted ONLY when it is unambiguous;
// the canonical composite "{connection}:{number}" always names one channel.
func TestChannelByID_DeterministicWithDuplicateNumbers(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.injectConnection(t, "conn-a", "u")
	e.injectConnection(t, "conn-b", "u")
	e.srv.Mutex.Lock()
	for _, id := range []string{"conn-a", "conn-b"} {
		conn := e.srv.Connections[id]
		conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))
	}
	e.srv.Mutex.Unlock()

	// The bare number is ambiguous: it must be a deterministic 404 on EVERY
	// call, never whichever connection Go's map order happens to yield.
	for i := 0; i < 20; i++ {
		rec := e.do(http.MethodGet, "/api/channels/1", nil, basicAuthHeader())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("ambiguous bare number must 404 deterministically; call %d got %d (%s)",
				i, rec.Code, truncate(rec.Body.String()))
		}
	}

	// The composite is canonical and resolves to the named connection.
	rec := e.do(http.MethodGet, "/api/channels/conn-a:1", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	got := decode[map[string]any](t, rec)
	if got["connection"] != "conn-a" || got["number"] != float64(1) {
		t.Fatalf("composite id resolved to %v/%v, want conn-a/1", got["connection"], got["number"])
	}
	rec = e.do(http.MethodGet, "/api/channels/conn-b:1", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	if got := decode[map[string]any](t, rec); got["connection"] != "conn-b" {
		t.Fatalf("composite id resolved to connection %v, want conn-b", got["connection"])
	}
}

// TestChannelByID_UnambiguousBareNumber pins the single-connection case: a bare
// number that matches exactly one channel still resolves.
func TestChannelByID_UnambiguousBareNumber(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.injectConnection(t, "only-conn", "u")
	e.srv.Mutex.Lock()
	conn := e.srv.Connections["only-conn"]
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))
	e.srv.Mutex.Unlock()

	rec := e.do(http.MethodGet, "/api/channels/1", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	if got := decode[map[string]any](t, rec); got["connection"] != "only-conn" {
		t.Fatalf("unambiguous bare number resolved to %v, want only-conn", got["connection"])
	}
}
