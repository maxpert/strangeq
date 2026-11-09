package server

import (
	"testing"
)

// These tests pin item ②'s honest half: protocol.Connection.AuthMechanism is
// recorded ONLY when a SASL mechanism actually authenticated the connection.
// When authentication is disabled no SASL verification happens, so reporting
// the offered mechanism would imply a validation that never took place.

// newAuthServerWithFile builds an auth-enabled server backed by the shared test
// auth file (user testuser / secret).
func newAuthServerWithFile(t *testing.T) *Server {
	t.Helper()
	cfg := newAuthTestConfig(t)
	authPath := t.TempDir() + "/auth.json"
	writeTestAuthFile(t, authPath)
	srv, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(authPath).Build()
	if err != nil {
		t.Fatalf("build auth server: %v", err)
	}
	return srv
}

// TestHandleConnectionStartOK_RecordsMechanismOnSuccess: a validated PLAIN
// start-ok records the mechanism that authenticated.
func TestHandleConnectionStartOK_RecordsMechanismOnSuccess(t *testing.T) {
	srv := newAuthServerWithFile(t)
	conn := newLoopbackPipeConn(t)

	if err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00testuser\x00secret"))); err != nil {
		t.Fatalf("valid PLAIN credentials must succeed: %v", err)
	}
	if conn.Username != "testuser" {
		t.Fatalf("username = %q, want testuser", conn.Username)
	}
	if conn.AuthMechanism != "PLAIN" {
		t.Fatalf("AuthMechanism = %q, want PLAIN (the mechanism that authenticated)", conn.AuthMechanism)
	}
}

// TestHandleConnectionStartOK_MechanismEmptyOnFailure: a refused credential
// must not leave a mechanism behind.
func TestHandleConnectionStartOK_MechanismEmptyOnFailure(t *testing.T) {
	srv := newAuthServerWithFile(t)
	conn := newLoopbackPipeConn(t)

	if err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00testuser\x00wrong"))); err == nil {
		t.Fatal("wrong password must be rejected")
	}
	if conn.AuthMechanism != "" {
		t.Fatalf("a refused authentication must not report a mechanism: %q", conn.AuthMechanism)
	}
}

// TestHandleConnectionStartOK_MechanismEmptyOnUnsupportedMechanism: EXTERNAL is
// not registered, so the connection is refused and nothing is recorded.
func TestHandleConnectionStartOK_MechanismEmptyOnUnsupportedMechanism(t *testing.T) {
	srv := newAuthServerWithFile(t)
	conn := newLoopbackPipeConn(t)

	if err := srv.handleConnectionStartOK(conn, startOKPayload(t, "EXTERNAL", []byte("certificate"))); err == nil {
		t.Fatal("an unregistered mechanism must be refused")
	}
	if conn.AuthMechanism != "" {
		t.Fatalf("an unsupported mechanism must not report a mechanism: %q", conn.AuthMechanism)
	}
}

// TestHandleConnectionStartOK_MechanismEmptyWhenAuthDisabled: with authentication
// disabled the server never performs SASL, so the offered mechanism is not a
// validated one and must not be reported.
func TestHandleConnectionStartOK_MechanismEmptyWhenAuthDisabled(t *testing.T) {
	cfg := newAuthTestConfig(t)
	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("build unauthenticated server: %v", err)
	}
	conn := newLoopbackPipeConn(t)

	if err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00guest\x00guest"))); err != nil {
		t.Fatalf("auth-disabled start-ok must succeed: %v", err)
	}
	if conn.Username != "guest" {
		t.Fatalf("username = %q, want guest", conn.Username)
	}
	if conn.AuthMechanism != "" {
		t.Fatalf("authentication disabled must not report the offered mechanism: %q", conn.AuthMechanism)
	}
}
