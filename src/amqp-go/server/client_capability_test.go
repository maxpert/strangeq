package server

import (
	"net"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// TestHandleConnectionStartOK_StoresClientCapabilities exercises the full
// SQ-12 parse->store->query path: the client-advertised capabilities in
// connection.start-ok land on protocol.Connection so emission can later be
// gated on the client's connection.blocked opt-in.
func TestHandleConnectionStartOK_StoresClientCapabilities(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Security.AuthenticationEnabled = false

	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	srv := &Server{
		Log:              zap.NewNop(),
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}

	startOK := &protocol.ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product": "unit-test-client",
			"capabilities": map[string]interface{}{
				"connection.blocked": true,
			},
		},
		Mechanism: "PLAIN",
		Response:  []byte("\x00guest\x00guest"),
		Locale:    "en_US",
	}
	payload, err := startOK.Serialize()
	if err != nil {
		t.Fatalf("Serialize start-ok: %v", err)
	}

	if err := srv.handleConnectionStartOK(conn, payload); err != nil {
		t.Fatalf("handleConnectionStartOK: %v", err)
	}

	if conn.ClientProperties == nil {
		t.Fatal("ClientProperties was not stored on the connection")
	}
	if conn.ClientProperties["product"] != "unit-test-client" {
		t.Errorf("product = %v, want unit-test-client", conn.ClientProperties["product"])
	}
	if !conn.ClientSupportsBlocked() {
		t.Error("ClientSupportsBlocked() = false, want true after advertising the capability")
	}
}

// TestHandleConnectionStartOK_NoBlockedCapability verifies a client that omits
// the connection.blocked capability is not treated as opted-in — SQ-12 must
// never emit blocked frames to it.
func TestHandleConnectionStartOK_NoBlockedCapability(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Security.AuthenticationEnabled = false

	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	srv := &Server{
		Log:              zap.NewNop(),
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}

	startOK := &protocol.ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product": "no-caps-client",
		},
		Mechanism: "PLAIN",
		Response:  []byte("\x00guest\x00guest"),
		Locale:    "en_US",
	}
	payload, err := startOK.Serialize()
	if err != nil {
		t.Fatalf("Serialize start-ok: %v", err)
	}

	if err := srv.handleConnectionStartOK(conn, payload); err != nil {
		t.Fatalf("handleConnectionStartOK: %v", err)
	}

	if conn.ClientSupportsBlocked() {
		t.Error("ClientSupportsBlocked() = true, want false when capability omitted")
	}
}

// TestServerAdvertisesConnectionBlocked verifies the broker now advertises the
// connection.blocked capability in connection.start (flipped false -> true).
func TestServerAdvertisesConnectionBlocked(t *testing.T) {
	cfg := config.DefaultConfig()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	srv := &Server{
		Log:              zap.NewNop(),
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}

	// Read the connection.start frame the server writes.
	done := make(chan *protocol.ConnectionStartMethod, 1)
	errc := make(chan error, 1)
	go func() {
		frame, err := protocol.ReadFrame(clientConn)
		if err != nil {
			errc <- err
			return
		}
		m, err := protocol.ParseConnectionStart(frame.Payload[4:])
		if err != nil {
			errc <- err
			return
		}
		done <- m
	}()

	if err := srv.sendConnectionStart(conn); err != nil {
		t.Fatalf("sendConnectionStart: %v", err)
	}

	select {
	case err := <-errc:
		t.Fatalf("reading connection.start: %v", err)
	case m := <-done:
		caps, ok := m.ServerProperties["capabilities"].(map[string]interface{})
		if !ok {
			t.Fatalf("server properties missing capabilities table: %#v", m.ServerProperties)
		}
		v, ok := caps["connection.blocked"].(bool)
		if !ok || !v {
			t.Errorf("connection.blocked capability = %v (present=%v), want true", v, ok)
		}
	}
}
