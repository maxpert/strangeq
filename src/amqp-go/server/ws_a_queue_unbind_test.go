package server

import (
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
)

// WS-A (WI-2): handleQueueUnbind decodes a real queue.unbind (50.50) — which
// has NO no-wait octet — and removes only the binding whose arguments table
// matches the client's, per the AMQP 0-9-1 binding identity.

// newUnbindTestServer copies newTestServer but also returns the storage, so
// the test can inspect bindings directly.
func newUnbindTestServer(t *testing.T) (*Server, interfaces.Storage) {
	t.Helper()
	logger := zap.NewNop()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()

	storageImpl, err := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
	unifiedBroker := NewStorageBrokerAdapter(storageBroker)

	return &Server{
		Addr:             ":0",
		Connections:      make(map[string]*protocol.Connection),
		Log:              logger,
		Broker:           unifiedBroker,
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}, storageImpl
}

// queueUnbindPayload builds the spec wire bytes for queue.unbind (50.50):
// reserved-1 short, queue shortstr, exchange shortstr, routing-key shortstr,
// arguments table — NO no-wait octet.
func queueUnbindPayload(queue, exchange, routingKey string, table []byte) []byte {
	payload := []byte{0x00, 0x00}
	payload = append(payload, byte(len(queue)))
	payload = append(payload, queue...)
	payload = append(payload, byte(len(exchange)))
	payload = append(payload, exchange...)
	payload = append(payload, byte(len(routingKey)))
	payload = append(payload, routingKey...)
	return append(payload, table...)
}

// xMatchKVTable encodes {"x-match":"all","k":<v>} with single-char value v.
// Table length: entry1 (1+7) + (1+4+3) = 16, entry2 (1+1) + (1+4+1) = 8 → 24.
func xMatchKVTable(v byte) []byte {
	return []byte{
		0x00, 0x00, 0x00, 0x18, // table length 24
		0x07, 'x', '-', 'm', 'a', 't', 'c', 'h', 'S', 0x00, 0x00, 0x00, 0x03, 'a', 'l', 'l',
		0x01, 'k', 'S', 0x00, 0x00, 0x00, 0x01, v,
	}
}

func TestHandleQueueUnbind_SpecWireWithArguments(t *testing.T) {
	srv, st := newUnbindTestServer(t)

	if err := srv.Broker.DeclareExchange("hx", "headers", false, false, false, nil); err != nil {
		t.Fatalf("DeclareExchange: %v", err)
	}
	if _, err := srv.Broker.DeclareQueue("q", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}
	if err := srv.Broker.BindQueue("q", "hx", "", map[string]interface{}{"x-match": "all", "k": "v"}); err != nil {
		t.Fatalf("BindQueue: %v", err)
	}

	conn := newPipeConn(t)
	conn.ID = "c1"
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	// 1. Mismatched table (k=w): unbind succeeds but the binding survives.
	payload := queueUnbindPayload("q", "hx", "", xMatchKVTable('w'))
	if err := srv.handleQueueUnbind(conn, 1, payload); err != nil {
		t.Fatalf("handleQueueUnbind(mismatched args): %v", err)
	}
	if _, err := st.GetBinding("q", "hx", ""); err != nil {
		t.Fatalf("binding with different arguments must survive, got: %v", err)
	}

	// 2. Matching table (k=v): the binding is removed.
	payload = queueUnbindPayload("q", "hx", "", xMatchKVTable('v'))
	if err := srv.handleQueueUnbind(conn, 1, payload); err != nil {
		t.Fatalf("handleQueueUnbind(matching args): %v", err)
	}
	if _, err := st.GetBinding("q", "hx", ""); err == nil {
		t.Fatal("binding with matching arguments must be removed")
	}
}

// customUnbindBroker is NOT a *StorageBrokerAdapter: it proves the
// optional-interface assertion gives ANY broker argument-aware unbind.
type customUnbindBroker struct {
	UnifiedBroker
	argsCalls int
	lastArgs  map[string]interface{}
}

func (c *customUnbindBroker) UnbindQueueWithArguments(queue, exchange, key string, args map[string]interface{}) error {
	c.argsCalls++
	c.lastArgs = args
	return c.UnifiedBroker.UnbindQueue(queue, exchange, key)
}

// plainBroker exposes ONLY the UnifiedBroker surface (the embedded
// interface), so the argument-aware method does not promote and the
// triple-only fallback must run.
type plainBroker struct{ UnifiedBroker }

func TestHandleQueueUnbind_CustomBrokerGetsArgumentAwarePath(t *testing.T) {
	srv, st := newUnbindTestServer(t)
	cb := &customUnbindBroker{UnifiedBroker: srv.Broker}
	srv.Broker = cb

	if err := srv.Broker.DeclareExchange("hx", "headers", false, false, false, nil); err != nil {
		t.Fatalf("DeclareExchange: %v", err)
	}
	if _, err := srv.Broker.DeclareQueue("q", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}
	if err := srv.Broker.BindQueue("q", "hx", "", map[string]interface{}{"x-match": "all", "k": "v"}); err != nil {
		t.Fatalf("BindQueue: %v", err)
	}

	conn := newPipeConn(t)
	conn.ID = "c1"
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	payload := queueUnbindPayload("q", "hx", "", xMatchKVTable('v'))
	if err := srv.handleQueueUnbind(conn, 1, payload); err != nil {
		t.Fatalf("handleQueueUnbind: %v", err)
	}
	if cb.argsCalls != 1 {
		t.Fatalf("custom broker's argument-aware unbind must be used, got %d calls", cb.argsCalls)
	}
	if cb.lastArgs["k"] != "v" {
		t.Fatalf("arguments must reach the custom broker verbatim, got %v", cb.lastArgs)
	}
	if _, err := st.GetBinding("q", "hx", ""); err == nil {
		t.Fatal("matching-arguments binding must be removed")
	}
}

func TestHandleQueueUnbind_BrokerWithoutArgumentsAPIFallsBack(t *testing.T) {
	srv, st := newUnbindTestServer(t)
	srv.Broker = plainBroker{UnifiedBroker: srv.Broker}

	if err := srv.Broker.DeclareExchange("hx", "headers", false, false, false, nil); err != nil {
		t.Fatalf("DeclareExchange: %v", err)
	}
	if _, err := srv.Broker.DeclareQueue("q", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}
	if err := srv.Broker.BindQueue("q", "hx", "", nil); err != nil {
		t.Fatalf("BindQueue: %v", err)
	}

	conn := newPipeConn(t)
	conn.ID = "c1"
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	payload := queueUnbindPayload("q", "hx", "", []byte{0x00, 0x00, 0x00, 0x00})
	if err := srv.handleQueueUnbind(conn, 1, payload); err != nil {
		t.Fatalf("handleQueueUnbind: %v", err)
	}
	if _, err := st.GetBinding("q", "hx", ""); err == nil {
		t.Fatal("triple-only fallback must remove the binding")
	}
}

func TestHandleQueueUnbind_EmptyTableStillWorks(t *testing.T) {
	srv, st := newUnbindTestServer(t)

	if err := srv.Broker.DeclareExchange("hx", "headers", false, false, false, nil); err != nil {
		t.Fatalf("DeclareExchange: %v", err)
	}
	if _, err := srv.Broker.DeclareQueue("q", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}
	if err := srv.Broker.BindQueue("q", "hx", "", nil); err != nil {
		t.Fatalf("BindQueue: %v", err)
	}

	conn := newPipeConn(t)
	conn.ID = "c1"
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	payload := queueUnbindPayload("q", "hx", "", []byte{0x00, 0x00, 0x00, 0x00})
	if err := srv.handleQueueUnbind(conn, 1, payload); err != nil {
		t.Fatalf("handleQueueUnbind: %v", err)
	}
	if _, err := st.GetBinding("q", "hx", ""); err == nil {
		t.Fatal("empty-table unbind must remove the nil-arguments binding")
	}
}
