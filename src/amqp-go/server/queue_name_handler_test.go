package server

import (
	"io"
	"net"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
)

// ============================================================================
// Server-generated queue name tests (AMQP 0.9.1 spec compliance)
//
// Spec "default-name" rule in queue.declare:
//   "The queue name MAY be empty, in which case the server MUST create
//    a new queue with a unique generated name and return this to the
//    client in the Declare-Ok method."
//
// Spec "queue-known" rule in queue.bind/unbind/purge/delete:
//   "The client MUST either specify a queue name or have previously
//    declared a queue on the same channel"
//
// Spec queue-name domain doc:
//   "If the client did not declare a queue, and the method needs a
//    queue name, this will result in a 502 (syntax error) channel
//    exception."
// ============================================================================

func newTestServer(t *testing.T) *Server {
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
	}
}

// newTestServer and createTestStorage removed — using storage.NewDisruptorStorageWithDataDir directly.

// newPipeConn creates a protocol.Connection backed by a net.Pipe so that
// response frames can be written without a real TCP connection. A background
// goroutine drains the pipe to prevent blocking writes.
func newPipeConn(t *testing.T) *protocol.Connection {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	go io.Copy(io.Discard, clientConn) // drain response frames
	return protocol.NewConnection(serverConn)
}

func TestQueueDeclareEmptyNameGeneratesServerName(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	// Serialize a queue.declare with empty queue name
	declareMethod := &protocol.QueueDeclareMethod{
		Queue:      "",
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
	}
	payload, err := declareMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.declare: %v", err)
	}

	// Call the handler directly
	err = srv.handleQueueDeclare(conn, 1, payload)
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	// The broker should have created a queue with a server-generated name.
	// Verify by checking the broker's queue list.
	queues := srv.Broker.GetQueues()
	var generatedName string
	for _, q := range queues {
		if strings.HasPrefix(q.Name, "amq.gen.") {
			generatedName = q.Name
			break
		}
	}
	if generatedName == "" {
		t.Error("server should have created a queue with 'amq.gen.' prefix when queue name was empty")
	}
}

func TestQueueDeclareEmptyNameSetsCurrentQueue(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	// Create a channel on the connection
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Serialize a queue.declare with empty queue name
	declareMethod := &protocol.QueueDeclareMethod{
		Queue:      "",
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
	}
	payload, err := declareMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.declare: %v", err)
	}

	err = srv.handleQueueDeclare(conn, 1, payload)
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	// Verify that CurrentQueue was set on the channel
	ch.Mutex.RLock()
	currentQueue := ch.CurrentQueue
	ch.Mutex.RUnlock()

	if currentQueue == "" {
		t.Error("CurrentQueue should be set after queue.declare with empty name")
	}
	if !strings.HasPrefix(currentQueue, "amq.gen.") {
		t.Errorf("CurrentQueue should be a server-generated name with 'amq.gen.' prefix, got %q", currentQueue)
	}
}

func TestQueueDeclareNamedSetsCurrentQueue(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{
		Queue:      "my-named-queue",
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
	}
	payload, err := declareMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.declare: %v", err)
	}

	err = srv.handleQueueDeclare(conn, 1, payload)
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	ch.Mutex.RLock()
	currentQueue := ch.CurrentQueue
	ch.Mutex.RUnlock()

	if currentQueue != "my-named-queue" {
		t.Errorf("CurrentQueue should be 'my-named-queue', got %q", currentQueue)
	}
}

func TestQueueBindEmptyNameResolvesCurrentQueue(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// First declare a queue to set CurrentQueue
	declareMethod := &protocol.QueueDeclareMethod{
		Queue:      "my-queue",
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
	}
	payload, err := declareMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.declare: %v", err)
	}
	err = srv.handleQueueDeclare(conn, 1, payload)
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	// Now bind with empty queue name — should resolve to "my-queue"
	bindMethod := &protocol.QueueBindMethod{
		Queue:      "",
		Exchange:   "",
		RoutingKey: "test-key",
		NoWait:     false,
		Arguments:  map[string]interface{}{},
	}
	bindPayload, err := bindMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.bind: %v", err)
	}

	err = srv.handleQueueBind(conn, 1, bindPayload)
	if err != nil {
		t.Fatalf("handleQueueBind with empty queue name should resolve to current queue: %v", err)
	}
}

func TestQueueBindEmptyNameNoCurrentQueueReturnsError(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Do NOT declare any queue first — CurrentQueue is empty

	// Bind with empty queue name — should fail (502 syntax error per spec)
	bindMethod := &protocol.QueueBindMethod{
		Queue:      "",
		Exchange:   "",
		RoutingKey: "test-key",
		NoWait:     false,
		Arguments:  map[string]interface{}{},
	}
	bindPayload, err := bindMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.bind: %v", err)
	}

	err = srv.handleQueueBind(conn, 1, bindPayload)
	if err == nil {
		t.Error("handleQueueBind with empty queue name and no current queue should return an error (spec: 502 syntax error)")
	}
}

func TestQueueDeleteEmptyNameResolvesCurrentQueue(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// First declare a queue
	declareMethod := &protocol.QueueDeclareMethod{
		Queue:      "delete-me-queue",
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
	}
	payload, err := declareMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.declare: %v", err)
	}
	err = srv.handleQueueDeclare(conn, 1, payload)
	if err != nil {
		t.Fatalf("handleQueueDeclare failed: %v", err)
	}

	// Delete with empty queue name — should resolve to "delete-me-queue"
	deleteMethod := &protocol.QueueDeleteMethod{
		Queue:    "",
		IfUnused: false,
		IfEmpty:  false,
	}
	deletePayload, err := deleteMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.delete: %v", err)
	}

	err = srv.handleQueueDelete(conn, 1, deletePayload)
	if err != nil {
		t.Fatalf("handleQueueDelete with empty queue name should resolve to current queue: %v", err)
	}
}

func TestQueueDeleteEmptyNameNoCurrentQueueReturnsError(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	conn.ID = "test-conn"

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// No queue declared — CurrentQueue is empty
	deleteMethod := &protocol.QueueDeleteMethod{
		Queue:    "",
		IfUnused: false,
		IfEmpty:  false,
	}
	deletePayload, err := deleteMethod.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize queue.delete: %v", err)
	}

	err = srv.handleQueueDelete(conn, 1, deletePayload)
	if err == nil {
		t.Error("handleQueueDelete with empty queue name and no current queue should return an error (spec: 502 syntax error)")
	}
}
