package server

import (
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
)

// newProcessCompleteMessageBenchServer builds a Server backed by real
// in-memory storage with a Nop logger (see basic_get_purge_test.go's
// newGetPurgeTestServer for the *testing.T equivalent — this one takes
// testing.TB's *testing.B specifically and skips the network listener,
// TransactionManager, etc. that processCompleteMessage's plain-publish path
// never touches). A large ring buffer keeps every published message resident
// in memory for the whole benchmark: this bench never drains its queue, and
// storage's actual default (DisruptorStorage.DefaultRingBufferSize =
// 1024*256, 80% spill = 209,715 messages) sits well within reach of the
// large -benchtime this benchmark needs (see BenchmarkProcessCompleteMessage's
// doc comment) — crossing it mid-run would silently switch the back half of
// the run onto the synchronous WAL group-commit path instead of the pure
// in-memory ring, which is both a correctness-of-measurement problem (only
// LATER iterations pay a completely different cost) and, under real disk
// contention, was observed to make the whole run stall for minutes waiting
// on WAL batch commits. NewDisruptorStorageWithEngineConfig (not
// NewDisruptorStorageWithDataDir, which ignores cfg.Engine entirely and
// always uses the default) is what actually wires RingBufferSize through.
func newProcessCompleteMessageBenchServer(b *testing.B) *Server {
	b.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = b.TempDir()
	cfg.Engine.RingBufferSize = 1 << 21 // power of 2; ~1.67M-message spill threshold at 80%

	storageImpl, err := storage.NewDisruptorStorageWithEngineConfig(cfg.Storage.Path, storage.DefaultCheckpointInterval, cfg.GetEngine())
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	b.Cleanup(func() { _ = storageImpl.Close() })

	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
	unifiedBroker := NewStorageBrokerAdapter(storageBroker)

	return &Server{
		Addr:             ":0",
		Connections:      make(map[string]*protocol.Connection),
		Log:              zap.NewNop(),
		Broker:           unifiedBroker,
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}
}

// BenchmarkProcessCompleteMessage drives server.processCompleteMessage
// directly — the frame-processing layer a publish actually goes through
// (basic_handlers.go:314), one level above broker.PublishMessage.
// broker/bench_test.go's publish benchmarks call PublishMessage in isolation
// and never reach this layer, so a gate check inserted at the top of
// processCompleteMessage (the SQ-12 alarm gate is planned to land here) would
// otherwise be defended only by the noisier root-package versus benchmarks
// (real TCP round trips). This exercises the plain publish case — no
// confirm mode, no mandatory flag, no transaction, no per-message
// Expiration — the zero-cost path that must stay one atomic load plus one
// not-taken branch once that gate lands.
//
// conn is built with a nil net.Conn: NewConnection documents this as
// supported for tests that never read/write frames, and this scenario
// (non-confirm, non-mandatory, no-route-error-free publish) never touches
// conn.Conn, so there is no listener, pipe, or drain goroutine to add
// scheduling noise to the measurement.
func BenchmarkProcessCompleteMessage(b *testing.B) {
	srv := newProcessCompleteMessageBenchServer(b)

	const queueName = "bench-process-complete"
	if _, err := srv.Broker.DeclareQueue(queueName, false, false, false, nil); err != nil {
		b.Fatalf("DeclareQueue: %v", err)
	}

	conn := protocol.NewConnection(nil)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	body := []byte("bench-process-complete-message-body")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Acquire/release through the real pools (protocol.Get*/Put*), the
		// same sequence processHeaderFrame/processBodyFrame use around every
		// real call to processCompleteMessage (basic_handlers.go). A
		// benchmark that instead allocates a fresh
		// PendingMessage/BasicPublishMethod/ContentHeader per iteration
		// measures its own harness overhead, not the hot path: those three
		// struct literals are exactly the "3 allocs/op, 544 B/op" this
		// benchmark used to report before switching to the pools, none of
		// it attributable to processCompleteMessage itself.
		method := protocol.GetBasicPublishMethod()
		method.RoutingKey = queueName

		hdr := protocol.GetContentHeader()
		hdr.BodySize = uint64(len(body))

		pendingMsg := protocol.GetPendingMessage()
		pendingMsg.Method = method
		pendingMsg.Header = hdr
		pendingMsg.Body = body
		pendingMsg.Received = uint64(len(body))

		err := srv.processCompleteMessage(conn, 1, pendingMsg)

		protocol.PutBasicPublishMethod(pendingMsg.Method)
		protocol.PutContentHeader(pendingMsg.Header)
		protocol.PutPendingMessage(pendingMsg)

		if err != nil {
			b.Fatal(err)
		}
	}
}
