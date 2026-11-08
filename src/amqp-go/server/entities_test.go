package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newEntityTestServer builds a Server wired to a real StorageBroker over a
// temp-dir storage engine, with the entity counters uninitialised (the caller
// calls InitEntityCounters, exactly as Build will at integration).
func newEntityTestServer(t testing.TB) (*Server, *broker.StorageBroker) {
	t.Helper()
	store, err := storage.NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)

	sb := broker.NewStorageBroker(store, interfaces.EngineConfig{
		MaxQueueBacklog:         52428,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	})

	srv := &Server{
		Log:              zap.NewNop(),
		Broker:           NewStorageBrokerAdapter(sb),
		Config:           config.DefaultConfig(),
		Connections:      make(map[string]*protocol.Connection),
		MetricsCollector: &NoOpMetricsCollector{},
	}
	t.Cleanup(func() {
		sb.Close()
		_ = store.Close()
	})
	return srv, sb
}

// attachConnection inserts a connection with the given channels into the
// server's live registry (bypassing the wire handshake for a unit test).
func attachConnection(srv *Server, connID, user, vhost string, channels ...*protocol.Channel) *protocol.Connection {
	conn := protocol.NewConnection(nil)
	conn.ID = connID
	conn.Username = user
	conn.Vhost = vhost
	for _, ch := range channels {
		conn.Channels.Store(ch.ID, ch)
	}
	srv.Mutex.Lock()
	srv.Connections[connID] = conn
	srv.Mutex.Unlock()
	return conn
}

// --- BOB-11: coherent entity snapshot ---------------------------------------

func TestInitEntityCounters_SeedsFromRegistries(t *testing.T) {
	srv, sb := newEntityTestServer(t)

	// Entities created BEFORE wiring (the boot-recovery case) are not observed;
	// InitEntityCounters must seed them from the registries.
	_, err := sb.DeclareQueue("recovered", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, sb.DeclareExchange("recovered-x", "direct", true, false, false, nil))

	srv.InitEntityCounters()

	got := srv.SnapshotEntityTotals()
	require.EqualValues(t, len(sb.GetQueues()), got.Queues, "recovered queue counted without any observer event")
	require.EqualValues(t, len(sb.GetExchanges()), got.Exchanges)
	require.EqualValues(t, len(sb.GetConsumers()), got.Consumers)
	require.True(t, srv.EntityObserverWired())
}

func TestSnapshotEntityTotals_SeededAndStable(t *testing.T) {
	srv, sb := newEntityTestServer(t)
	_, err := sb.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	srv.InitEntityCounters()

	first := srv.SnapshotEntityTotals()
	second := srv.SnapshotEntityTotals()

	require.Equal(t, first.Generation, second.Generation, "no mutation => same generation")
	require.Equal(t, first, second, "equal generation => identical counts")
	require.EqualValues(t, len(sb.GetQueues()), first.Queues)
}

func TestSnapshotEntityTotals_ChangesWithTopology(t *testing.T) {
	srv, sb := newEntityTestServer(t)
	srv.InitEntityCounters()

	base := srv.SnapshotEntityTotals()

	_, err := sb.DeclareQueue("q1", false, false, false, nil)
	require.NoError(t, err)
	afterDeclare := srv.SnapshotEntityTotals()
	require.EqualValues(t, base.Queues+1, afterDeclare.Queues)
	require.Greater(t, afterDeclare.Generation, base.Generation)

	_, err = sb.DeleteQueue("q1", false, false)
	require.NoError(t, err)
	afterDelete := srv.SnapshotEntityTotals()
	require.EqualValues(t, base.Queues, afterDelete.Queues)
	require.Greater(t, afterDelete.Generation, afterDeclare.Generation)
}

func TestSnapshotEntityTotals_BindingsNil(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	srv.InitEntityCounters()
	require.Nil(t, srv.SnapshotEntityTotals().Bindings, "BOB-4 is unbuilt; bindings must be JSON null")
}

func TestEntityObserverWired_OnlyForSeamBroker(t *testing.T) {
	// A custom UnifiedBroker lacking the SetEntityObserver seam must leave the
	// snapshot incoherent (EntityObserverWired false).
	fake := &Server{
		Log:              zap.NewNop(),
		Broker:           newAckTestBroker(),
		Config:           config.DefaultConfig(),
		Connections:      make(map[string]*protocol.Connection),
		MetricsCollector: &NoOpMetricsCollector{},
	}
	fake.InitEntityCounters()
	require.False(t, fake.EntityObserverWired(), "a broker without the observer seam must not report wired")

	// The production StorageBrokerAdapter implements the seam.
	real, _ := newEntityTestServer(t)
	real.InitEntityCounters()
	require.True(t, real.EntityObserverWired())
}

// --- BOB-11: lock-free channel registry read --------------------------------

func TestChannelViews_LockFreeFields(t *testing.T) {
	srv, sb := newEntityTestServer(t)
	_, err := sb.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	conn := protocol.NewConnection(nil)
	conn.ID = "conn-1"
	ch1 := protocol.NewChannel(1, conn)
	ch2 := protocol.NewChannel(2, conn)
	ch2.FlowActive.Store(false) // channel.flow false
	ch1.ConfirmMode.Store(true)

	require.NoError(t, sb.RegisterConsumer("q", "cid-1", &protocol.Consumer{
		ID: "cid-1", Tag: "t1", Queue: "q", Channel: ch1, PrefetchCount: 1,
		Messages: make(chan *protocol.Delivery, 4),
	}))

	attachConnection(srv, "conn-1", "alice", "/prod", ch1, ch2)
	srv.InitEntityCounters()

	views := srv.ChannelViews()
	require.Len(t, views, 2)

	byNum := map[uint16]ChannelView{}
	for _, v := range views {
		byNum[v.Number] = v
	}

	v1, ok := byNum[1]
	require.True(t, ok)
	require.Equal(t, "conn-1", v1.ConnectionID)
	require.Equal(t, "alice", v1.User)
	require.Equal(t, "/prod", v1.Vhost)
	require.True(t, v1.Confirm)
	require.True(t, v1.FlowActive)
	require.Equal(t, 1, v1.Consumers, "consumers derived from ConsumerViews, not Channel.Consumers")
	require.True(t, v1.HasUnacked)

	v2, ok := byNum[2]
	require.True(t, ok)
	require.False(t, v2.FlowActive)
	require.Equal(t, 0, v2.Consumers)

	// The struct deliberately carries no Channel.Mutex-guarded field.
	_, hasPrefetch := reflect.TypeOf(ChannelView{}).FieldByName("PrefetchCount")
	require.False(t, hasPrefetch, "ChannelView must not expose Channel.PrefetchCount")
	_, hasClosed := reflect.TypeOf(ChannelView{}).FieldByName("Closed")
	require.False(t, hasClosed, "ChannelView must not expose Channel.Closed")
}

func TestChannelViews_DoesNotBlockOnChannelMutex(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	conn := protocol.NewConnection(nil)
	conn.ID = "conn-lock"
	ch := protocol.NewChannel(1, conn)
	attachConnection(srv, "conn-lock", "u", "/", ch)
	srv.InitEntityCounters()

	ch.Mutex.Lock()
	defer ch.Mutex.Unlock()

	done := make(chan struct{})
	go func() {
		srv.ChannelViews()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ChannelViews blocked on Channel.Mutex (forbidden)")
	}
}

func TestSnapshotEntityTotals_DoesNotBlockOnChannelMutex(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	conn := protocol.NewConnection(nil)
	conn.ID = "conn-lock2"
	ch := protocol.NewChannel(1, conn)
	attachConnection(srv, "conn-lock2", "u", "/", ch)
	srv.InitEntityCounters()

	ch.Mutex.Lock()
	defer ch.Mutex.Unlock()

	done := make(chan struct{})
	go func() {
		srv.SnapshotEntityTotals()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SnapshotEntityTotals blocked on Channel.Mutex (forbidden)")
	}
}

// --- race + forbidden-lock gates --------------------------------------------

func TestAccessors_RaceWithLiveTraffic(t *testing.T) {
	srv, sb := newEntityTestServer(t)
	_, err := sb.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	conn := protocol.NewConnection(nil)
	conn.ID = "race-conn"
	ch := protocol.NewChannel(1, conn)
	attachConnection(srv, "race-conn", "u", "/", ch)
	srv.InitEntityCounters()

	consumer := &protocol.Consumer{
		ID: "cid-race", Tag: "t", Queue: "q", Channel: ch,
		Messages: make(chan *protocol.Delivery, 256),
	}
	require.NoError(t, sb.RegisterConsumer("q", "cid-race", consumer))

	stop := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-consumer.Messages:
			case <-stop:
				return
			}
		}
	}()

	const publishes = 400
	const readers = 4
	const iterations = 150

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < publishes; i++ {
			_ = sb.PublishMessage("", "q", &protocol.Message{Exchange: "", RoutingKey: "q", Body: []byte("m")})
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				sb.QueueMessageCounts("q")
				sb.ConsumerViews()
				srv.SnapshotEntityTotals()
				srv.ChannelViews()
			}
		}()
	}

	// Concurrent topology churn exercises the observer deltas + entityGate
	// against the snapshot read.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if _, err := sb.DeclareQueue("churn", false, false, false, nil); err != nil {
				return
			}
			if _, err := sb.DeleteQueue("churn", false, false); err != nil {
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("accessors + live traffic did not complete within the 30s deadline")
	}

	require.NoError(t, sb.UnregisterConsumer("cid-race"))
	close(stop)
	select {
	case <-drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer drain goroutine did not stop")
	}
}

// lockUnexported locks an unexported sync.Mutex field of target (by name) and
// returns its unlock. It uses the reflected field offset so a struct-layout
// change cannot silently point at the wrong field. Test-only.
func lockUnexported(t *testing.T, target interface{}, field string) func() {
	t.Helper()
	v := reflect.ValueOf(target)
	require.Equal(t, reflect.Pointer, v.Kind(), "target must be a pointer")
	f, ok := v.Elem().Type().FieldByName(field)
	require.True(t, ok, "field %q not found on %T", field, target)
	mu := (*sync.Mutex)(unsafe.Add(unsafe.Pointer(v.Pointer()), f.Offset))
	mu.Lock()
	return mu.Unlock
}

func TestForbiddenLockGate_HoldEveryForbiddenLock(t *testing.T) {
	srv, sb := newEntityTestServer(t)
	_, err := sb.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	conn := protocol.NewConnection(nil)
	conn.ID = "lock-conn"
	ch := protocol.NewChannel(1, conn)
	attachConnection(srv, "lock-conn", "u", "/", ch)
	srv.InitEntityCounters()

	limiter := srv.publishLimiter()

	run := func(name string, hold func() func()) {
		t.Run(name, func(t *testing.T) {
			release := hold()
			defer release()

			done := make(chan struct{})
			go func() {
				sb.QueueMessageCounts("q")
				sb.ConsumerViews()
				srv.SnapshotEntityTotals()
				srv.ChannelViews()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Errorf("management accessors blocked while %s is held", name)
			}
		})
	}

	run("channel.Mutex", func() func() { ch.Mutex.Lock(); return ch.Mutex.Unlock })
	run("connection.WriteMutex", func() func() { conn.WriteMutex.Lock(); return conn.WriteMutex.Unlock })
	run("publish-limiter.mu", func() func() { limiter.mu.Lock(); return limiter.mu.Unlock })
	run("channel.wireTagMu", func() func() { return lockUnexported(t, ch, "wireTagMu") })
	run("channel.confirmOrderMu", func() func() { return lockUnexported(t, ch, "confirmOrderMu") })
	run("channel.confirmFlushMu", func() func() { return lockUnexported(t, ch, "confirmFlushMu") })
}

// TestForbiddenLockGate_ASTScan is the mechanical gate behind
// amqp_management_forbidden_lock_acquired_total: the management-facing source
// must never reference a forbidden lock/field/method.
//
// The scan is syntactic (the repo's established source-guard style): the
// unambiguous lock/field names are forbidden on ANY receiver, while the three
// names that also exist on the immutable protocol.Consumer
// (Consumers/Closed/PrefetchCount) are flagged only when the receiver is
// provably a channel/connection — an explicitly annotated parameter/var, a
// type-assertion target, or a conventional channel/connection identifier.
func TestForbiddenLockGate_ASTScan(t *testing.T) {
	paths := []string{"entities.go", "queue_stats.go", "../broker/management_accessors.go"}

	// The management package is owned by a sibling workstream; scan it when it
	// exists (it does not on this branch).
	if entries, err := os.ReadDir("management"); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
				paths = append(paths, filepath.Join("management", e.Name()))
			}
		}
	}

	alwaysForbidden := map[string]bool{
		"GetStats":       true,
		"WriteMutex":     true,
		"wireTagMu":      true,
		"confirmOrderMu": true,
		"confirmFlushMu": true,
	}
	guardedOnChan := map[string]bool{
		"Consumers":     true,
		"Closed":        true,
		"PrefetchCount": true,
	}
	chanLikeNames := map[string]bool{
		"ch": true, "channel": true, "chan": true, "conn": true, "connection": true,
	}

	for _, path := range paths {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		// Identifiers known to hold a channel/connection in this file.
		chanTyped := map[string]bool{}
		markTyped := func(id *ast.Ident, typ ast.Expr) {
			if id != nil && isChanConnType(typ) {
				chanTyped[id.Name] = true
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.Field:
				for _, nm := range d.Names {
					markTyped(nm, d.Type)
				}
			case *ast.AssignStmt:
				for i, rhs := range d.Rhs {
					if i >= len(d.Lhs) {
						break
					}
					switch r := rhs.(type) {
					case *ast.TypeAssertExpr:
						markTyped(asIdent(d.Lhs[i]), r.Type)
					case *ast.UnaryExpr: // &protocol.Channel{...}
						if id := asIdent(d.Lhs[i]); id != nil {
							_ = id
						}
					}
				}
			case *ast.ValueSpec:
				for i, nm := range d.Names {
					if i < len(d.Values) {
						if ta, ok := d.Values[i].(*ast.TypeAssertExpr); ok {
							markTyped(nm, ta.Type)
						}
					}
				}
			}
			return true
		})

		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			name := sel.Sel.Name
			if alwaysForbidden[name] {
				t.Errorf("%s:%d: forbidden selector %q", path, fset.Position(sel.Pos()).Line, name)
				return true
			}
			if guardedOnChan[name] {
				base := rootIdent(sel.X)
				if chanLikeNames[base] || chanTyped[base] || isChanConnSelector(sel.X) {
					t.Errorf("%s:%d: forbidden channel/connection field %q on receiver %q",
						path, fset.Position(sel.Pos()).Line, name, base)
				}
			}
			return true
		})
	}
}

func asIdent(e ast.Expr) *ast.Ident {
	if id, ok := e.(*ast.Ident); ok {
		return id
	}
	return nil
}

func rootIdent(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return rootIdent(x.X)
	case *ast.StarExpr:
		return rootIdent(x.X)
	case *ast.IndexExpr:
		return rootIdent(x.X)
	case *ast.CallExpr:
		return rootIdent(x.Fun)
	}
	return ""
}

func isChanConnSelector(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return sel.Sel.Name == "Channel" || sel.Sel.Name == "Connection"
}

func isChanConnType(e ast.Expr) bool {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "protocol" {
		return false
	}
	return sel.Sel.Name == "Channel" || sel.Sel.Name == "Connection"
}

// BenchmarkSnapshotEntityTotals measures the O(1) coherent snapshot with 10 000
// entity units preloaded. The snapshot reads five counters under entityGate and
// never touches the registries, so it must allocate zero.
//
// Deviation from spec item 24: preloading 10 000 *metadata* queues would cost
// 10 000 metadata fsyncs (~3 minutes). The snapshot's cost is independent of the
// registry, so the counters are seeded directly (same value, no I/O).
func BenchmarkSnapshotEntityTotals(b *testing.B) {
	srv, _ := newEntityTestServer(b)

	srv.entityGate.Lock()
	srv.entityConnections = 10000
	srv.entityChannels = 10000
	srv.entityConsumers = 10000
	srv.entityQueues = 10000
	srv.entityExchanges = 10000
	srv.entityGate.Unlock()

	if allocs := testing.AllocsPerRun(1000, func() { _ = srv.SnapshotEntityTotals() }); allocs != 0 {
		b.Fatalf("SnapshotEntityTotals allocated %.2f allocs/op, want 0", allocs)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = srv.SnapshotEntityTotals()
	}
}
