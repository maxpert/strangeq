//go:build conformance

// Package main / conformance suite.
//
// This is the Wave-2 fidelity referee (design doc "W7"). ONE set of amqp091-go
// assertions runs against whichever broker AMQP_TARGET selects, so both are held
// to byte-identical wire expectations:
//
//	# embedded StrangeQ (default):
//	go test -tags=conformance -run TestConformance . -v
//	# real RabbitMQ 4.x in docker on localhost:5672:
//	AMQP_TARGET=rabbitmq go test -tags=conformance -run TestConformance . -v
//
// Method: TDD-for-conformance. Every expected wire value was captured against
// real RabbitMQ 4.3.2 FIRST (see the LOCKED-* constants below and w7-report.md),
// then asserted against embedded StrangeQ. Where StrangeQ genuinely diverges the
// assertion is target-aware (asserts RabbitMQ's truth on rabbitmq, StrangeQ's
// documented behaviour on strangeq) and the divergence is logged — a documented
// xfail, never a false green.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/storage"
)

// ----------------------------------------------------------------------------
// LOCKED wire values — captured against real RabbitMQ 4.3.2 (rabbitmq:4-management
// in docker) on 2026-07-07. These are the byte contracts the suite holds BOTH
// brokers to. Re-lock by running `AMQP_TARGET=rabbitmq go test -tags=conformance`
// against a fresh rabbitmq:4.x and reading the DIVERGENCE / LOCK log lines.
// ----------------------------------------------------------------------------

const (
	// x-death reasons (RabbitMQ rabbit_dead_letter.erl; StrangeQ DeadLetterReason).
	reasonExpired  = "expired"
	reasonRejected = "rejected"
	reasonMaxLen   = "maxlen"

	// connection.blocked reason shortstrs. RabbitMQ builds these from the active
	// alarm set; the disk string is what a `set_disk_free_limit` alarm produces.
	// Locked against RabbitMQ 4.3.2 (rabbitmq:4-management) on 2026-07-07 — see
	// w7-report.md §7. The combined string "low on disk & memory" is an
	// EMPIRICALLY captured byte value (RabbitMQ joins over the active alarm SET;
	// the element order is an internal representation detail, not a guaranteed
	// sort), NOT "low on memory & low on disk". StrangeQ's alarmReason() was FIXED
	// in W7 to match, so the two are equal.
	lockedRMQReasonMem      = "low on memory"
	lockedRMQReasonDisk     = "low on disk"
	lockedRMQReasonMemDisk  = "low on disk & memory"
	strangeQReasonMem       = "low on memory"
	strangeQReasonDisk      = "low on disk"
	strangeQReasonMemDisk   = "low on disk & memory"
	dockerRabbitContainer   = "sq-conformance-rmq"
	rabbitCtlRestoreMemHigh = "0.4"
	rabbitCtlRestoreDisk    = "50MB"
)

// ----------------------------------------------------------------------------
// Target selection + divergence bookkeeping
// ----------------------------------------------------------------------------

func confTarget() string {
	if os.Getenv("AMQP_TARGET") == "rabbitmq" {
		return "rabbitmq"
	}
	return "strangeq"
}

func targetIsRabbit() bool { return confTarget() == "rabbitmq" }

// expectByTarget returns the value the ACTIVE target must produce. When the two
// operands differ, the case is a DOCUMENTED DIVERGENCE (both values live here in
// source, so the divergence is legible and green on both brokers); when equal it
// is proven parity. Divergences are logged so a -v run enumerates them.
func expectByTarget[T any](t *testing.T, label string, rabbit, strangeq T) T {
	t.Helper()
	if !reflect.DeepEqual(rabbit, strangeq) {
		t.Logf("DIVERGENCE[%s]: rabbitmq=%v strangeq=%v (target=%s)", label, rabbit, strangeq, confTarget())
	}
	if targetIsRabbit() {
		return rabbit
	}
	return strangeq
}

// lockLog records a value we captured from the active broker for the report.
func lockLog(t *testing.T, label string, v any) {
	t.Helper()
	t.Logf("LOCK[%s target=%s]: %#v", label, confTarget(), v)
}

// ----------------------------------------------------------------------------
// Unique naming — RabbitMQ persists queues/exchanges across a container's life,
// so every case uses collision-proof names and deletes what it declares.
// ----------------------------------------------------------------------------

var confNameCounter atomic.Int64

func uniqueName(prefix string) string {
	return fmt.Sprintf("cf.%s.%d", prefix, confNameCounter.Add(1))
}

// ----------------------------------------------------------------------------
// confBroker: dual-target broker handle. Embedded StrangeQ owns its storage
// explicitly so restart() can Close() and reopen the SAME path (durable
// round-trip). RabbitMQ is the live container; restart() is a no-op there (a
// durable queue survives in the running broker).
// ----------------------------------------------------------------------------

type confBroker struct {
	t        *testing.T
	target   string
	uri      string
	storeDir string

	// embedded-only
	srv *server.Server
	st  interfaces.Storage
	cfg *config.AMQPConfig
}

// newConfBroker builds a plain dual-target broker (alarms disabled).
func newConfBroker(t *testing.T) *confBroker {
	t.Helper()
	return newConfBrokerCfg(t, nil)
}

// newConfBrokerCfg builds a dual-target broker, letting the caller tweak the
// embedded StrangeQ config before Build() (e.g. to arm resource alarms). The
// mutator is never invoked for the RabbitMQ target.
func newConfBrokerCfg(t *testing.T, mutate func(*config.AMQPConfig)) *confBroker {
	t.Helper()
	b := &confBroker{t: t, target: confTarget()}
	if b.target == "rabbitmq" {
		b.uri = "amqp://guest:guest@localhost:5672/"
		return b
	}
	b.storeDir = t.TempDir()
	b.cfg = config.DefaultConfig()
	b.cfg.Server.LogLevel = "silent"
	b.cfg.Network.Address = "127.0.0.1:0" // OS-assigned free port
	b.cfg.Storage.Path = b.storeDir
	b.cfg.Storage.Fsync = true // strict durability for the round-trip case
	if mutate != nil {
		mutate(b.cfg)
	}
	b.startEmbedded()
	return b
}

func (b *confBroker) startEmbedded() {
	t := b.t
	checkpoint := time.Duration(b.cfg.Storage.CheckpointIntervalMS) * time.Millisecond
	st, err := storage.NewDisruptorStorageWithEngineConfig(b.cfg.Storage.Path, checkpoint, b.cfg.GetEngine())
	require.NoError(t, err, "open storage at %s", b.cfg.Storage.Path)
	b.st = st

	srv, err := server.NewServerBuilder().WithConfig(b.cfg).WithStorage(st).Build()
	require.NoError(t, err, "build embedded server")
	b.srv = srv
	go func() { _ = srv.Start() }()
	for i := 0; i < 100; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.True(t, srv.IsListening(), "embedded server did not start listening")
	b.uri = fmt.Sprintf("amqp://guest:guest@%s/", srv.Listener.Addr().String())
}

// restart stops the embedded broker, closes storage, and reopens a fresh server
// on the SAME storage path (new OS port). No-op for RabbitMQ. The URI is updated.
func (b *confBroker) restart() {
	if b.target == "rabbitmq" {
		return
	}
	require.NoError(b.t, b.srv.Stop(), "stop embedded server")
	if c, ok := b.st.(interface{ Close() error }); ok {
		require.NoError(b.t, c.Close(), "close storage before reopen")
	}
	// Fresh config sharing the same storage path; recovery runs in Build().
	b.cfg.Network.Address = "127.0.0.1:0"
	b.startEmbedded()
}

func (b *confBroker) cleanup() {
	if b.target == "rabbitmq" {
		return
	}
	if b.srv != nil {
		_ = b.srv.Stop()
	}
	if c, ok := b.st.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// dial opens a connection + channel with the default (capability-advertising)
// amqp091-go client and registers cleanup.
func (b *confBroker) dial(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err, "dial %s", b.uri)
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	require.NoError(t, err, "open channel")
	return conn, ch
}

// ----------------------------------------------------------------------------
// Resource-alarm forcing (connection.blocked case)
// ----------------------------------------------------------------------------

// rabbitctl runs `docker exec <container> rabbitmqctl <args...>`; used only when
// the active target is RabbitMQ, to force/clear resource alarms out of band.
func rabbitctl(t *testing.T, args ...string) {
	t.Helper()
	full := append([]string{"exec", dockerRabbitContainer, "rabbitmqctl"}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	require.NoError(t, err, "rabbitmqctl %v: %s", args, out)
}

// ----------------------------------------------------------------------------
// Small assertion helpers. These are basic.get-based (synchronous poll) rather
// than basic.consume-based on purpose: a lingering consumer on a shared channel
// re-grabs requeued messages in the multi-hop cases, and StrangeQ's
// queue.declare-ok message-count is unreliable (see w7-report §7 finding F3), so
// passive-declare depth cannot be trusted for a ready-count assertion. Draining
// what a consumer actually receives is the honest, broker-agnostic check.
// ----------------------------------------------------------------------------

// getOne polls basic.get (autoAck=false) until a delivery arrives or the timeout
// elapses. Returns (delivery, true) or (zero, false). No lingering consumer.
func getOne(t *testing.T, ch *amqp.Channel, queue string, timeout time.Duration) (amqp.Delivery, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		d, ok, err := ch.Get(queue, false)
		require.NoError(t, err, "basic.get %s", queue)
		if ok {
			return d, true
		}
		if time.Now().After(deadline) {
			return amqp.Delivery{}, false
		}
		time.Sleep(75 * time.Millisecond)
	}
}

// consumeOne pulls exactly one delivery (basic.get, autoAck=false) or fails.
func consumeOne(t *testing.T, ch *amqp.Channel, queue string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	d, ok := getOne(t, ch, queue, timeout)
	if !ok {
		t.Fatalf("no delivery from %s within %s", queue, timeout)
	}
	return d
}

// drainBodies basic.get(autoAck=true)s every ready message and returns the bodies
// in delivery order. Destructive.
func drainBodies(t *testing.T, ch *amqp.Channel, queue string) [][]byte {
	t.Helper()
	var out [][]byte
	for {
		d, ok, err := ch.Get(queue, true)
		require.NoError(t, err, "basic.get %s", queue)
		if !ok {
			return out
		}
		out = append(out, d.Body)
	}
}

// readyCount drains and counts the ready messages (destructive).
func readyCount(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	return len(drainBodies(t, ch, queue))
}

// queueDepth reports the ready-message count via a passive redeclare
// (non-destructive). Used by the cycle cases to confirm a queue drains to — and
// STAYS at — zero without consuming the message (a destructive drain would hide
// a re-looping message by removing it between polls). A passive declare on a
// missing queue closes the channel, so callers must only use it on a declared
// queue. Both brokers report an accurate ready count for an idle queue here
// (verified against RabbitMQ 4.x and embedded StrangeQ in W7); the earlier
// "declare-ok count is unreliable" note applied only to counts taken while
// messages were in-flight/unacked, which these cases avoid.
func queueDepth(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	require.NoError(t, err, "passive declare %s", queue)
	return q.Messages
}

// declareDurable declares a durable queue and schedules its deletion.
func declareDurable(t *testing.T, ch *amqp.Channel, name string, args amqp.Table) {
	t.Helper()
	_, err := ch.QueueDeclare(name, true, false, false, false, args)
	require.NoError(t, err, "declare durable queue %s", name)
	t.Cleanup(func() { _, _ = ch.QueueDelete(name, false, false, false) })
}

// declareFanout declares a durable fanout exchange + schedules delete.
func declareFanout(t *testing.T, ch *amqp.Channel, name string) {
	t.Helper()
	err := ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil)
	require.NoError(t, err, "declare fanout exchange %s", name)
	t.Cleanup(func() { _ = ch.ExchangeDelete(name, false, false) })
}
