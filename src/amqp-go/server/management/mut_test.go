package management

import (
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
)

// declareQueue is a small helper for the mutation tests.
func (e *env) declareQueue(t *testing.T, name string, body queueDeclareRequest) {
	t.Helper()
	rec := e.do(http.MethodPut, "/api/queues/%2F/"+name, e.jsonBody(t, body), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
}

// publishTo publishes a message through the broker directly (the API publish
// path is tested separately).
func (e *env) publishTo(t *testing.T, queue, body string) {
	t.Helper()
	msg := &protocol.Message{Body: []byte(body), RoutingKey: queue}
	if err := e.srv.Broker.PublishMessage("", queue, msg); err != nil {
		t.Fatalf("publish to %s: %v", queue, err)
	}
}

// --- exchanges ---

// TestExchangeDeclare_201Then204ThenInequivalent (FR-API-053/054, spec §11
// test 57).
func TestExchangeDeclare_201Then204ThenInequivalent(t *testing.T) {
	e := basicEnv(t, true, nil)
	body := exchangeDeclareRequest{Type: "direct", Durable: true}

	created := e.do(http.MethodPut, "/api/exchanges/%2F/ex1", e.jsonBody(t, body), basicAuthHeader())
	wantStatus(t, created, http.StatusCreated)
	if loc := created.Header().Get("Location"); !strings.Contains(loc, "ex1") {
		t.Fatalf("Location = %q", loc)
	}

	again := e.do(http.MethodPut, "/api/exchanges/%2F/ex1", e.jsonBody(t, body), basicAuthHeader())
	wantStatus(t, again, http.StatusNoContent)

	conflict := e.do(http.MethodPut, "/api/exchanges/%2F/ex1", e.jsonBody(t, exchangeDeclareRequest{Type: "fanout", Durable: true}), basicAuthHeader())
	wantStatus(t, conflict, http.StatusBadRequest)
	env := decode[errorEnvelope](t, conflict)
	if env.Error != errInequivalent || !strings.Contains(env.Reason, "type") {
		t.Fatalf("inequivalent envelope = %+v, want it to name the type field", env)
	}

	invalid := e.do(http.MethodPut, "/api/exchanges/%2F/ex2", e.jsonBody(t, exchangeDeclareRequest{Type: "nope"}), basicAuthHeader())
	wantStatus(t, invalid, http.StatusBadRequest)
}

// TestExchangeDelete_System403 (FR-API-056, spec §11 test 58).
func TestExchangeDelete_System403(t *testing.T) {
	e := basicEnv(t, true, nil)
	for _, name := range []string{"amq.direct", "amq.fanout", "amq.topic", "amq.headers"} {
		rec := e.do(http.MethodDelete, "/api/exchanges/%2F/"+name, nil, basicAuthHeader())
		if rec.Code != http.StatusForbidden {
			t.Fatalf("DELETE %s = %d, want 403 (system exchange)", name, rec.Code)
		}
	}
}

// TestExchangeDelete_UserExchange204.
func TestExchangeDelete_UserExchange204(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/exchanges/%2F/doomed", e.jsonBody(t, exchangeDeclareRequest{Type: "direct", AutoDelete: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	del := e.do(http.MethodDelete, "/api/exchanges/%2F/doomed", nil, basicAuthHeader())
	wantStatus(t, del, http.StatusNoContent)
	if absent := e.do(http.MethodDelete, "/api/exchanges/%2F/doomed", nil, basicAuthHeader()); absent.Code != http.StatusNotFound {
		t.Fatalf("deleting an absent exchange must be 404, got %d", absent.Code)
	}
}

// TestExchangeDelete_IfUnusedRefused400 (FR-API-055, spec §11 test 59). The
// broker enforces the in-use check; a bound, if-unused delete is refused 400.
func TestExchangeDelete_IfUnusedRefused400(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPut, "/api/exchanges/%2F/bound-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct", Durable: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	e.declareQueue(t, "bound-q", queueDeclareRequest{Durable: true})
	if err := e.srv.Broker.BindQueue("bound-q", "bound-ex", "rk", nil); err != nil {
		t.Fatalf("bind: %v", err)
	}
	del := e.do(http.MethodDelete, "/api/exchanges/%2F/bound-ex?if-unused=true", nil, basicAuthHeader())
	wantStatus(t, del, http.StatusBadRequest)
}

// --- queues ---

// TestQueueDeclare_InequivalentArgs (FR-API-075/MIL-020, spec §11 test 60).
func TestQueueDeclare_InequivalentArgs(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "q60", queueDeclareRequest{Durable: true})
	again := e.do(http.MethodPut, "/api/queues/%2F/q60", e.jsonBody(t, queueDeclareRequest{Durable: true}), basicAuthHeader())
	wantStatus(t, again, http.StatusNoContent)
	conflict := e.do(http.MethodPut, "/api/queues/%2F/q60", e.jsonBody(t, queueDeclareRequest{Durable: false}), basicAuthHeader())
	wantStatus(t, conflict, http.StatusBadRequest)
	env := decode[errorEnvelope](t, conflict)
	if env.Error != errInequivalent || !strings.Contains(env.Reason, "durable") {
		t.Fatalf("conflict envelope = %+v", env)
	}
	badType := e.do(http.MethodPut, "/api/queues/%2F/q60b", e.jsonBody(t, queueDeclareRequest{Type: "quorum"}), basicAuthHeader())
	wantStatus(t, badType, http.StatusBadRequest)
}

// TestQueueServerNamed_PostReturnsLocation (FR-API-076/DIV-14, spec §11 test 61).
func TestQueueServerNamed_PostReturnsLocation(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodPost, "/api/queues/%2F",
		e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	loc := rec.Header().Get("Location")
	if !strings.Contains(loc, "amq.gen.") {
		t.Fatalf("server-generated queue Location = %q, want an amq.gen.* name", loc)
	}
}

// TestQueueDelete_IfEmptyHyphenOnly (FR-CONV-006/MIL-021, spec §11 test 62).
func TestQueueDelete_IfEmptyHyphenOnly(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "empty-q", queueDeclareRequest{Durable: true})
	e.publishTo(t, "empty-q", "one")

	// if_empty (underscore) is IGNORED: the queue is deleted anyway.
	ignored := e.do(http.MethodDelete, "/api/queues/%2F/empty-q?if_empty=true", nil, basicAuthHeader())
	wantStatus(t, ignored, http.StatusNoContent)
	if got := ignored.Header().Get("X-Sq-Messages-Deleted"); got == "" {
		t.Fatalf("delete must report X-Sq-Messages-Deleted")
	}

	e.declareQueue(t, "empty-q2", queueDeclareRequest{Durable: true})
	e.publishTo(t, "empty-q2", "one")
	// if-empty (hyphen) IS honoured: a non-empty queue refuses.
	refused := e.do(http.MethodDelete, "/api/queues/%2F/empty-q2?if-empty=true", nil, basicAuthHeader())
	wantStatus(t, refused, http.StatusBadRequest)
}

// TestQueuePurge_Headers (FR-API-078/DIV-2, spec §11 test 63).
func TestQueuePurge_Headers(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "purge-q", queueDeclareRequest{Durable: true})
	for i := 0; i < 3; i++ {
		e.publishTo(t, "purge-q", "m")
	}
	rec := e.do(http.MethodDelete, "/api/queues/%2F/purge-q/contents", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusNoContent)
	if got := rec.Header().Get("X-Sq-Messages-Purged"); got != "3" {
		t.Fatalf("X-Sq-Messages-Purged = %q, want 3", got)
	}
	// Present and empty ⇒ 204 with 0 (FR-CONV-062).
	empty := e.do(http.MethodDelete, "/api/queues/%2F/purge-q/contents", nil, basicAuthHeader())
	wantStatus(t, empty, http.StatusNoContent)
	if got := empty.Header().Get("X-Sq-Messages-Purged"); got != "0" {
		t.Fatalf("second purge = %q, want 0", got)
	}
	// Absent queue ⇒ 404.
	absent := e.do(http.MethodDelete, "/api/queues/%2F/nope/contents", nil, basicAuthHeader())
	wantStatus(t, absent, http.StatusNotFound)
}

// TestPurge_OverThreshold202Done (MIL-037, spec §11 test 79).
func TestPurge_OverThreshold202Done(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Async.MessageThreshold = 0
	})
	e.declareQueue(t, "async-purge", queueDeclareRequest{Durable: true})
	e.publishTo(t, "async-purge", "m")

	rec := e.do(http.MethodDelete, "/api/queues/%2F/async-purge/contents", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusAccepted)
	loc := rec.Header().Get("Location")
	if !strings.HasPrefix(loc, "/api/strangeq/jobs/") {
		t.Fatalf("Location = %q, want a job URL", loc)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatalf("202 must carry Retry-After")
	}

	// Poll the job until it is done, with a bounded deadline.
	deadline := time.Now().Add(5 * time.Second)
	for {
		jobRec := e.do(http.MethodGet, loc, nil, basicAuthHeader())
		wantStatus(t, jobRec, http.StatusOK)
		job := decode[JobView](t, jobRec)
		if job.State == jobDone {
			return
		}
		if job.State == jobFailed {
			t.Fatalf("purge job failed: %+v", job)
		}
		if time.Now().After(deadline) {
			t.Fatalf("purge job did not finish within the deadline: %+v", job)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestQueueGet_AckModesAndBase64 (FR-API-079/080, spec §11 test 64).
func TestQueueGet_AckModesAndBase64(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "get-q", queueDeclareRequest{Durable: true})
	e.publishTo(t, "get-q", "hello")
	e.publishTo(t, "get-q", "\x00\x01\x02binary")

	// Default ackmode is ack_requeue_true: the message comes back and stays.
	rec := e.do(http.MethodPost, "/api/queues/%2F/get-q/get", e.jsonBody(t, queueGetRequest{Count: 1}), basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	msgs := decode[[]getMessage](t, rec)
	if len(msgs) != 1 {
		t.Fatalf("get returned %d messages, want 1", len(msgs))
	}
	if msgs[0].Payload != "hello" || msgs[0].PayloadEncoding != "string" {
		t.Fatalf("first get = %+v", msgs[0])
	}

	// Drain with reject_requeue_false to reach the binary message.
	drained := e.do(http.MethodPost, "/api/queues/%2F/get-q/get",
		e.jsonBody(t, queueGetRequest{Count: 10, AckMode: "reject_requeue_false"}), basicAuthHeader())
	wantStatus(t, drained, http.StatusOK)
	all := decode[[]getMessage](t, drained)
	seenBase64 := false
	for _, m := range all {
		if m.PayloadEncoding == "base64" {
			if _, err := base64.StdEncoding.DecodeString(m.Payload); err != nil {
				t.Fatalf("base64 payload does not decode: %v", err)
			}
			seenBase64 = true
		}
	}
	if !seenBase64 {
		t.Fatalf("binary payload must be base64-encoded: %+v", all)
	}

	bad := e.do(http.MethodPost, "/api/queues/%2F/get-q/get", e.jsonBody(t, queueGetRequest{AckMode: "nonsense"}), basicAuthHeader())
	wantStatus(t, bad, http.StatusBadRequest)
}

// TestQueueGet_CountCap (FR-API-081, spec §11 test 65).
func TestQueueGet_CountCap(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "cap-q", queueDeclareRequest{Durable: true})
	rec := e.do(http.MethodPost, "/api/queues/%2F/cap-q/get", e.jsonBody(t, queueGetRequest{Count: 101}), basicAuthHeader())
	wantStatus(t, rec, http.StatusBadRequest)
}

// --- publish ---

// TestPublish_RoutedAndUnroutable (FR-API-058, spec §11 test 66).
func TestPublish_RoutedAndUnroutable(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "pub-q", queueDeclareRequest{Durable: true})
	rec := e.do(http.MethodPut, "/api/exchanges/%2F/pub-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	if err := e.srv.Broker.BindQueue("pub-q", "pub-ex", "key", nil); err != nil {
		t.Fatalf("bind: %v", err)
	}

	routed := e.do(http.MethodPost, "/api/exchanges/%2F/pub-ex/publish",
		e.jsonBody(t, publishRequest{RoutingKey: "key", Payload: "hi", PayloadEncoding: "string"}), basicAuthHeader())
	wantStatus(t, routed, http.StatusOK)
	if out := decode[publishResponse](t, routed); !out.Routed {
		t.Fatalf("a bound routing key must be routed: %+v", out)
	}

	unroutable := e.do(http.MethodPost, "/api/exchanges/%2F/pub-ex/publish",
		e.jsonBody(t, publishRequest{RoutingKey: "nowhere", Payload: "hi"}), basicAuthHeader())
	wantStatus(t, unroutable, http.StatusOK)
	if out := decode[publishResponse](t, unroutable); out.Routed {
		t.Fatalf("an unbound routing key must be routed:false, got %+v", out)
	}

	badEnc := e.do(http.MethodPost, "/api/exchanges/%2F/pub-ex/publish",
		e.jsonBody(t, publishRequest{RoutingKey: "key", Payload: "hi", PayloadEncoding: "hex"}), basicAuthHeader())
	wantStatus(t, badEnc, http.StatusBadRequest)
}

// TestPublish_Oversize413 (FR-API-059, spec §11 test 68).
func TestPublish_Oversize413(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Server.MaxMessageSize = 16
	})
	rec := e.do(http.MethodPost, "/api/exchanges/%2F/amq.direct/publish",
		e.jsonBody(t, publishRequest{RoutingKey: "k", Payload: strings.Repeat("x", 64)}), basicAuthHeader())
	wantStatus(t, rec, http.StatusRequestEntityTooLarge)
	env := decode[errorEnvelope](t, rec)
	if env.Error != errPayloadTooLarge {
		t.Fatalf("token = %q, want payload_too_large", env.Error)
	}
}

// --- connection close ---

// TestClose_ForceClose204AndAMQPSeesEOF (FC §7.6, spec §11 test 69). With
// broker authentication disabled, force-close is gated on the explicit
// management.allow_connection_close opt-in (SEC-066); the AMQP peer must then
// observe EOF.
func TestClose_ForceClose204AndAMQPSeesEOF(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.AllowConnectionClose = true
	})
	peer := e.injectConnection(t, "conn-1", "someone")

	rec := e.do(http.MethodDelete, "/api/connections/conn-1", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusNoContent)
	if absent := e.do(http.MethodDelete, "/api/connections/missing", nil, basicAuthHeader()); absent.Code != http.StatusNotFound {
		t.Fatalf("closing an absent connection must be 404, got %d", absent.Code)
	}

	// The AMQP peer must observe the socket closing: a bounded read deadline
	// turns a silent hang into a deterministic failure.
	mustSeeEOF(t, peer)
}

// TestCloseByUser_ForceClose204AndAMQPSeesEOF is the /username/{user} variant:
// every connection for the user is force-closed and the peer sees EOF.
func TestCloseByUser_ForceClose204AndAMQPSeesEOF(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.AllowConnectionClose = true
	})
	peer := e.injectConnection(t, "conn-u", "alice")

	rec := e.do(http.MethodDelete, "/api/connections/username/alice", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusNoContent)

	mustSeeEOF(t, peer)
}

// mustSeeEOF requires peer.Read to fail within a bounded deadline: a closed
// socket yields EOF (or a reset), never a successful read.
func mustSeeEOF(t *testing.T, peer net.Conn) {
	t.Helper()
	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 1)
	n, err := peer.Read(buf)
	if err == nil {
		t.Fatalf("the AMQP peer must observe EOF/reset after a force-close, read %d bytes", n)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("the AMQP peer did not observe EOF within the deadline")
	}
}

// injectConnection puts a live connection (with a real socket) into the broker
// registry so the close path has something to close.
func (e *env) injectConnection(t *testing.T, id, user string) net.Conn {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	srvSide, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	e.srv.Mutex.Lock()
	e.srv.Connections[id] = &protocol.Connection{ID: id, Conn: srvSide, Username: user, Vhost: "/"}
	e.srv.Mutex.Unlock()
	return peer
}

// TestClose_OthersPermission (AUTH-015, spec §11 test 70).
func TestClose_OthersPermission(t *testing.T) {
	e := inheritEnv(t, `{"users":[
		{"username":"mon","password_hash":"`+secretHash(t)+`","tags":["monitoring"],
		 "vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]},
		{"username":"adm","password_hash":"`+secretHash(t)+`","tags":["administrator"],
		 "vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	e.injectConnection(t, "victim-1", "other-user")

	mon := e.do(http.MethodDelete, "/api/connections/victim-1", nil, map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")})
	if mon.Code != http.StatusForbidden {
		t.Fatalf("monitoring must not close another user's connection: %d", mon.Code)
	}

	e.injectConnection(t, "victim-2", "other-user")
	adm := e.do(http.MethodDelete, "/api/connections/victim-2", nil, map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")})
	if adm.Code != http.StatusNoContent {
		t.Fatalf("administrator must close another user's connection: %d (%s)", adm.Code, adm.Body.String())
	}
}

// TestClose_AuthDisabledRequiresFlag (AUTH-015/SEC-066, spec §11 test 71).
func TestClose_AuthDisabledRequiresFlag(t *testing.T) {
	e := newEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationNone
		cfg.Management.AllowUnauthenticatedMutations = true
		cfg.Management.AllowConnectionClose = false
	})
	e.injectConnection(t, "guest-conn", "whoever")
	rec := e.get("/api/connections/guest-conn")
	wantStatus(t, rec, http.StatusOK)

	denied := e.do(http.MethodDelete, "/api/connections/guest-conn", nil, nil)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("with broker auth disabled and allow_connection_close false the close must be 403, got %d", denied.Code)
	}

	e2 := newEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationNone
		cfg.Management.AllowUnauthenticatedMutations = true
		cfg.Management.AllowConnectionClose = true
	})
	e2.injectConnection(t, "guest-conn", "whoever")
	allowed := e2.do(http.MethodDelete, "/api/connections/guest-conn", nil, nil)
	if allowed.Code != http.StatusNoContent {
		t.Fatalf("with allow_connection_close true the close must succeed, got %d (%s)", allowed.Code, allowed.Body.String())
	}
}

// --- definitions import ---

// TestDefinitionsImport_MergeAndDisclosure (MIL-041, spec §11 test 75).
func TestDefinitionsImport_MergeAndDisclosure(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "already-there", queueDeclareRequest{Durable: true})

	defs := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "imp-ex", "type": "direct", "durable": true}},
		Queues: []map[string]any{
			{"name": "imp-q", "durable": true},
			{"name": "already-there", "durable": true},
		},
		Users:       []map[string]any{{"name": "bob"}},
		Permissions: []map[string]any{{"user": "bob"}},
		Bindings:    []map[string]any{{"source": "imp-ex"}},
	}
	rec := e.do(http.MethodPost, "/api/definitions", e.jsonBody(t, defs), basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	res := decode[definitionsImportResult](t, rec)
	applied := strings.Join(res.Applied, ",")
	skipped := strings.Join(res.Skipped, ",")
	if !strings.Contains(applied, "exchange:imp-ex") || !strings.Contains(applied, "queue:imp-q") {
		t.Fatalf("applied = %v", res.Applied)
	}
	if !strings.Contains(skipped, "already-there") {
		t.Fatalf("an equivalent entity must be skipped: %v", res.Skipped)
	}
	for _, want := range []string{"users:", "permissions:", "bindings:"} {
		if !strings.Contains(skipped, want) {
			t.Fatalf("skipped must disclose %q: %v", want, res.Skipped)
		}
	}
	if res.StrangeQ == nil {
		t.Fatalf("response must carry the strangeq disclosure object")
	}
}

// --- jobs ---

// TestJobs_BoundedOwnedExpired404 (FR-API-142/DATA-064, spec §11 test 78).
func TestJobs_BoundedOwnedExpired404(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	pool := newJobPool(2, 30*time.Second, 1)
	pool.now = func() time.Time { return now }
	t.Cleanup(func() { pool.close() })

	release := make(chan struct{})
	j1, err := pool.submit("purge", "alice", nil, func() (any, error) { <-release; return nil, nil })
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := pool.submit("purge", "alice", nil, func() (any, error) { <-release; return nil, nil }); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, err := pool.submit("purge", "alice", nil, func() (any, error) { <-release; return nil, nil }); err != ErrJobPoolFull {
		t.Fatalf("third job = %v, want ErrJobPoolFull (max=2)", err)
	}
	close(release)

	// Ownership: another principal sees nothing.
	if _, ok := pool.get(j1.id, "bob"); ok {
		t.Fatalf("a job must not be visible to another principal")
	}
	if _, ok := pool.get(j1.id, "alice"); !ok {
		t.Fatalf("the owner must see the job")
	}

	// Wait (bounded) for both jobs to reach a terminal state before advancing
	// the retention clock, so the reap decision is deterministic.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if st, _ := pool.stateOf(j1.id, "alice"); st == jobDone || st == jobFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not reach a terminal state")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Retention: after the window the job is reaped.
	now = now.Add(2 * time.Minute)
	if _, ok := pool.get(j1.id, "alice"); ok {
		t.Fatalf("a job older than the retention window must be gone")
	}
}

// --- failure isolation ---

// TestPanicRecovered500WithRequestID (NFR-020, spec §11 test 80).
func TestPanicRecovered500WithRequestID(t *testing.T) {
	e := basicEnv(t, true, nil)
	for _, rt := range e.mgmt.router.routes {
		if rt.pattern == "/api/overview" {
			rt.handler = func(http.ResponseWriter, *http.Request) { panic("boom") }
		}
	}
	rec := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusInternalServerError)
	env := decode[errorEnvelope](t, rec)
	if env.Error != errInternal {
		t.Fatalf("token = %q, want internal_error", env.Error)
	}
	if !strings.Contains(env.Reason, "internal error") {
		t.Fatalf("500 reason must be generic, got %q", env.Reason)
	}
	if env.RequestID == "" {
		t.Fatalf("500 must carry the correlation id")
	}
	if !strings.Contains(rec.Body.String(), env.RequestID) {
		t.Fatalf("the correlation id must be in the body")
	}
}

// TestInFlightCap503 (SEC-061, spec §11 test 81).
func TestInFlightCap503(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.MaxInFlight = 1
	})
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	for _, rt := range e.mgmt.router.routes {
		if rt.pattern == "/api/overview" {
			rt.handler = func(w http.ResponseWriter, r *http.Request) {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-block
				w.WriteHeader(http.StatusOK)
			}
		}
	}
	go func() { _ = e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("first request never entered the handler")
	}
	second := e.do(http.MethodGet, "/api/overview", nil, basicAuthHeader())
	wantStatus(t, second, http.StatusServiceUnavailable)
	close(block)
}

// TestRateLimit429 (SEC-068, spec §11 test 82).
func TestRateLimit429(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.Publish = 1
	})
	e.declareQueue(t, "rl-q", queueDeclareRequest{Durable: true})
	body := e.jsonBody(t, publishRequest{RoutingKey: "rl-q", Payload: "x"})
	first := e.do(http.MethodPost, "/api/exchanges/%2F/amq.default/publish", body, basicAuthHeader())
	if first.Code == http.StatusTooManyRequests {
		t.Fatalf("first publish must not be rate limited")
	}
	second := e.do(http.MethodPost, "/api/exchanges/%2F/amq.default/publish", body, basicAuthHeader())
	wantStatus(t, second, http.StatusTooManyRequests)
	if second.Header().Get("Retry-After") == "" {
		t.Fatalf("429 must carry Retry-After")
	}
}

// TestBodyLimits (SEC-065/MIL-046, spec §11 test 83).
func TestBodyLimits(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.MaxBodyBytes = 1024
	})
	big := strings.Repeat("a", 2*1024*1024)
	rec := e.do(http.MethodPost, "/api/strangeq/session", []byte(`{"username":"`+big+`"}`), nil)
	wantStatus(t, rec, http.StatusRequestEntityTooLarge)
}

// TestAuditRedaction (SEC-080/082/085, spec §11 test 86).
func TestAuditRedaction(t *testing.T) {
	e := basicEnv(t, true, nil)
	sink := &captureSink{}
	e.mgmt.auditSink = sink

	// A control character in an entity name must be sanitised before it reaches
	// the audit record.
	rec := e.do(http.MethodPut, "/api/queues/%2F/cr%0Alf%09tab",
		e.jsonBody(t, queueDeclareRequest{Exclusive: true, AutoDelete: true}), basicAuthHeader())
	if rec.Code != http.StatusCreated {
		t.Fatalf("declare: got %d (%s)", rec.Code, rec.Body.String())
	}
	recs := sink.records()
	if len(recs) == 0 {
		t.Fatalf("a mutation must emit an audit record")
	}
	for _, r := range recs {
		for _, field := range []string{r.Entity, r.Action, r.Path, r.Principal, r.Client} {
			if strings.ContainsAny(field, "\r\n\t") {
				t.Fatalf("audit field must not contain control characters: %q", field)
			}
			if len(field) > 256 {
				t.Fatalf("audit field must be length-capped, got %d bytes", len(field))
			}
		}
	}
}

// TestAuditDestructiveDurable_SinkOverflow503 (SEC-084, spec §11 test 85).
func TestAuditDestructiveDurable_SinkOverflow503(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.mgmt.auditSink = failingSink{}
	e.declareQueue(t, "audit-q", queueDeclareRequest{Durable: true})

	// A destructive mutation whose audit record cannot be written must fail
	// with 503 and NOT proceed.
	rec := e.do(http.MethodDelete, "/api/queues/%2F/audit-q", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusServiceUnavailable)

	// The queue must still exist.
	if got := e.do(http.MethodGet, "/api/queues/%2F/audit-q", nil, basicAuthHeader()); got.Code != http.StatusOK {
		t.Fatalf("a refused destructive mutation must not have deleted the queue: %d", got.Code)
	}
}

// TestAuditDestructive_NonDestructiveProceeds proves the other half of SEC-084:
// a non-destructive mutation proceeds and counts a dropped record.
func TestAuditDestructive_NonDestructiveProceeds(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.mgmt.auditSink = failingSink{}
	rec := e.do(http.MethodPut, "/api/exchanges/%2F/nondestructive", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	if e.mgmt.auditDropped.Load() == 0 {
		t.Fatalf("a dropped non-destructive audit record must be counted")
	}
}

// TestNullDiscipline_ChannelsLockFreeSubset (locked scope #3, spec §11 test 48).
func TestChannels_LockFreeSubset(t *testing.T) {
	e := basicEnv(t, true, nil)
	// No channels exist; the list must still be a well-formed 200.
	rec := e.do(http.MethodGet, "/api/channels", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
		t.Fatalf("channels = %s, want []", body)
	}
	if got := e.do(http.MethodGet, "/api/channels/999", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("unknown channel must be 404, got %d", got.Code)
	}
	// There is no close verb: /api/channels/{id}/close must not exist.
	if got := e.do(http.MethodDelete, "/api/channels/1", nil, basicAuthHeader()); got.Code != http.StatusMethodNotAllowed {
		t.Fatalf("channels must be read-only (405 on DELETE), got %d", got.Code)
	}
}

// TestConsumers_DuplicateTagBothReturned (FR-API-042, spec §11 test 47). The
// management layer keys by consumer identity, never by tag.
func TestConsumers_DuplicateTagBothReturned(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "dup-q", queueDeclareRequest{Durable: true})
	// Register the same client-visible tag on two different channels.
	conn := &protocol.Connection{ID: "dup-conn", Vhost: "/", Username: "u"}
	ch1 := protocol.NewChannel(1, conn)
	ch2 := protocol.NewChannel(2, conn)
	for i, ch := range []*protocol.Channel{ch1, ch2} {
		c := &protocol.Consumer{ID: "cid-" + string(rune('a'+i)), Tag: "same-tag", Queue: "dup-q", Channel: ch}
		if err := e.srv.Broker.RegisterConsumer("dup-q", c.ID, c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	rec := e.do(http.MethodGet, "/api/consumers", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	out := decode[[]consumerInfo](t, rec)
	if len(out) != 2 {
		t.Fatalf("two consumers sharing a tag must both be returned, got %d (%+v)", len(out), out)
	}
}

// TestRequestBodyRejectedBeforeRouting (SEC-065, spec §11 test 84): an
// oversized body is refused 413 without a route match, an authenticator, or a
// handler running — the request never reaches routing.
func TestRequestBodyRejectedBeforeRouting(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.MaxBodyBytes = 1024
		cfg.Management.Limits.MaxImportBodyBytes = 4096
	})
	// A path that has NO route at all: without the pre-routing cap this would
	// be 404. It must be 413 instead.
	big := strings.Repeat("x", 8192)
	rec := e.do(http.MethodPost, "/api/does-not-exist", []byte(`{"a":"`+big+`"}`), basicAuthHeader())
	wantStatus(t, rec, http.StatusRequestEntityTooLarge)
	if env := decode[errorEnvelope](t, rec); env.Error != errPayloadTooLarge {
		t.Fatalf("token = %q, want payload_too_large", env.Error)
	}
}

// TestDefinitionsImport_OverCapFailsEntity (MIL-043, spec §11 test 77).
func TestDefinitionsImport_OverCapFailsEntity(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Definitions.MaxQueues = 1
		cfg.Management.Definitions.MaxExchanges = 1
	})
	defs := definitionsImportRequest{
		Exchanges: []map[string]any{
			{"name": "cap-ex-1", "type": "direct"},
			{"name": "cap-ex-2", "type": "direct"},
		},
		Queues: []map[string]any{
			{"name": "cap-q-1"},
			{"name": "cap-q-2"},
		},
	}
	rec := e.do(http.MethodPost, "/api/definitions", e.jsonBody(t, defs), basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	res := decode[definitionsImportResult](t, rec)
	if len(res.Applied) != 2 {
		t.Fatalf("only one exchange and one queue may be applied, got %v", res.Applied)
	}
	failed := strings.Join(res.Failed, ",")
	if !strings.Contains(failed, "cap-ex-2") || !strings.Contains(failed, "cap-q-2") {
		t.Fatalf("over-cap entities must be reported as failed, got %v", res.Failed)
	}
}

// --- sinks used by the audit tests ---

type captureSink struct {
	mu   sync.Mutex
	recs []auditRecord
}

func (c *captureSink) Write(r auditRecord, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r)
	return nil
}

func (c *captureSink) records() []auditRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]auditRecord(nil), c.recs...)
}

// failingSink simulates an audit sink that cannot make a record durable.
type failingSink struct{}

func (failingSink) Write(auditRecord, bool) error { return errAuditSinkDown }

var errAuditSinkDown = errors.New("audit sink unavailable")
