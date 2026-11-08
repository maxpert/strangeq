package management

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// TestForbiddenLock_OverviewDoesNotBlock holds every lock that is FORBIDDEN to
// a management request and proves /api/overview and /api/queues still return
// within a per-call deadline (NFR-011/FR-API-007, spec §12C).
func TestForbiddenLock_OverviewDoesNotBlock(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "locked-q", queueDeclareRequest{Durable: true})

	// Put a live connection + channel into the registry, then hold the
	// FORBIDDEN locks on them.
	peer := e.injectConnection(t, "locked-conn", "u")
	defer peer.Close()

	conn := &protocol.Connection{ID: "locked-conn", Vhost: "/", Username: "u"}
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	ch.Mutex.Lock()
	defer ch.Mutex.Unlock()
	conn.WriteMutex.Lock()
	defer conn.WriteMutex.Unlock()

	for _, target := range []string{"/api/overview", "/api/queues", "/api/queues?page=1"} {
		done := make(chan int, 1)
		go func(tgt string) {
			done <- e.do(http.MethodGet, tgt, nil, basicAuthHeader()).Code
		}(target)
		select {
		case code := <-done:
			if code != http.StatusOK {
				t.Fatalf("GET %s = %d while a forbidden lock is held, want 200", target, code)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("GET %s did not return within 1s while a forbidden lock is held", target)
		}
	}
	if n := e.mgmt.forbiddenLockHits.Load(); n != 0 {
		t.Fatalf("amqp_management_forbidden_lock_acquired_total = %d, want 0", n)
	}
}

// TestForbiddenLockGate_ASTScan is the mechanical gate behind
// amqp_management_forbidden_lock_acquired_total: the management package source
// must never reference a forbidden accessor (NFR-011/016, spec-03 §5 test 21).
//
// `conn.Closed` (protocol.Connection) is allowed; `ch.Closed`
// (protocol.Channel) is not — the connection-level atomic is the AMQP-side
// close flag read only through the connection registry.
func TestForbiddenLockGate_ASTScan(t *testing.T) {
	// ALWAYS forbidden: an accessor whose mere presence takes a forbidden lock.
	alwaysForbidden := map[string]bool{
		"GetStats":       true, // Lifecycle.GetStats takes Channel.Mutex
		"WriteMutex":     true,
		"wireTagMu":      true,
		"confirmOrderMu": true,
		"confirmFlushMu": true,
	}
	// CHANNEL-GUARDED: protocol.Channel.Consumers/.PrefetchCount/.Closed are
	// reachable only through a channel-typed receiver; flag them when the
	// receiver identifier is channel-named. server.ChannelView exposes
	// same-named LOCK-FREE fields and conn.Closed is protocol.Connection's
	// atomic, so both are exempt. The hold-the-lock test above is the
	// deterministic proof; this scan is the mechanical guard.
	channelFields := map[string]bool{
		"Consumers":     true,
		"PrefetchCount": true,
		"Closed":        true,
	}
	channelReceivers := map[string]bool{
		"ch": true, "channel": true, "chn": true, "chan": true, "protocolChannel": true,
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		checked++
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			line := fset.Position(sel.Pos()).Line
			if alwaysForbidden[sel.Sel.Name] {
				t.Errorf("%s:%d: management request path references forbidden %q",
					path, line, sel.Sel.Name)
				return true
			}
			if !channelFields[sel.Sel.Name] {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if sel.Sel.Name == "Closed" && id.Name == "conn" {
				return true // protocol.Connection.Closed is an atomic read
			}
			if channelReceivers[id.Name] {
				t.Errorf("%s:%d: management request path reads channel-guarded %s.%s",
					path, line, id.Name, sel.Sel.Name)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatalf("AST scan found no source files")
	}
}
