package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// testShutdownServer is the single bounded-teardown seam for root-package
// embedded brokers. Shutdown is idempotent and safe to call after Stop().
// On a forced join it legitimately returns context.DeadlineExceeded or
// ErrShutdownIncomplete (server/shutdown.go:105-117,138-142); a teardown must
// never turn that into a machine-speed test failure, so this LOGS, never fails.
func testShutdownServer(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Logf("teardown: Shutdown returned %v (listener closed; storage may remain open)", err)
	}
}

// isolatedTestConfig returns a default config bound to addr whose storage
// points at a per-test temporary directory.
//
// config.DefaultConfig() resolves Storage.Path to the repo-relative "./data",
// which is shared between every test in the package, persists across runs, and
// is gitignored — so its contents are invisible to `git status` and absent
// from a clean clone. A stale WAL left there once produced 25 swallowed
// "Recovery failed" errors on every suite run while the suite stayed green.
// Tests must not share persistent storage state.
func isolatedTestConfig(t *testing.T, addr string) *config.AMQPConfig {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = t.TempDir()
	return cfg
}

// newIsolatedTestServer builds a server from cfg.
//
// It deliberately does NOT use server.NewServer: that constructor discards any
// caller configuration (it calls config.DefaultConfig() itself), and it
// SWALLOWS a Build() failure, returning a hollow Server with no broker, no
// storage and no config rather than an error — so a test whose server failed
// to build would keep running against a shell and could still report PASS.
func newIsolatedTestServer(t *testing.T, cfg *config.AMQPConfig) *server.Server {
	t.Helper()
	requireIsolatedStorage(t, cfg)
	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("building test server at %s: %v", cfg.Network.Address, err)
	}
	t.Cleanup(func() { testShutdownServer(t, srv) })
	return srv
}

// requireIsolatedStorage asserts at runtime that a test's storage really is
// isolated, rather than leaving that as a comment for the next person to
// break. A relative Storage.Path resolves against the package directory and
// is therefore shared and persistent.
func requireIsolatedStorage(t *testing.T, cfg *config.AMQPConfig) {
	t.Helper()
	if cfg.Storage.Path == "" || !filepath.IsAbs(cfg.Storage.Path) {
		t.Fatalf("test storage path %q is not absolute: it resolves against the package "+
			"directory, so this test shares persistent state with every other test and "+
			"with previous runs; use isolatedTestConfig / t.TempDir()", cfg.Storage.Path)
	}
}

// waitForListening starts srv on its own goroutine and blocks until its
// listener is bound, failing the test if it never binds.
//
// It surfaces Start()'s error instead of discarding it. Start() calls
// net.Listen synchronously and returns a bind failure before it blocks in its
// accept loop, so a port collision is reported here as the real errno rather
// than as a poll-budget timeout sixty seconds later. The budget is
// brokerStartupPolls × brokerStartupPollInterval (bet0_multiqueue_test.go), so
// tightening it can be reasoned about in one place for every caller.
//
// COVERAGE, stated exactly rather than as "everything". Two helpers in this
// package deliberately do NOT route through here, and both say so at their own
// site: bet0TryServer and its sibling in bet0_multiqueue_test.go hand startup
// failures back to their callers — the refusal-to-boot cases assert on the
// returned error — so they cannot use a helper that calls FailNow. They still
// capture Start()'s error rather than discarding it.
//
// This sentence used to claim "every embedded-broker helper in this package",
// which was false three ways, and restartableBroker.start() sat outside it
// discarding Start()'s error for exactly as long as the claim went unchecked.
// A comment asserting its own completeness is a coverage claim, and it needs
// counting like any other — the step that wrote this one repeated its framing
// instead of counting the helpers, and inherited the same error.
func waitForListening(t testing.TB, srv *server.Server) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	for i := 0; i < brokerStartupPolls; i++ {
		if srv.IsListening() {
			return
		}
		select {
		case err := <-errCh:
			require.NoError(t, err, "server failed to start")
			require.FailNow(t, "server Start() returned without ever listening")
		default:
		}
		time.Sleep(brokerStartupPollInterval)
	}
	require.FailNow(t, "server listener not ready")
}

// repoRelativeDataDir is the directory config.DefaultConfig() resolves
// Storage.Path to ("./data"), relative to this package's directory. `go test`
// runs each package with its own directory as cwd, so this same name denotes a
// DIFFERENT directory per package — which is why two of them existed
// (src/amqp-go/data and src/amqp-go/storage/data) and why the guard below has
// a counterpart in the storage package rather than living in one place.
// Go gives test helpers no way to be shared across packages without exporting
// them from a non-test package, so the duplication is forced; if you change
// this, change storage/storage_isolation_test.go too.
const repoRelativeDataDir = "data"

// TestMain fails this package if any test left repo-relative persistent
// storage behind.
//
// This is the difference between the leak being *fixed* and the class being
// *closed*. A future test that reaches for config.DefaultConfig() without
// overriding Storage.Path will re-create this directory, and nothing else
// would notice: the directory is gitignored, so `git status` stays clean; the
// broker swallows the resulting WAL-version recovery failures and boots
// anyway; and a fresh clone does not reproduce it. The only signal is the
// directory's existence, so that is what gets asserted — at runtime, rather
// than written down in a comment for someone to not read.
func TestMain(m *testing.M) {
	os.Exit(runWithStorageLeakCheck(repoRelativeDataDir, m.Run))
}

// runWithStorageLeakCheck is TestMain's testable body. It takes the directory
// and the run function as parameters rather than reading the package constant
// and *testing.M, so its own behaviour can be exercised against a scratch
// directory — a guard that has never been seen to fire guards nothing, and
// this one's whole job is to fire on a condition that is otherwise invisible.
func runWithStorageLeakCheck(dir string, run func() int) int {
	_, existedBefore := os.Stat(dir)
	code := run()

	if _, err := os.Stat(dir); err != nil {
		return code // no repo-relative storage — the isolation held
	}

	abs, _ := filepath.Abs(dir)
	if existedBefore == nil {
		fmt.Fprintf(os.Stderr, "\nFAIL: %s existed before this run: every test built from "+
			"config.DefaultConfig() inherited its contents, including across previous runs. "+
			"It is gitignored, so `git status` cannot see it. Delete it and re-run.\n", abs)
	} else {
		fmt.Fprintf(os.Stderr, "\nFAIL: %s was created by this run: a test resolved storage to a "+
			"repo-relative path instead of t.TempDir(). Persistent state shared between tests "+
			"and across runs is invisible to `git status` and absent from a clean clone. "+
			"Use isolatedTestConfig / newIsolatedTestServer.\n", abs)
	}
	if code == 0 {
		return 1
	}
	return code
}

// TestRunWithStorageLeakCheck is the standing mutation proof for the guard
// above: it forces each branch rather than trusting that a guard which has
// never fired would fire. Without it, a silently dead TestMain would report
// success forever, which is the exact failure shape it exists to prevent.
func TestRunWithStorageLeakCheck(t *testing.T) {
	t.Run("clean run passes through", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		if got := runWithStorageLeakCheck(dir, func() int { return 0 }); got != 0 {
			t.Fatalf("got %d, want 0 — a run that created no storage must pass", got)
		}
	})
	t.Run("a run that creates the directory fails", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		got := runWithStorageLeakCheck(dir, func() int {
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatalf("seeding the leak: %v", err)
			}
			return 0
		})
		if got == 0 {
			t.Fatal("a run that left repo-relative storage behind reported success")
		}
	})
	t.Run("a pre-existing directory fails", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("seeding the leak: %v", err)
		}
		if got := runWithStorageLeakCheck(dir, func() int { return 0 }); got == 0 {
			t.Fatal("a run that inherited repo-relative storage reported success")
		}
	})
	t.Run("an underlying failure is not masked", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		if got := runWithStorageLeakCheck(dir, func() int { return 3 }); got != 3 {
			t.Fatalf("got %d, want the underlying failure code 3 preserved", got)
		}
	})
}
