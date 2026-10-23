package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// repoRelativeDataDir is the directory NewDisruptorStorage() hardcodes
// (disruptor_storage.go: `return NewDisruptorStorageWithDataDir("./data")`),
// relative to this package's directory. `go test` runs each package with its
// own directory as cwd, so this name denotes src/amqp-go/storage/data here and
// src/amqp-go/data in the root package — which is why two contaminated
// directories existed, one per leak route.
//
// This is a deliberate second copy of the same guard. Go gives test helpers no
// way to be shared across packages without exporting them from a non-test
// package, and the whole point is that the check runs with each package's own
// cwd. If you change this, change testserver_test.go in the root package too.
const repoRelativeDataDir = "data"

// TestMain fails this package if any test left repo-relative persistent
// storage behind.
//
// The route that produced src/amqp-go/storage/data was the zero-argument
// NewDisruptorStorage(), which hardcodes "./data": seven tests and benchmarks
// in pending_ack_index_test.go shared one directory with each other AND across
// runs. They now take b.TempDir()/t.TempDir(). This assertion is what stops
// the next caller of NewDisruptorStorage() from silently reinstating it — the
// directory is gitignored, so nothing else in the toolchain would report it.
func TestMain(m *testing.M) {
	// WS-C: arm the runtime invariant assertions (I3 and its segment twin)
	// package-wide, so every test in this package runs with them. See
	// storage/invariants.go. A test that deliberately mutates a LIVE active
	// file disables them locally with SetStorageInvariantChecksForTest(false).
	storageInvariantChecks.Store(true)
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
		fmt.Fprintf(os.Stderr, "\nFAIL: %s existed before this run, so every test that built "+
			"storage from a relative path inherited its contents, including across previous "+
			"runs. It is gitignored, so `git status` cannot see it. Delete it and re-run.\n", abs)
	} else {
		fmt.Fprintf(os.Stderr, "\nFAIL: %s was created by this run: something resolved storage to "+
			"a repo-relative path (most likely the zero-argument NewDisruptorStorage()) instead "+
			"of t.TempDir()/b.TempDir(). Use NewDisruptorStorageWithDataDir(t.TempDir()).\n", abs)
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
