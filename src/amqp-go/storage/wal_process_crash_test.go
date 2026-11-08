//go:build !windows

package storage

// T-BE-7 / T-BAR-6: process-crash regression.
//
// A child process (this test binary re-exec'd) opens a WALManager, publishes
// records with SYNCHRONOUS confirms, and appends each confirmed offset to a
// side file AFTER its Write returned (so the side file is the confirmed set).
// The parent SIGKILLs the child mid-stream, reboots a WALManager on the same
// directory and asserts:
//
//   - every offset the child CONFIRMED is recovered;
//   - recovery never invents an offset more than one record past the last the
//     child confirmed (the serial per-call duplication window);
//   - recovery succeeds (a bounded tail defect from the kill is benign).
//
// HONEST SCOPE: SIGKILL is safe even with no sync, so this guards the
// confirm -> index -> recovery ordering in BOTH crash models; it cannot prove
// the barrier's power-loss property, which no CI test can.

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

const (
	envCrashChild    = "STQ_WAL_CRASH_CHILD"
	envCrashDir      = "STQ_WAL_CRASH_DIR"
	envCrashMode     = "STQ_WAL_CRASH_MODE"
	envCrashProgress = "STQ_WAL_CRASH_PROGRESS"
)

// walCrashConfig is the config the child writes with and the parent recovers
// with: production-sized files, maintenance loops an hour away, the requested
// crash model.
func walCrashConfig(mode string) WALConfig {
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour
	if mode == interfaces.CrashModelForceFlush {
		cfg.ConfirmSync = walConfirmForceFlush
	}
	return cfg
}

func TestWALProcessCrashRegression(t *testing.T) {
	if os.Getenv(envCrashChild) == "1" {
		walCrashChild()
		return
	}
	for _, mode := range []string{interfaces.CrashModelBarrier, interfaces.CrashModelForceFlush} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			progressDir := t.TempDir()
			progress := progressDir + "/confirmed.txt"

			cmd := exec.Command(os.Args[0], "-test.run=^TestWALProcessCrashRegression$", "-test.v")
			cmd.Env = append(os.Environ(),
				envCrashChild+"=1",
				envCrashDir+"="+dir,
				envCrashMode+"="+mode,
				envCrashProgress+"="+progress,
			)
			var stderr strings.Builder
			cmd.Stderr = &stderr
			require.NoError(t, cmd.Start(), "start child")

			_ = waitForConfirmed(t, progress, 5, 20*time.Second)
			require.NoError(t, cmd.Process.Kill(), "SIGKILL the child")
			_ = cmd.Wait()
			// Read the final confirmed set (the child may have appended more
			// between the wait and the kill).
			confirmed := readConfirmedFile(progress)

			wm, err := NewWALManagerWithConfig(dir, walCrashConfig(mode))
			require.NoError(t, err, "recovery after a SIGKILL must succeed (bounded tail benign)")
			defer func() { _ = wm.Close() }()

			recoveredMsgs, rerr := wm.RecoverFromWAL()
			require.NoError(t, rerr)
			recovered := map[uint64]bool{}
			var maxRecovered uint64
			for _, m := range recoveredMsgs {
				recovered[m.Offset] = true
				if m.Offset > maxRecovered {
					maxRecovered = m.Offset
				}
			}

			var maxConfirmed uint64
			for off := range confirmed {
				require.True(t, recovered[off],
					"a confirmed offset (%d) must be recovered; confirmed=%v recovered=%v",
					off, sortedKeys(confirmed), sortedKeys(recovered))
				if off > maxConfirmed {
					maxConfirmed = off
				}
			}
			require.NotEmpty(t, confirmed, "the child must have confirmed at least one record")
			require.LessOrEqual(t, maxRecovered, maxConfirmed+1,
				"recovery must not invent more than one record past the last confirm "+
					"(maxRecovered=%d maxConfirmed=%d)", maxRecovered, maxConfirmed)
		})
	}
}

// walCrashChild is the child body: publish records forever with synchronous
// confirms, appending each confirmed offset to the progress file.
func walCrashChild() {
	dir := os.Getenv(envCrashDir)
	mode := os.Getenv(envCrashMode)
	progress := os.Getenv(envCrashProgress)

	wm, err := NewWALManagerWithConfig(dir, walCrashConfig(mode))
	if err != nil {
		fmt.Fprintln(os.Stderr, "child: open WAL:", err)
		os.Exit(2)
	}
	pf, err := os.OpenFile(progress, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child: open progress:", err)
		os.Exit(2)
	}
	defer pf.Close()
	w := bufio.NewWriter(pf)

	for off := uint64(1); off < 5_000_000; off++ {
		msg := &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "c",
			Body:         []byte(fmt.Sprintf("crash-child-record-%d", off)),
			DeliveryMode: 2, DeliveryTag: off,
		}
		if werr := wm.Write("c", msg, off); werr != nil {
			fmt.Fprintln(os.Stderr, "child: write:", werr)
			os.Exit(3)
		}
		// The confirm has returned: this offset is confirmed.
		fmt.Fprintf(w, "%d\n", off)
		if err := w.Flush(); err != nil {
			os.Exit(3)
		}
	}
}

func waitForConfirmed(t *testing.T, progress string, want int, timeout time.Duration) map[uint64]bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := readConfirmedFile(progress)
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("child confirmed only %d records within %s", len(got), timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func readConfirmedFile(progress string) map[uint64]bool {
	out := map[uint64]bool{}
	f, err := os.Open(progress)
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if n, err := strconv.ParseUint(line, 10, 64); err == nil {
			out[n] = true
		}
	}
	return out
}

func sortedKeys(m map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
