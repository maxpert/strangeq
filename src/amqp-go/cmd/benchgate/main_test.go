package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// realTools points at the actual pinned benchstat module so these
// integration tests exercise the real subprocess, not a stub. Tests run
// with cwd = this package directory (cmd/benchgate).
const realToolsDir = "../../tools"

// benchLines renders n synthetic go-test-benchmark result lines for name,
// centered on baseNs nanoseconds with the given per-sample deltas (so
// callers control exactly how much jitter/shift each sample has, instead of
// relying on randomness that would make a "does this trip the gate" test
// flaky).
func benchLines(name string, baseNs float64, deltasNs []float64) string {
	var sb strings.Builder
	for _, d := range deltasNs {
		fmt.Fprintf(&sb, "%s-8    	   10000	       %.4f ns/op	      64 B/op	       2 allocs/op\n", name, baseNs+d)
	}
	return sb.String()
}

func pkgBlock(pkg, benchBody string) string {
	return "goos: darwin\ngoarch: arm64\npkg: " + pkg + "\ncpu: Test CPU\n" + benchBody + "PASS\nok  \t" + pkg + "\t0.01s\n"
}

// tenJitterSamples is a fixed, non-random ±1% jitter pattern with no true
// mean shift, used for both baseline and new to prove noise never trips the
// gate (TestRun_PassesWithNoiseOnly / TestRun_SelfComparisonAlwaysPasses).
func tenJitterSamples(baseNs float64) []float64 {
	pct := []float64{0, 1, -1, 0.5, -0.5, 0.8, -0.8, 0.2, -0.2, 0}
	out := make([]float64, len(pct))
	for i, p := range pct {
		out[i] = baseNs * p / 100
	}
	return out
}

func TestRun_CatchesInjectedRegressionOverThreshold(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlock("pkg/regressed", benchLines("BenchmarkFoo", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/clean", benchLines("BenchmarkBar", 50, tenJitterSamples(50)))

	// pkg/regressed shifts its mean by +10% (well over the 2% gate);
	// pkg/clean keeps the same jitter pattern with no true shift.
	shifted := make([]float64, 10)
	for i, d := range tenJitterSamples(100) {
		shifted[i] = d + 10 // +10ns on a ~100ns base ≈ +10%
	}
	newRun := pkgBlock("pkg/regressed", benchLines("BenchmarkFoo", 100, shifted)) +
		pkgBlock("pkg/clean", benchLines("BenchmarkBar", 50, tenJitterSamples(50)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("got exit code %d, want 1 (regression must fail the gate)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "pkg/regressed") || !strings.Contains(stderr.String(), "Foo") {
		t.Errorf("stderr does not identify the regressed package/benchmark:\n%s", stderr.String())
	}
	if strings.Contains(stderr.String(), "pkg/clean") {
		t.Errorf("clean package was reported as a regression:\n%s", stderr.String())
	}
}

func TestRun_PassesWithNoiseOnly(t *testing.T) {
	dir := t.TempDir()

	// Same jitter pattern on both sides, no true mean shift, for two
	// packages sharing a benchmark NAME — also proves per-package
	// separation doesn't accidentally cross-contaminate a clean result.
	baseline := pkgBlock("pkg/a", benchLines("BenchmarkSame", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/b", benchLines("BenchmarkSame", 100, tenJitterSamples(100)))
	newRun := pkgBlock("pkg/a", benchLines("BenchmarkSame", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/b", benchLines("BenchmarkSame", 100, tenJitterSamples(100)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("got exit code %d, want 0 (identical noise must pass)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func TestRun_MissingBaselineSectionIsNotFatal(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlock("pkg/old", benchLines("BenchmarkOld", 100, tenJitterSamples(100)))
	// "new" introduces a brand-new package the baseline has never seen (e.g.
	// a benchmark just added in this PR) — must be skipped with a note, not
	// treated as a gate failure, so adding coverage doesn't self-block.
	newRun := pkgBlock("pkg/old", benchLines("BenchmarkOld", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/new", benchLines("BenchmarkNew", 100, tenJitterSamples(100)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("got exit code %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "pkg/new") {
		t.Errorf("expected a note about the uncovered new package, got stdout:\n%s", stdout.String())
	}
}

func writeFileT(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}
