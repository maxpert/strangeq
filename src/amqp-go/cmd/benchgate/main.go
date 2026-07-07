// Command benchgate is the no-regression referee for StrangeQ's Wave 2 perf
// gate (design doc "W0 — Perf-gate harness"). It compares a HEAD benchmark
// run against the committed baseline (testdata/bench-baseline.txt) and
// fails if any in-scope benchmark's mean regresses by more than
// -max-delta-pct at p<0.05, or if any allocs/op (or other non-latency /op
// metric) increases at all, benchstat-significantly.
//
// Both inputs may contain benchmarks from several packages appended
// together (see the Makefile's bench-gate/bench-baseline-refresh targets,
// one `go test <pkg> ...` invocation per in-scope package). benchgate splits
// each input by its "pkg:" markers and runs a SEPARATE benchstat comparison
// per package, so two benchmarks that happen to share a name in different
// packages are never compared against each other.
//
// Usage:
//
//	go run ./cmd/benchgate -baseline testdata/bench-baseline.txt -new build/bench/new.txt
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's testable body: it returns a process exit code instead of
// calling os.Exit directly, and writes to the given streams instead of
// os.Stdout/os.Stderr so tests can capture output.
func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseFlags(args)
	if err != nil {
		fmt.Fprintln(stderr, "benchgate:", err)
		return 2
	}

	baselineData, err := os.ReadFile(cfg.baselinePath)
	if err != nil {
		fmt.Fprintf(stderr, "benchgate: reading baseline %s: %v\n", cfg.baselinePath, err)
		return 2
	}
	newData, err := os.ReadFile(cfg.newPath)
	if err != nil {
		fmt.Fprintf(stderr, "benchgate: reading new run %s: %v\n", cfg.newPath, err)
		return 2
	}

	baselineByPkg, _ := mergeSections(splitByPackage(baselineData))
	newByPkg, newOrder := mergeSections(splitByPackage(newData))

	if err := os.MkdirAll(cfg.scratchDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "benchgate: creating scratch dir %s: %v\n", cfg.scratchDir, err)
		return 2
	}

	var allRegressions []regression
	anyCompared := false
	for _, pkg := range newOrder {
		baselineBody, ok := baselineByPkg[pkg]
		if !ok {
			fmt.Fprintf(stdout, "benchgate: %s: no baseline section found — new benchmark(s) not yet in testdata/bench-baseline.txt; skipping (run make bench-baseline-refresh once these are reviewed)\n", pkg)
			continue
		}

		csvOut, err := runBenchstat(cfg.toolsDir, cfg.scratchDir, pkg, baselineBody, newByPkg[pkg], cfg.alpha)
		if err != nil {
			fmt.Fprintf(stderr, "benchgate: %s: running benchstat: %v\n", pkg, err)
			return 2
		}
		anyCompared = true

		results, err := parseBenchstatCSV(csvOut)
		if err != nil {
			fmt.Fprintf(stderr, "benchgate: %s: %v\n", pkg, err)
			return 2
		}

		regressions := evaluate(pkg, results, cfg.maxDeltaPct)
		allRegressions = append(allRegressions, regressions...)

		if len(regressions) == 0 {
			fmt.Fprintf(stdout, "benchgate: %s: PASS (%d benchmark result rows compared)\n", pkg, len(results))
		}
	}

	for pkg := range baselineByPkg {
		if _, ok := newByPkg[pkg]; !ok {
			fmt.Fprintf(stdout, "benchgate: %s: in baseline but not in this run — benchmark removed or -bench filter changed\n", pkg)
		}
	}

	if len(allRegressions) > 0 {
		fmt.Fprintln(stderr, "benchgate: FAIL — regression(s) exceeding the gate:")
		for _, r := range allRegressions {
			fmt.Fprintln(stderr, "  "+r.String())
		}
		return 1
	}

	if !anyCompared {
		fmt.Fprintln(stderr, "benchgate: no package present in both baseline and new run — nothing was actually gated")
		return 2
	}

	fmt.Fprintln(stdout, "benchgate: PASS — no regression exceeding the gate")
	return 0
}

// runBenchstat writes baselineBody/newBody to pkg-scoped scratch files and
// invokes the pinned benchstat (tools/go.mod) on them, returning its
// -format=csv stdout.
func runBenchstat(toolsDir, scratchDir, pkg string, baselineBody, newBody []byte, alpha float64) ([]byte, error) {
	safe := sanitizePkgName(pkg)
	oldFile := filepath.Join(scratchDir, safe+"-baseline.txt")
	newFile := filepath.Join(scratchDir, safe+"-new.txt")

	if err := os.WriteFile(oldFile, baselineBody, 0o644); err != nil {
		return nil, fmt.Errorf("writing %s: %w", oldFile, err)
	}
	if err := os.WriteFile(newFile, newBody, 0o644); err != nil {
		return nil, fmt.Errorf("writing %s: %w", newFile, err)
	}

	absOld, err := filepath.Abs(oldFile)
	if err != nil {
		return nil, err
	}
	absNew, err := filepath.Abs(newFile)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("go", "run", "-C", toolsDir,
		"golang.org/x/perf/cmd/benchstat",
		"-format=csv",
		"-alpha", fmt.Sprintf("%g", alpha),
		absOld, absNew,
	)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("benchstat: %w\n%s", err, exitErr.Stderr)
		}
		return nil, fmt.Errorf("benchstat: %w", err)
	}
	return out, nil
}

// sanitizePkgName turns an import path into a filesystem-safe file name
// stem (import paths contain '/', which would otherwise create unwanted
// subdirectories under scratchDir).
func sanitizePkgName(pkg string) string {
	out := make([]rune, 0, len(pkg))
	for _, r := range pkg {
		switch r {
		case '/', '\\', ':', ' ':
			out = append(out, '_')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}
