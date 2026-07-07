package main

import "flag"

// gateConfig holds benchgate's resolved CLI flags.
type gateConfig struct {
	baselinePath string
	newPath      string
	toolsDir     string
	scratchDir   string
	maxDeltaPct  float64
	alpha        float64
}

// parseFlags parses benchgate's CLI flags. Defaults match how `make
// bench-gate` invokes it (see Makefile): baseline testdata/bench-baseline.txt,
// new build/bench/new.txt, tools dir "tools", scratch dir build/bench/split.
func parseFlags(args []string) (gateConfig, error) {
	fs := flag.NewFlagSet("benchgate", flag.ContinueOnError)
	cfg := gateConfig{}
	fs.StringVar(&cfg.baselinePath, "baseline", "testdata/bench-baseline.txt", "path to the committed baseline benchmark output")
	fs.StringVar(&cfg.newPath, "new", "build/bench/new.txt", "path to the HEAD benchmark output to gate")
	fs.StringVar(&cfg.toolsDir, "tools-dir", "tools", "directory of the isolated Go module pinning golang.org/x/perf/cmd/benchstat")
	fs.StringVar(&cfg.scratchDir, "scratch-dir", "build/bench/split", "scratch directory for per-package benchstat input files (not committed)")
	fs.Float64Var(&cfg.maxDeltaPct, "max-delta-pct", 2.0, "maximum allowed significant sec/op mean regression, in percent")
	fs.Float64Var(&cfg.alpha, "alpha", 0.05, "significance level passed to benchstat's two-sample test")

	if err := fs.Parse(args); err != nil {
		return gateConfig{}, err
	}
	return cfg, nil
}
