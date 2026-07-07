package main

import "fmt"

// regression is one benchmark/metric combination that fails the
// no-regression gate for a given package. reason overrides the default
// "regressed X%" wording for non-magnitude failures (e.g. a benchmark
// dropped from the new run).
type regression struct {
	pkg      string
	metric   string
	name     string
	deltaPct float64
	reason   string
}

func (r regression) String() string {
	if r.reason != "" {
		return fmt.Sprintf("%s: %s %s", r.pkg, r.name, r.reason)
	}
	return fmt.Sprintf("%s: %s %s regressed %+.2f%%", r.pkg, r.name, r.metric, r.deltaPct)
}

// evaluate applies the gate's threshold rule to one package's parsed
// benchstat results and returns every failing (benchmark, metric) pair.
// Rows benchstat itself called non-significant ("~"), and rows whose delta
// is negative (an improvement, not a regression), never fail.
//
// A benchmark present in the baseline but missing from this run
// (statusMissingFromNew) is ALWAYS a hard failure, deduplicated to one
// message per benchmark rather than one per metric table: silently letting
// it through would let a gated benchmark evade the gate by disappearing
// (dropped from a -bench regex, deleted, renamed) instead of regressing. A
// benchmark present in this run but not yet in the baseline
// (statusMissingFromBaseline) never fails — see parse.go.
//
// sec/op (latency/throughput): fails only once the increase is BOTH
// benchstat-significant AND exceeds maxDeltaPct. A single always-on atomic
// load is real cost but sub-2%, and separating that from run-to-run noise
// is exactly what -count=10 plus benchstat's two-sample test is for (design
// doc §1, "<2% regression... with x-args absent").
//
// Every other per-op metric (B/op, allocs/op, and any future benchstat unit):
// fails on ANY significant increase, not a magnitude threshold — "1
// allocs/op vs 2 allocs/op" has no meaningful percentage step, and the
// standing performance gate requires a feature-unset hot path to add zero
// allocations, not "under 2% more" allocations (brief point 4: gate
// allocs/op via the two-sample test, not a raw 0-delta, since existing
// benches jitter ±1 alloc — benchstat's significance test is what absorbs
// that jitter, not a magnitude tolerance).
func evaluate(pkg string, results []benchResult, maxDeltaPct float64) []regression {
	var regressions []regression
	droppedAlready := make(map[string]bool)

	for _, r := range results {
		switch r.status {
		case statusMissingFromNew:
			if droppedAlready[r.name] {
				continue
			}
			droppedAlready[r.name] = true
			regressions = append(regressions, regression{
				pkg:  pkg,
				name: r.name,
				reason: "is in the baseline but missing from this run" +
					" (deleted, or dropped from a -bench filter)",
			})
			continue
		case statusMissingFromBaseline:
			continue
		}

		if !r.significant || r.deltaPct <= 0 {
			continue
		}
		isRegression := r.metric == "sec/op" && r.deltaPct > maxDeltaPct
		if r.metric != "sec/op" {
			isRegression = true
		}
		if isRegression {
			regressions = append(regressions, regression{
				pkg:      pkg,
				metric:   r.metric,
				name:     r.name,
				deltaPct: r.deltaPct,
			})
		}
	}
	return regressions
}
