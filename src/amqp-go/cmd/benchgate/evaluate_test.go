package main

import "testing"

// TestEvaluate_SecOp_CatchesRegressionOverThreshold is the unit-level proof
// that a >2% significant sec/op delta fails the gate — the same rule
// exercised end-to-end in the injected-regression proof documented in the
// W0 report.
func TestEvaluate_SecOp_CatchesRegressionOverThreshold(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Foo-8", significant: true, deltaPct: 6.0},
	}
	got := evaluate("pkg", results, 2.0)
	if len(got) != 1 {
		t.Fatalf("got %d regressions, want 1: %+v", len(got), got)
	}
	if got[0].name != "Foo-8" || got[0].deltaPct != 6.0 {
		t.Errorf("got %+v", got[0])
	}
}

// TestEvaluate_SecOp_PassesUnderThreshold proves the gate does not fire on
// a significant but sub-2% delta (the "one atomic load" case the design
// doc's standing performance gate is calibrated for).
func TestEvaluate_SecOp_PassesUnderThreshold(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Foo-8", significant: true, deltaPct: 1.5},
	}
	if got := evaluate("pkg", results, 2.0); len(got) != 0 {
		t.Fatalf("got %d regressions, want 0: %+v", len(got), got)
	}
}

// TestEvaluate_SecOp_PassesAtExactThreshold: the gate's own wording is
// "any benchmark mean delta > +2%" — strictly greater than, so exactly 2.0%
// must pass.
func TestEvaluate_SecOp_PassesAtExactThreshold(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Foo-8", significant: true, deltaPct: 2.0},
	}
	if got := evaluate("pkg", results, 2.0); len(got) != 0 {
		t.Fatalf("got %d regressions at exactly the threshold, want 0: %+v", len(got), got)
	}
}

// TestEvaluate_SecOp_NonSignificantNeverFails proves noise (a large raw
// delta that benchstat itself called "~") never fails the gate — this is
// what -count=10 and benchstat's two-sample test are for, versus a naive
// raw-percentage-only check.
func TestEvaluate_SecOp_NonSignificantNeverFails(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Foo-8", significant: false, deltaPct: 0},
	}
	if got := evaluate("pkg", results, 2.0); len(got) != 0 {
		t.Fatalf("got %d regressions for a non-significant result, want 0: %+v", len(got), got)
	}
}

// TestEvaluate_SecOp_ImprovementNeverFails: a significant DECREASE
// (deltaPct negative) is an improvement, never a regression, regardless of
// magnitude.
func TestEvaluate_SecOp_ImprovementNeverFails(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Foo-8", significant: true, deltaPct: -15.0},
	}
	if got := evaluate("pkg", results, 2.0); len(got) != 0 {
		t.Fatalf("got %d regressions for an improvement, want 0: %+v", len(got), got)
	}
}

// TestEvaluate_AllocsOp_AnySignificantIncreaseFails proves allocs/op (and
// B/op) are gated by significance alone, not a magnitude threshold — brief
// point 4: "not a raw 0-delta" (existing benches jitter ±1 alloc) but ANY
// benchstat-significant increase, however small the percentage, is still a
// real allocation added on a path that is supposed to add none when its
// feature is unset.
func TestEvaluate_AllocsOp_AnySignificantIncreaseFails(t *testing.T) {
	results := []benchResult{
		{metric: "allocs/op", name: "Foo-8", significant: true, deltaPct: 1.0}, // well under the 2% sec/op threshold
	}
	got := evaluate("pkg", results, 2.0)
	if len(got) != 1 {
		t.Fatalf("got %d regressions, want 1 (allocs/op has no magnitude floor): %+v", len(got), got)
	}
}

// TestEvaluate_AllocsOp_JitterNeverFails proves the ±1-alloc jitter the
// brief calls out (existing benches) is absorbed by benchstat's own
// significance test, not by this function re-deriving a tolerance.
func TestEvaluate_AllocsOp_JitterNeverFails(t *testing.T) {
	results := []benchResult{
		{metric: "allocs/op", name: "Foo-8", significant: false, deltaPct: 8.0}, // benchstat called this "~"
		{metric: "B/op", name: "Bar-8", significant: false, deltaPct: 10.0},
	}
	if got := evaluate("pkg", results, 2.0); len(got) != 0 {
		t.Fatalf("got %d regressions for non-significant B/op and allocs/op noise, want 0: %+v", len(got), got)
	}
}

// TestEvaluate_MissingFromNew_HardFailsOnce proves a benchmark dropped from
// the new run (present in the baseline, absent here) always fails the gate,
// and is reported once per benchmark rather than once per metric table even
// though statusMissingFromNew rows appear in all three (sec/op, B/op,
// allocs/op).
func TestEvaluate_MissingFromNew_HardFailsOnce(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Dropped-8", status: statusMissingFromNew},
		{metric: "B/op", name: "Dropped-8", status: statusMissingFromNew},
		{metric: "allocs/op", name: "Dropped-8", status: statusMissingFromNew},
	}
	got := evaluate("pkg", results, 2.0)
	if len(got) != 1 {
		t.Fatalf("got %d regressions, want 1 (deduplicated across metric tables): %+v", len(got), got)
	}
	if got[0].name != "Dropped-8" {
		t.Errorf("got %+v", got[0])
	}
}

// TestEvaluate_MissingFromBaseline_NeverFails proves a benchmark newly
// added in this run (not yet in the baseline) never fails — it's
// informational, so adding coverage doesn't self-block before
// make bench-baseline-refresh is run.
func TestEvaluate_MissingFromBaseline_NeverFails(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "New-8", status: statusMissingFromBaseline},
	}
	if got := evaluate("pkg", results, 2.0); len(got) != 0 {
		t.Fatalf("got %d regressions for a new, not-yet-baselined benchmark, want 0: %+v", len(got), got)
	}
}

func TestEvaluate_MultipleBenchmarks_OnlyFailingOnesReported(t *testing.T) {
	results := []benchResult{
		{metric: "sec/op", name: "Fast-8", significant: true, deltaPct: 0.5},
		{metric: "sec/op", name: "Slow-8", significant: true, deltaPct: 12.0},
		{metric: "allocs/op", name: "Fast-8", significant: false, deltaPct: 0},
	}
	got := evaluate("pkg", results, 2.0)
	if len(got) != 1 {
		t.Fatalf("got %d regressions, want 1: %+v", len(got), got)
	}
	if got[0].name != "Slow-8" {
		t.Errorf("got regression for %q, want Slow-8", got[0].name)
	}
}
