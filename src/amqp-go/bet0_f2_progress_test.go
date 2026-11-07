package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// E11 (R4) — an OPT-IN, strictly-conjunctive deficit detector for F2.
//
// This is NOT the determinism fix and it is NOT the verdict: it can only ADD a
// failure, and only when bet0CheckLiveness already PASSED. The balance verdict
// is never suppressed (TestBet0_DeficitVerdictCannotSuppressBalance).
//
// The sampler is a 100ms wall-clock ticker and is therefore a machine-speed
// surface exactly like the ones this task removes, so it MUST NEVER be enabled
// in CI or in the §5.6 determinism soak. It runs only when STRANGEQ_F2_DIAG is
// set, and bet0F2SeriesIfDiagnosticEnabled returns nil otherwise.
// ============================================================================

// bet0Interval is one cumulative sample of both queues' counters.
type bet0Interval struct{ published, consumed [bet0Queues]int64 }

const (
	bet0DeficitInterval = 100 * time.Millisecond
	bet0DeficitAlpha    = 0.50
	bet0DeficitRun      = 20   // 2s of consecutive permitted deficit
	bet0DeficitBacklog  = 1000 // per-queue published-consumed for "backlogged"
)

// bet0F2DiagnosticEnabled is true only when STRANGEQ_F2_DIAG is set.
func bet0F2DiagnosticEnabled() bool {
	_, ok := os.LookupEnv("STRANGEQ_F2_DIAG")
	return ok
}

var (
	bet0F2DiagMu     sync.Mutex
	bet0F2DiagSeries []bet0Interval
)

// bet0F2StartSampler starts a 100ms ticker that records cumulative
// published/consumed per queue while a run is in flight. It is a no-op unless
// STRANGEQ_F2_DIAG is set. The returned func stops the sampler and waits for it.
func bet0F2StartSampler(results []*bet0LoadResult) (stop func()) {
	if !bet0F2DiagnosticEnabled() {
		return func() {}
	}
	bet0F2DiagMu.Lock()
	bet0F2DiagSeries = nil
	bet0F2DiagMu.Unlock()

	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(bet0DeficitInterval)
		defer tick.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-tick.C:
				iv := bet0Interval{}
				for i, r := range results {
					iv.published[i] = r.published.Load()
					iv.consumed[i] = r.consumed.Load()
				}
				bet0F2DiagMu.Lock()
				bet0F2DiagSeries = append(bet0F2DiagSeries, iv)
				bet0F2DiagMu.Unlock()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stopCh)
			<-done
		})
	}
}

// bet0F2SeriesIfDiagnosticEnabled returns the recorded series, or nil when the
// diagnostic is off.
func bet0F2SeriesIfDiagnosticEnabled() []bet0Interval {
	if !bet0F2DiagnosticEnabled() {
		return nil
	}
	bet0F2DiagMu.Lock()
	defer bet0F2DiagMu.Unlock()
	out := make([]bet0Interval, len(bet0F2DiagSeries))
	copy(out, bet0F2DiagSeries)
	return out
}

// bet0DeficitVerdict fires when, over >=bet0DeficitRun consecutive intervals,
// (a) BOTH queues are backlogged and the low/high consumed-delta ratio is below
// bet0DeficitAlpha, or (b) a backlogged queue consumed nothing. Pure: no clock,
// no broker. It never fires when either queue is not backlogged, so the balance
// premise stays falsifiable.
func bet0DeficitVerdict(series []bet0Interval, queues int) error {
	if queues < 2 || len(series) < 2 {
		return nil
	}
	partialRun, deadRun := 0, 0
	for i := 1; i < len(series); i++ {
		prev, cur := series[i-1], series[i]
		bothBacklogged := true
		lo, hi := int64(-1), int64(0)
		anyDead := false
		for q := 0; q < queues; q++ {
			backlog := cur.published[q] - cur.consumed[q]
			if backlog < bet0DeficitBacklog {
				bothBacklogged = false
			}
			dc := cur.consumed[q] - prev.consumed[q]
			if dc < 0 {
				dc = 0
			}
			if lo < 0 || dc < lo {
				lo = dc
			}
			if dc > hi {
				hi = dc
			}
			if dc == 0 && backlog >= bet0DeficitBacklog {
				anyDead = true
			}
		}
		if !bothBacklogged {
			partialRun, deadRun = 0, 0
			continue
		}
		if anyDead {
			deadRun++
		} else {
			deadRun = 0
		}
		if hi > 0 && float64(lo)/float64(hi) < bet0DeficitAlpha {
			partialRun++
		} else {
			partialRun = 0
		}
	}
	if deadRun >= bet0DeficitRun {
		return fmt.Errorf("DELIVERY DEAD: a backlogged queue consumed nothing for %d consecutive intervals (%s each)",
			deadRun, bet0DeficitInterval)
	}
	if partialRun >= bet0DeficitRun {
		return fmt.Errorf("IMBALANCE: sustained per-queue dispatch deficit for %d consecutive intervals (%s each)",
			partialRun, bet0DeficitInterval)
	}
	return nil
}

// bet0BuildSeries builds a cumulative series: an initial backlog, then `steps`
// intervals adding dp/dc per queue.
func bet0BuildSeries(startPub, startCon, dp, dc [bet0Queues]int64, steps int) []bet0Interval {
	series := make([]bet0Interval, steps+1)
	series[0].published, series[0].consumed = startPub, startCon
	for i := 1; i <= steps; i++ {
		for q := 0; q < bet0Queues; q++ {
			series[i].published[q] = series[i-1].published[q] + dp[q]
			series[i].consumed[q] = series[i-1].consumed[q] + dc[q]
		}
	}
	return series
}

// TestBet0_DeficitVerdictDiscriminates: synthetic series, table.
func TestBet0_DeficitVerdictDiscriminates(t *testing.T) {
	const steps = 30 // > bet0DeficitRun
	backlogStartPub := [bet0Queues]int64{100000, 100000}
	zero := [bet0Queues]int64{0, 0}

	cases := []struct {
		name    string
		series  []bet0Interval
		wantErr string // "" means nil
	}{
		{
			name:   "balanced",
			series: bet0BuildSeries(backlogStartPub, zero, [bet0Queues]int64{1000, 1000}, [bet0Queues]int64{400, 400}, steps),
		},
		{
			name:    "partial starvation",
			series:  bet0BuildSeries(backlogStartPub, zero, [bet0Queues]int64{1000, 1000}, [bet0Queues]int64{300, 100}, steps),
			wantErr: "IMBALANCE",
		},
		{
			name:    "total starvation",
			series:  bet0BuildSeries(backlogStartPub, zero, [bet0Queues]int64{1000, 1000}, [bet0Queues]int64{300, 0}, steps),
			wantErr: "DELIVERY DEAD",
		},
		{
			name:   "uniform collapse",
			series: bet0BuildSeries(backlogStartPub, zero, [bet0Queues]int64{2, 2}, [bet0Queues]int64{1, 1}, steps),
		},
		{
			name:   "out of regime: not backlogged",
			series: bet0BuildSeries([bet0Queues]int64{100, 100}, zero, [bet0Queues]int64{10, 10}, [bet0Queues]int64{1, 3}, steps),
		},
		{
			name:   "too short",
			series: bet0BuildSeries(backlogStartPub, zero, [bet0Queues]int64{1000, 1000}, [bet0Queues]int64{300, 100}, bet0DeficitRun-1),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := bet0DeficitVerdict(tc.series, bet0Queues)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want no deficit verdict, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want a %q verdict, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want a %q verdict, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

// TestBet0_DeficitVerdictCannotSuppressBalance: the harness consults the deficit
// predicate ONLY after liveness passed, so liveness's IMBALANCE is always the
// returned verdict — even when a deficit series would also fire.
func TestBet0_DeficitVerdictCannotSuppressBalance(t *testing.T) {
	// The balance term is what must not be suppressed, so use an explicitly
	// balance-enabled policy. bet0Policy() only enables minBalance under -race
	// (the instrumented path); pinning it here makes the test deterministic on
	// both builds instead of passing only under the race detector.
	policy := bet0Policy()
	policy.minBalance = bet0MinQueueBalance
	policy.minRateRatio = 0
	rows := []struct {
		name    string
		samples []bet0Sample
	}{
		{
			"build-split :790",
			[]bet0Sample{
				{name: "q1", published: 1000000, consumed: 900000},
				{name: "q2", published: 100000, consumed: 90000},
			},
		},
		{
			"partial",
			[]bet0Sample{
				{name: "q1", published: 100000, consumed: 90000},
				{name: "q2", published: 100000, consumed: 35000},
			},
		},
	}
	// A series the deficit predicate WOULD reject, to show it cannot override.
	deficitSeries := bet0BuildSeries(
		[bet0Queues]int64{100000, 100000}, [bet0Queues]int64{0, 0},
		[bet0Queues]int64{1000, 1000}, [bet0Queues]int64{300, 100}, 30)
	if err := bet0DeficitVerdict(deficitSeries, bet0Queues); err == nil {
		t.Fatal("premise: the deficit series must be rejected by bet0DeficitVerdict")
	}

	// The harness composition: liveness first, deficit only if liveness passed.
	compose := func(samples []bet0Sample) error {
		if err := bet0CheckLiveness(samples, policy); err != nil {
			return err
		}
		return bet0DeficitVerdict(deficitSeries, bet0Queues)
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			livenessErr := bet0CheckLiveness(row.samples, policy)
			if livenessErr == nil || !strings.Contains(livenessErr.Error(), "IMBALANCE") {
				t.Fatalf("premise: liveness must return IMBALANCE, got %v", livenessErr)
			}
			got := compose(row.samples)
			if got == nil || got.Error() != livenessErr.Error() {
				t.Fatalf("liveness verdict must be returned unchanged; got %v want %v", got, livenessErr)
			}
		})
	}
}

// TestBet0_DeficitVerdictOffByDefault proves the sampler is not enabled without
// the env var, so CI and the determinism soak never pay for it.
func TestBet0_DeficitVerdictOffByDefault(t *testing.T) {
	if _, set := os.LookupEnv("STRANGEQ_F2_DIAG"); set {
		t.Skip("STRANGEQ_F2_DIAG is set; this control only applies with it unset")
	}
	results := []*bet0LoadResult{{}, {}}
	stop := bet0F2StartSampler(results)
	stop()
	if got := bet0F2SeriesIfDiagnosticEnabled(); got != nil {
		t.Fatalf("diagnostic must be off by default, got %d intervals", len(got))
	}
}
