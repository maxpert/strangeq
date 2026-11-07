package main

import (
	"fmt"
	"strings"
	"testing"
)

// ----------------------------------------------------------------------------
// The multi-queue liveness predicate — THIS IS THE P0 GATE.
// ----------------------------------------------------------------------------
//
// TestBet0_ConcurrentMultiQueueDelivery and TestBet0_MultiQueueWithConfirmsDelivers
// both assert through bet0CheckLiveness. It is the assertion that proves
// multi-queue delivery starvation is dead.
//
// ⛔ READ THIS BEFORE RETUNING ANY CONSTANT BELOW. ⛔
//
// The natural way to stop this test flaking is to loosen a threshold until it
// goes quiet. That retires the detector while leaving the suite green, which is
// strictly worse than a red test. Every constant here has a MEASURED band on
// both sides and the measurements are recorded in the comment on each. If this
// flakes, the answer is to find out why the broker's numbers moved, not to move
// the number.
//
// WHY THERE ARE THREE TERMS AND NOT ONE.
//
// Under the race detector, consumed/published stops being a liveness measure.
// Publishers run unthrottled in a tight loop, so the ratio measures the
// publish:consume SPEED DIFFERENTIAL, and instrumentation moves that
// differential without any queue being starved. The measured bands overlap:
//
//	HEALTHY tree, CI run 116 under -race:   0.1397 .. 0.4359
//	BROKEN  tree (8a65f98) under -race:     0.0921 .. 0.1520  (surviving queue)
//
// A broken tree's surviving queue scores 0.1089 while a healthy CI run scores
// 0.1397. The bands interleave, and the broken value is the HIGHER-scoring
// queue of a tree whose neighbour is at absolute zero. So under -race no rate
// floor can separate "degraded but alive" from "healthy": a floor can detect
// DEATH, never HEALTH. Hence:
//
//	minRatio   (L) — LIVENESS. "this queue is not dead." Never "fast enough".
//	minBalance (B) — the DIAGNOSTIC. One queue materially below its neighbour.
//	minRateRatio   — the real "keeping up" floor. Only an uninstrumented run
//	                 can support it, so it is disabled under -race.
//
// L and B cover precisely each other's blind spots. L cannot catch gate §11's
// partial case (one queue at 0.35 beside a healthy neighbour sails over any
// usable floor); B catches it at 0.35/0.90 = 0.389. B cannot catch a UNIFORM
// collapse (both queues equally dead ⇒ balance ≈ 1.0 — the dossier's perftest
// shape, 558 consumed of 1.67M across two queues); L catches it at ~0.0003.
// Neither term is redundant. Removing either one opens a hole with a name.
//
// BALANCE IS COMPUTED ON CONSUMED COUNTS, NOT ON consumed/published RATIOS.
//
// This matters, and it is the one place where getting the metric wrong costs
// almost all the margin. Dividing by `published` imports publisher-side noise
// into what is supposed to be a delivery-liveness assertion. Measured on the
// fixed tree, -race, GOMAXPROCS=4, `mixed` shape: q1's producers published
// 237,201 while q2's published 117,345 — a 2x spread, because a durable queue
// and a transient queue apply different backpressure to their PRODUCERS. Over
// the same window the two queues consumed 42,353 and 40,100: the dispatch
// planes did nearly identical work. Ratio-balance calls that 0.5225; consumed
// balance calls it 0.9468. The first number is an artifact of the divisor.
//
// Across n=32 healthy observations on both trees:
//
//	metric            worst healthy   starved   margin at the 0.50 floor
//	ratio-balance          0.5152      0.0000       1.03x  — a coin flip
//	consumed-balance       0.746       0.0000       1.49x
//
// Consumed counts measure the invariant the test actually states: a queue's
// delivery liveness must not depend on whether OTHER, unrelated queues are
// being published to. Gate §11's partial case (0.35 beside 0.90 on equal
// published) trips at 0.389 under either metric, so nothing is given up.
//
// ⚠️ LOAD-BEARING PREMISE, stated because leaving it implicit was Loop 1's most
// common error class. Consumed-balance is valid only while ALL THREE hold:
//
//  1. Every queue has an identical consumer count and identical prefetch, so
//     comparable consumption is the expected healthy state.
//  2. Every queue is deeply backlogged, PER QUEUE (consumed << published), so
//     consumption measures dispatch capacity rather than offered load. One
//     queue going publish-limited is enough to break the term even if the run
//     looks backlogged in aggregate: a queue that published 12,001 and consumed
//     all of them beside a neighbour that consumed 30,000 reads as balance 0.40
//     on a perfectly healthy broker.
//  3. Every queue carries comparable PER-MESSAGE delivery cost — uniform body
//     size across queues. Consumed counts compare messages, not bytes; a
//     variant with 128 B on one queue and 64 KB on its neighbour would compare
//     unlike quantities and could read a healthy broker as imbalanced by the
//     size ratio. All current shapes satisfy this by construction (128 B on
//     both queues in the repro, 1 KB on both in the confirms test).
//
// Heterogeneous delivery PATHS are fine and are not a fourth condition: the
// `mixed` shape already has one — the durable queue's deliveries go through the
// WAL, the transient queue's do not — and consumed still tracks within 5%
// (42,353 vs 40,100). It is size, not path, that would break this.
//
// A variant breaking any of the three must RE-DERIVE this term, not re-tune its
// constant. Condition 2 is not merely documented — bet0PremiseNotice checks it
// at runtime, per queue, and says so out loud when it starts to erode. That is
// the difference between a premise and a comment: this one is falsifiable, and
// it has already been falsified once (it does not hold on the uninstrumented
// path, which is why balance is disabled there).
//
// The evidence behind the numbers, and the table test at the bottom of this
// file that keeps the predicate honest against every recorded dataset, is the
// standing double-guard: a mutation proof on the real broker PLUS a check that
// the predicate is actually discriminating, so a silently dead fixture cannot
// report success.
//
// ⛔ IF YOU ARE HERE BECAUSE "THE OLD TEST FAILED 3 SUBTESTS ON THE BROKEN TREE
// AND THIS ONE ONLY FAILS 2" — READ THIS BEFORE RESTORING ANYTHING.
//
// That comparison is real and its obvious conclusion is wrong. On the pre-fix
// tree (8a65f98) under -race, the old 0.50 rate floor reddened all three
// subtests. This predicate reds `durable` and `mixed` and leaves `transient`
// green. The difference is not lost coverage: **the old red on `transient` was a
// FALSE POSITIVE**, and one third of the evidence that the old predicate
// detected the P0 was never evidence at all.
//
// Measured on the broken tree, 4 independent runs: transient q1/q2 ratios
// 0.3847/0.3526, 0.3946/0.3693, 0.3632/0.3344, 0.3762/0.3361 — both queues
// alive, both delivering at ~35-39% of an unthrottled instrumented publisher,
// balanced to within 11%. There is no starvation on that shape on that tree. It
// failed the old floor purely because -race depresses the ratio, at 0.3847
// against 0.50.
//
// And that is expected, not lucky. This file's own header says all-transient
// "is carried as a guard that must stay healthy": on an empty WAL the cold-read
// miss returns fast enough that hole-crawl outruns tag-mint, so the P0 does not
// manifest there at all. A predicate that reds on `transient` is red for the
// wrong reason.
//
// The general lesson, which cost this loop real time to learn: **a test going
// red on a broken tree is not proof that it detects the break.** Check WHICH
// assertion fired and whether the mechanism is even present, before counting it
// as coverage.
//
// ⛔ WHAT THIS PREDICATE DOES NOT CATCH — READ BEFORE TRUSTING A GREEN CI RUN.
//
// Under -race, a regression that slows EVERY queue equally is invisible to this
// predicate. Broken-tree per-queue ratios (0.0921-0.1520) interleave with
// healthy CI ratios (0.1397), so no floor can separate them; balance reads ~1.0
// because the degradation is uniform. Only the uninstrumented 0.50 rate floor
// catches this class, and CI never runs uninstrumented. **CI green is therefore
// not evidence that uniform delivery degradation is absent.**
//
// Note this is a different hole from uniform COLLAPSE (both queues near zero),
// which the liveness term does catch at ~0.0003. The gap is uniform
// DEGRADATION — both queues alive but equally slowed.
//
// WHAT KIND OF EVIDENCE BACKS EACH TERM, stated because they are not equal:
// LIVENESS is proven against the real defect — it is the term that fires on the
// pre-fix tree, 6/6 reds in the pristine proof. BALANCE never fires there,
// because liveness is checked first and the starved queue sits at exactly zero.
// Balance is independently sufficient on that data (0/12538 = 0.000), but the
// pristine proof does not EXERCISE it. Its only active guard is the synthesized
// `partial starvation — 0.35 beside 0.90` row in the table test below, which is
// the one case where liveness passes and only balance can fire. So: liveness is
// proven against a measured defect, balance against a constructed one. Both
// earn their place — the L→0.0 mutation shows liveness alone misses uniform
// collapse — but do not read the pristine RED as evidence that balance works.

const (
	// bet0MinDeliveredFraction is the real liveness floor: each queue must
	// consume at least this fraction of what it published, DURING the publish
	// window. Healthy single-queue behaviour is ~0.97 and a broken broker is
	// ~0.0006, so uninstrumented this has three orders of magnitude of margin
	// and is not a tuning knob.
	//
	// ⛔ THIS VALUE IS FIXED AT 0.50 AND DOES NOT MOVE. ⛔ It is disabled under
	// -race (see bet0Policy) because instrumentation makes it undecidable, NOT
	// lowered — lowering it is the banned fix.
	bet0MinDeliveredFraction = 0.5

	// bet0MinLiveFraction (L) is the liveness backstop that replaces the floor
	// above when the race detector is on. It answers exactly one question: is
	// this queue completely dead?
	//
	// It is NOT a performance assertion and must never be read or retuned as
	// one. Band: the starved queue on the broken tree sits at literal
	// consumed=0 (8/8 across 4 pristine runs, durable q2 and mixed q2 every
	// run), while the worst healthy observation anywhere is 0.1397 (CI run 116,
	// mixed q1). 0.02 clears the worst healthy sample by 7x and the target is
	// zero.
	bet0MinLiveFraction = 0.02

	// bet0MinQueueBalance (B) is the diagnostic: min(consumed)/max(consumed)
	// across queues carrying identical load. This is what actually detects the
	// P0. Consumed counts, not ratios — see the header.
	//
	// Band, from n=32 healthy observations across both trees (CI run 116, two
	// local surveys, and a -count=5 run on the shipped predicate, all
	// GOMAXPROCS=4 under -race), against the case gate §11 requires us to
	// catch:
	//
	//	worst healthy observation (consumed-balance) .. 0.746
	//	partial starvation, 0.35 beside 0.90 .......... 0.389
	//	broken tree (8a65f98), durable and mixed ...... 0.0000
	//
	// 0.50 sits 1.29x above the regression it must catch and 1.49x below the
	// worst healthy sample. On the ratio metric the healthy side would be
	// 1.03x — see the header for why the metric, not the constant, is what
	// bought that margin.
	//
	// ⛔ If this flakes, DO NOT LOWER IT. Lowering B below ~0.39 retires the
	// partial-starvation catch, which is the entire purpose of this term.
	// Investigate why balance degraded instead.
	//
	// The `0.746` is the worst **recorded** healthy row, `CI 116/transient`
	// `{q1 60310/8675, q2 41183/6469}` at :569 (`6469/8675 = 0.7457`); the older
	// `0.8577 / 1.72×` was inconsistent with this file's own table. The observed
	// `0.4895` sits 34 % below the worst recorded healthy balance, not 43 %.
	//
	// Regime note (lead, 2026-11-06): 30/30 fast-box `-race` runs of
	// `…/durable` passed with balance `0.9683–0.9998` and consumed/published ratio
	// `≈0.72`, versus `0.08` in the one recorded CI failure. On a fast box the
	// consumers keep up and the test does not reach the deep-backlog,
	// consumer-bottlenecked state it was written to measure. This is a **power**
	// limitation, not a flakiness source: the assertion is unchanged and the
	// runtime premise check (`bet0PremiseNotice`, `:357-375`) still fires at the
	// `0.80` ceiling if that changes. See the spec §6 for why the plan does not
	// force the regime.
	bet0MinQueueBalance = 0.50

	// bet0MinPublishedForHealth is the PUBLISH-HEALTH floor, and it is a hard
	// failure. It is not a vacuity guard — bet0DemonstratedManifestationVolume
	// below covers that — it asserts that publishing did not catastrophically
	// collapse, which is a P0 symptom in its own right: the original defect
	// wedged producers at ring-full and dropped transient publish to ~250/s.
	// At 250/s a 4s window yields ~1000, an order of magnitude under this.
	//
	// It was previously 20000 and doing BOTH jobs, which is why no single value
	// worked: CI run 116 published 21506 on its slowest queue, a 7.5% margin
	// that would fire on any slower runner and redden CI for "load too low"
	// rather than for starvation.
	bet0MinPublishedForHealth = 5000 // per queue

	// bet0DemonstratedManifestationVolume is the lowest per-queue publish volume
	// at which the P0 has actually been OBSERVED to manifest on the pre-fix
	// tree: 38/38 observations from 28,398 upward, q2 consumed = 0 in every one,
	// across two GOMAXPROCS settings and five window lengths.
	//
	// ⚠️ CI RUNS BELOW THIS. Run 116's slowest queue published 21,506. Nothing
	// could be measured below 28,398: shortening the publish window does not
	// reduce volume (publishers saturate the ~65K ring immediately, so the
	// window only sets when the counters are sampled), and lowering GOMAXPROCS
	// RAISES volume, because starving the machine starves the dispatch side
	// while publishers only have to write to the ring.
	//
	// So manifestation at CI's actual operating volume is EXTRAPOLATED from a
	// rate-ratio mechanism (hole-crawl vs tag-mint), not demonstrated. It is
	// well motivated — q2 sits at exactly zero across a 4x volume range with no
	// trend toward recovery — but it is extrapolation. Hence a loud non-fatal
	// notice rather than either a silent pass or a permanent red.
	bet0DemonstratedManifestationVolume = 28398 // per queue

)

// bet0Sample is one queue's liveness measurement: publishes that returned nil
// and deliveries received and acked, sampled at the SAME instant with
// publishers still running. Consumption that only happens after publishing
// stops does not prove the broker can deliver WHILE it is being published to,
// which is the property under test.
type bet0Sample struct {
	name      string
	published int64
	consumed  int64
}

func (s bet0Sample) ratio() float64 {
	return float64(s.consumed) / float64(max64(s.published, 1))
}

// bet0LivenessPolicy is the set of thresholds bet0CheckLiveness enforces.
type bet0LivenessPolicy struct {
	// minPublished: PUBLISH-HEALTH floor. Each queue must have published MORE
	// than this or the run fails hard. This is not a vacuity guard — see
	// bet0SignalNotice for that — it catches catastrophic publish collapse,
	// which is a real P0 symptom in its own right.
	minPublished int64
	// minRatio (L): each queue must be delivering at all.
	minRatio float64
	// minBalance (B): min(consumed)/max(consumed) across queues. Zero DISABLES
	// this term, which is what a non-race build does — see bet0Policy.
	minBalance float64
	// minRateRatio: the full "keeping up" floor. Zero DISABLES this term, which
	// is what -race does — see bet0Policy.
	minRateRatio float64
}

// bet0Policy returns the thresholds for the standard multi-queue load shape.
//
// The two builds genuinely decide different things, and the policy says so
// rather than pretending otherwise. Vacuity and liveness run on both paths.
// The other two terms are mutually exclusive, each disabled on the path where
// its own premise fails:
//
//   - Under -race the RATE floor is undecidable: healthy and broken per-queue
//     ratios interleave (broken 0.0921-0.1520, healthy CI 0.1397).
//   - Without -race the BALANCE term degenerates. Balance on consumed counts is
//     only valid while the queues are BACKLOGGED. Uninstrumented the broker
//     keeps up — measured ratios 0.83-0.94 — so consumed ≈ published and
//     consumed-balance collapses into PUBLISHED-balance, which on the `mixed`
//     shape measures 0.596-0.616 while perfectly healthy. Worst observed
//     uninstrumented consumed-balance is 0.5488 against a 0.50 floor: 1.10x,
//     a coin flip.
//
// Nothing is lost by disabling balance uninstrumented, because the rate floor
// there is STRICTLY STRONGER: gate §11's partial case is one queue at 0.35, and
// 0.35 < 0.50 trips the rate floor per queue, absolutely, with no cross-queue
// comparison needed.
//
// Both are DISABLED, never lowered. Lowering is the banned fix.
func bet0Policy() bet0LivenessPolicy {
	p := bet0LivenessPolicy{
		minPublished: bet0MinPublishedForHealth,
		minRatio:     bet0MinLiveFraction,
		minBalance:   bet0MinQueueBalance,
		minRateRatio: bet0MinDeliveredFraction,
	}
	if bet0RaceEnabled {
		p.minRateRatio = 0
	} else {
		p.minBalance = 0
	}
	return p
}

// bet0SignalNotice reports whether a run reached the volume at which the P0 has
// actually been DEMONSTRATED to manifest. It returns "" when it has.
//
// This is deliberately NOT a failure. The two numbers do not meet: the defect is
// demonstrated present at every volume from 28,398 published/queue upward
// (38/38 pristine observations, q2 consumed = 0 in all of them), while CI run
// 116 published as little as 21,506 on its slowest queue. Failing below the
// demonstrated floor would make CI permanently red; passing silently would let
// a green be read as evidence it is not. So it passes, and says so out loud.
//
// Separating this from minPublished separates two questions that were conflated
// in one constant: "did the broker break" and "did we learn anything".
// bet0BacklogPremiseCeiling is the per-queue consumed/published ratio above
// which a queue can no longer be treated as backlogged, and consumed-balance
// therefore stops measuring dispatch capacity.
//
// Band: instrumented ratios top out at 0.5867 (the confirms shape, the highest
// measured anywhere under -race), while the uninstrumented path — where this
// premise demonstrably FAILS — runs 0.83-0.94. 0.80 sits between them with
// 1.36x of headroom over the worst instrumented observation. It is a warning
// threshold on a non-fatal log, so a spurious warning costs nothing and a
// missed one costs a silently degenerating predicate; the asymmetry says to
// keep it low rather than tight.
const bet0BacklogPremiseCeiling = 0.80

// bet0PremiseNotice checks the premise that consumed-balance rests on, at
// runtime, per queue, instead of merely asserting it in a comment.
//
// It returns "" when the premise holds or when balance is not active. It is
// deliberately non-fatal: an eroding premise does not mean the broker is broken,
// it means this predicate's balance term is losing its meaning, and the right
// response is a human re-deriving the term — not a red build.
//
// This exists because the premise was already wrong once. It was written down as
// universally true, and measurement then showed it fails on the uninstrumented
// path (broker keeps up ⇒ consumed ≈ published ⇒ consumed-balance degenerates
// into published-balance, 0.5488 against a 0.50 floor). If a future runner is
// fast enough under -race that instrumented ratios climb toward 0.9, the same
// degeneration reappears on the path CI actually runs, and nothing else in this
// file would notice.
func bet0PremiseNotice(samples []bet0Sample, p bet0LivenessPolicy) string {
	if p.minBalance <= 0 {
		return "" // balance is not active on this path; the premise is moot.
	}
	for _, s := range samples {
		if s.ratio() >= bet0BacklogPremiseCeiling {
			return fmt.Sprintf("PREMISE ERODING — consumed-balance is losing its meaning: "+
				"queue %s consumed %d of %d published (%.4f), at or above the %.2f ceiling, so "+
				"it is no longer backlogged and its consumed count is tracking OFFERED LOAD "+
				"rather than dispatch capacity. Consumed-balance degenerates into "+
				"published-balance in this regime. This is not a broker failure and is not "+
				"failing the run — it means the balance term must be RE-DERIVED, not re-tuned. "+
				"[%s]",
				s.name, s.consumed, s.published, s.ratio(), bet0BacklogPremiseCeiling,
				bet0SummarizeSamples(samples))
		}
	}
	return ""
}

func bet0SignalNotice(samples []bet0Sample) string {
	for _, s := range samples {
		if s.published < bet0DemonstratedManifestationVolume {
			return fmt.Sprintf("GREEN AT VOLUME BELOW THE DEMONSTRATED MANIFESTATION FLOOR: "+
				"queue %s published %d, under the %d at which multi-queue starvation has "+
				"actually been observed to manifest on the pre-fix tree. This run passing is "+
				"NOT evidence that multi-queue delivery is healthy. [%s]",
				s.name, s.published, bet0DemonstratedManifestationVolume,
				bet0SummarizeSamples(samples))
		}
	}
	return ""
}

func bet0SummarizeSamples(samples []bet0Sample) string {
	parts := make([]string, 0, len(samples))
	for _, s := range samples {
		parts = append(parts, fmt.Sprintf("%s=%d/%d (%.4f)",
			s.name, s.consumed, s.published, s.ratio()))
	}
	return strings.Join(parts, ", ")
}

// bet0CheckLiveness is the predicate. It is pure — no *testing.T, no clock, no
// broker — so it can be table-tested against recorded number sets from real
// runs, which is how we keep it from silently becoming vacuous.
//
// Checks run in order of diagnostic specificity so the failure message names
// the class of the problem rather than the first threshold that happened to
// trip.
func bet0CheckLiveness(samples []bet0Sample, p bet0LivenessPolicy) error {
	if len(samples) == 0 {
		return fmt.Errorf("INCONCLUSIVE: no samples were recorded — the fixture is dead")
	}
	summary := bet0SummarizeSamples(samples)

	// 1. Vacuity guard. A run that never got going proves nothing, and must not
	//    be allowed to pass by having nothing to measure.
	for _, s := range samples {
		if s.published <= p.minPublished {
			return fmt.Errorf("INCONCLUSIVE: queue %s published only %d messages in the "+
				"window (need > %d). The run was too slow to prove anything and a pass "+
				"would be vacuous. This is NOT a starvation failure — do not read it as one. [%s]",
				s.name, s.published, p.minPublished, summary)
		}
	}

	// 2. Liveness. Catches total death, and catches a UNIFORM collapse that the
	//    balance term below cannot see.
	for _, s := range samples {
		if s.ratio() < p.minRatio {
			return fmt.Errorf("DELIVERY DEAD: queue %s consumed %d of %d published during the "+
				"window (%.4f, need >= %.4f). This queue is not delivering at all. With %d "+
				"queues publishing concurrently, every queue's delivery must keep up just as "+
				"a single queue does. [%s]",
				s.name, s.consumed, s.published, s.ratio(), p.minRatio, len(samples), summary)
		}
	}

	// 3. Balance — THE DIAGNOSTIC. Queues given identical consumer counts and
	//    prefetch must do comparable amounts of DELIVERED WORK. The P0
	//    signature is one queue delivering while its neighbour delivers
	//    nothing. Compared on consumed counts, never on ratios — see the
	//    header, and the premise that makes it valid.
	lo, hi := samples[0], samples[0]
	for _, s := range samples[1:] {
		if s.consumed < lo.consumed {
			lo = s
		}
		if s.consumed > hi.consumed {
			hi = s
		}
	}
	if hi.consumed <= 0 {
		// Unreachable while minRatio > 0; guards the division regardless so a
		// future policy change cannot turn this into 0/0 = NaN, which compares
		// false against every threshold and would pass silently.
		return fmt.Errorf("DELIVERY DEAD: no queue delivered anything during the window. [%s]",
			summary)
	}
	if balance := float64(lo.consumed) / float64(hi.consumed); balance < p.minBalance {
		return fmt.Errorf("MULTI-QUEUE DELIVERY STARVATION (IMBALANCE): queue %s delivered "+
			"%d messages while queue %s delivered %d over the same window — a balance of "+
			"%.4f against a floor of %.2f. These queues have identical consumer counts and "+
			"prefetch, so one doing materially less delivery work than the other is the "+
			"multi-queue starvation signature. A queue's delivery liveness MUST NOT depend "+
			"on whether other, unrelated queues are being published to "+
			"(AMQP 0-9-1 §1.1, §2.1.1). [%s]",
			lo.name, lo.consumed, hi.name, hi.consumed, balance, p.minBalance, summary)
	}

	// 4. The full rate floor. Disabled under -race; see bet0Policy.
	if p.minRateRatio > 0 {
		for _, s := range samples {
			if s.ratio() < p.minRateRatio {
				return fmt.Errorf("MULTI-QUEUE DELIVERY STARVATION: queue %s consumed %d of %d "+
					"published during the window (%.4f of published; need >= %.2f). With %d "+
					"queues publishing concurrently, every queue's delivery must keep up just "+
					"as a single queue does. [%s]",
					s.name, s.consumed, s.published, s.ratio(), p.minRateRatio, len(samples), summary)
			}
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// Table test: the predicate must be seen to discriminate.
// ----------------------------------------------------------------------------

// TestBet0_LivenessPredicateDiscriminates runs bet0CheckLiveness over recorded
// number sets from real runs — healthy and broken, instrumented and not — and
// asserts which class of failure each one produces.
//
// This is the second half of the double-guard. The first half is the standing
// mutation proof against the real broker on the pre-fix tree; this half proves
// the predicate is still capable of returning an error at all, and returns the
// RIGHT error, without a 40-second broker run. A predicate that passes by
// construction gates nothing, and that failure mode is invisible from a green
// suite.
//
// Every "broken tree" row below is MEASURED on 8a65f98 under -race at
// GOMAXPROCS=4; every "healthy" row is measured on the fixed tree. Only the
// partial-starvation row is synthesized, because we have never had a partial
// starvation to measure — it is the regression gate §11 requires this predicate
// to catch, stated as numbers.
func TestBet0_LivenessPredicateDiscriminates(t *testing.T) {
	racePolicy := bet0LivenessPolicy{
		minPublished: bet0MinPublishedForHealth,
		minRatio:     bet0MinLiveFraction,
		minBalance:   bet0MinQueueBalance,
		minRateRatio: 0,
	}
	// The uninstrumented path: balance disabled (its backlog premise fails when
	// the broker keeps up), rate floor enforced.
	fullPolicy := bet0LivenessPolicy{
		minPublished: bet0MinPublishedForHealth,
		minRatio:     bet0MinLiveFraction,
		minBalance:   0,
		minRateRatio: bet0MinDeliveredFraction,
	}

	cases := []struct {
		name    string
		policy  bet0LivenessPolicy
		samples []bet0Sample
		// wantClass is "" when the predicate must pass, otherwise the prefix the
		// failure message must carry.
		wantClass string
	}{
		// --- BROKEN TREE (8a65f98), measured under -race. Must be caught. ---
		{
			name:      "pristine/durable — starved queue at absolute zero",
			policy:    racePolicy,
			samples:   []bet0Sample{{"q1", 137134, 14937}, {"q2", 107521, 0}},
			wantClass: "DELIVERY DEAD",
		},
		{
			// Both terms condemn this row; liveness reports it first because
			// the checks are ordered by diagnostic specificity.
			name:      "pristine/mixed — starved queue at absolute zero",
			policy:    racePolicy,
			samples:   []bet0Sample{{"q1", 187423, 20633}, {"q2", 77076, 0}},
			wantClass: "DELIVERY DEAD",
		},
		{
			// The all-transient shape is HEALTHY on the broken tree — the test
			// file's own header says it "is carried as a guard that must stay
			// healthy", because on an empty WAL the cold-read miss returns fast
			// enough that hole-crawl outruns tag-mint. The predicate must NOT
			// go red here: a red on this row would be red for the wrong reason,
			// and the old 0.50 floor's red on it was a false positive.
			name:      "pristine/transient — healthy on the broken tree, must NOT trip",
			policy:    racePolicy,
			samples:   []bet0Sample{{"q1", 127913, 49214}, {"q2", 119515, 42145}},
			wantClass: "",
		},

		// --- HEALTHY, instrumented. Must pass. These are the rows that made CI red. ---
		{
			name:    "CI 116/durable — healthy under -race",
			policy:  racePolicy,
			samples: []bet0Sample{{"q1", 26587, 4631}, {"q2", 21506, 4104}},
		},
		{
			name:    "CI 116/mixed — healthy under -race, worst ratio on record",
			policy:  racePolicy,
			samples: []bet0Sample{{"q1", 51644, 7216}, {"q2", 39117, 6127}},
		},
		{
			name:    "CI 116/transient — healthy under -race",
			policy:  racePolicy,
			samples: []bet0Sample{{"q1", 60310, 8675}, {"q2", 41183, 6469}},
		},
		{
			name:    "CI 116/confirms — healthy under -race",
			policy:  racePolicy,
			samples: []bet0Sample{{"q1", 43460, 18946}, {"q2", 42487, 16939}},
		},
		{
			// All-durable, from a -count=3 survey on a loaded laptop.
			// Ratio-balance 0.749, consumed-balance 0.914.
			name:    "local/durable — healthy under load",
			policy:  racePolicy,
			samples: []bet0Sample{{"q1", 138917, 45308}, {"q2", 169565, 41405}},
		},
		{
			// THE ROW THAT DECIDED THE METRIC. Measured on the FIXED tree under
			// -race: a 2x publisher-side spread (237201 vs 117345) beside near
			// identical delivered work (42353 vs 40100). Ratio-balance scores
			// this healthy run at 0.5225 — a 4.5% margin over the floor, i.e. a
			// flake with a date on it. Consumed-balance scores it 0.9468.
			name:    "local/mixed — 2x publisher spread, near-identical delivered work",
			policy:  racePolicy,
			samples: []bet0Sample{{"q1", 237201, 42353}, {"q2", 117345, 40100}},
		},

		// --- REGRESSIONS THE PREDICATE MUST CATCH ---
		{
			// Gate §11's stated partial case. Synthesized: it is the regression
			// we require the predicate to catch, not one we have measured.
			// Balance 0.35/0.90 = 0.389, below the 0.50 floor.
			name:      "partial starvation — one queue at 0.35 beside a healthy 0.90",
			policy:    racePolicy,
			samples:   []bet0Sample{{"q1", 100000, 90000}, {"q2", 100000, 35000}},
			wantClass: "MULTI-QUEUE DELIVERY STARVATION (IMBALANCE)",
		},
		{
			// The dossier's perftest shape: 558 consumed of 1,668,046 published
			// across two queues. BALANCE IS ~1.0 HERE — perfectly balanced,
			// perfectly dead. This row is why the liveness term exists.
			name:      "uniform collapse — both queues equally dead, balance ~1.0",
			policy:    racePolicy,
			samples:   []bet0Sample{{"q1", 834023, 279}, {"q2", 834023, 279}},
			wantClass: "DELIVERY DEAD",
		},
		{
			name:      "vacuous run — too little published to conclude anything",
			policy:    racePolicy,
			samples:   []bet0Sample{{"q1", 900, 880}, {"q2", 900, 875}},
			wantClass: "INCONCLUSIVE",
		},

		// --- THE UNINSTRUMENTED PATH still enforces the full 0.50 floor. ---
		{
			name:    "uninstrumented healthy — ~0.97, passes the full floor",
			policy:  fullPolicy,
			samples: []bet0Sample{{"q1", 500000, 485000}, {"q2", 500000, 487000}},
		},
		{
			// THE ROW THAT JUSTIFIES DISABLING BALANCE UNINSTRUMENTED. Balance is
			// off on this path, so the rate floor is the only thing that can fire
			// — and it does, because gate §11's partial case is a queue at 0.35
			// and 0.35 < 0.50 per queue, absolutely, with no cross-queue
			// comparison needed. The rate floor STRICTLY SUBSUMES balance here.
			name:      "uninstrumented partial starvation — rate floor subsumes balance",
			policy:    fullPolicy,
			samples:   []bet0Sample{{"q1", 100000, 90000}, {"q2", 100000, 35000}},
			wantClass: "MULTI-QUEUE DELIVERY STARVATION",
		},
		{
			// The uninstrumented degeneration that forced balance off this path:
			// a 1.6x publisher spread with the broker keeping up (ratios 0.83 /
			// 0.91), so consumed tracks published and consumed-balance would read
			// 0.5488 — a 1.10x margin over a 0.50 floor. Measured on the fixed
			// tree, no -race. It MUST pass; if it ever fails, balance has been
			// switched back on for the uninstrumented path.
			name:    "uninstrumented mixed — healthy, would flake if balance were enabled here",
			policy:  fullPolicy,
			samples: []bet0Sample{{"q1", 478298, 398978}, {"q2", 802005, 727017}},
		},
		{
			// Both queues degraded uniformly to 0.40. Balance is 1.0 and they
			// are far above the liveness floor, so ONLY the rate term catches
			// this — which is exactly why the rate term must survive on the
			// uninstrumented path rather than being deleted everywhere.
			name:      "uninstrumented uniform 0.40 — only the rate floor catches it",
			policy:    fullPolicy,
			samples:   []bet0Sample{{"q1", 500000, 200000}, {"q2", 500000, 200000}},
			wantClass: "MULTI-QUEUE DELIVERY STARVATION",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := bet0CheckLiveness(tc.samples, tc.policy)
			if tc.wantClass == "" {
				if err != nil {
					t.Fatalf("predicate rejected a set it must accept: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("PREDICATE IS VACUOUS on this set: expected %q, got a pass. "+
					"Samples: %s", tc.wantClass, bet0SummarizeSamples(tc.samples))
			}
			if !strings.HasPrefix(err.Error(), tc.wantClass) {
				t.Fatalf("predicate caught the right set for the wrong reason:\n want class: %s\n got:        %v",
					tc.wantClass, err)
			}
		})
	}
}

// TestBet0_LivenessPredicateBalancesDeliveredWork pins the METRIC the balance
// term uses: delivered work (consumed counts), not consumed/published ratios.
//
// It is written as a rule test rather than as table rows, deliberately. Every
// healthy observation on record clears 0.50 under BOTH metrics — the worst
// ratio-balance measured anywhere is 0.5152 — so no recorded dataset can
// distinguish the two implementations. A table row alone would let someone
// switch the metric back to ratios and stay green, right up until the day
// `mixed` lands on the low end of its distribution in CI.
//
// So this asserts the rule directly, using a publisher-side spread larger than
// any we have measured. It deliberately claims NOTHING about what spread a
// healthy broker actually produces; it only fixes what the predicate must do
// when handed one.
func TestBet0_LivenessPredicateBalancesDeliveredWork(t *testing.T) {
	policy := bet0LivenessPolicy{
		minPublished: bet0MinPublishedForHealth,
		minRatio:     bet0MinLiveFraction,
		minBalance:   bet0MinQueueBalance,
	}

	// Large publisher-side spread (4x), near-identical delivered work. Both
	// dispatch planes are healthy, so this MUST pass. Ratio-balance would score
	// it 0.263 and fail it.
	publisherSpread := []bet0Sample{
		{"q1", 400000, 40000}, // ratio 0.100
		{"q2", 100000, 38000}, // ratio 0.380
	}
	if err := bet0CheckLiveness(publisherSpread, policy); err != nil {
		t.Fatalf("balance must be computed on delivered work, not on consumed/published: "+
			"two queues doing near-identical delivery (40000 vs 38000) were failed because "+
			"their PRODUCERS ran at different rates. Got: %v", err)
	}

	// Same publisher spread, but now the delivery work really is lopsided.
	// This MUST fail — otherwise the term detects nothing.
	realImbalance := []bet0Sample{
		{"q1", 400000, 40000},
		{"q2", 100000, 8000}, // balance 0.20
	}
	err := bet0CheckLiveness(realImbalance, policy)
	if err == nil {
		t.Fatalf("PREDICATE IS VACUOUS: delivered work of 8000 beside 40000 is a balance of "+
			"0.20 and must be an IMBALANCE failure, got a pass. Samples: %s",
			bet0SummarizeSamples(realImbalance))
	}
	if !strings.HasPrefix(err.Error(), "MULTI-QUEUE DELIVERY STARVATION (IMBALANCE)") {
		t.Fatalf("real imbalance caught for the wrong reason: %v", err)
	}
}

// TestBet0_LivenessPredicateBuildSplitChangesTheVerdict proves the CONSEQUENCE
// of the -race/!race split, not merely its wiring.
//
// TestBet0_LivenessPredicateRacePolicyMatchesBuild asserts that minBalance is 0
// on the uninstrumented path. That is a wiring assertion, and a wiring assertion
// passes just as happily when the wiring is correct as when it is meaningless —
// the same trap that let a table row fail to notice the class grouping being
// deleted. This test feeds ONE set of numbers to BOTH policies and requires two
// DIFFERENT verdicts, so the split has to actually decide something.
//
// The sample is the degeneration in its purest form: both queues delivering
// healthily at ratio 0.90, well clear of the 0.50 uninstrumented rate floor,
// while a 10x publisher spread drags consumed-balance to 0.10. That is a HEALTHY
// broker which the pre-split code would have failed on the developer-loop path.
//
// It is synthesized, and deliberately more extreme than the 0.5488 tail actually
// measured — a live uninstrumented run does not reliably hit the bad draw, so a
// green from one would prove nothing about the split. This states the rule.
func TestBet0_LivenessPredicateBuildSplitChangesTheVerdict(t *testing.T) {
	// One side comes from bet0Policy() — the real wiring for THIS build — and
	// the other is constructed. That is deliberate: it means a change which
	// "tidies" the asymmetry back into symmetry fails here on whichever build it
	// breaks, not only in the wiring guard.
	var racePolicy, noRacePolicy bet0LivenessPolicy
	if bet0RaceEnabled {
		racePolicy = bet0Policy()
		noRacePolicy = bet0LivenessPolicy{
			minPublished: bet0MinPublishedForHealth,
			minRatio:     bet0MinLiveFraction,
			minBalance:   0,
			minRateRatio: bet0MinDeliveredFraction,
		}
	} else {
		noRacePolicy = bet0Policy()
		racePolicy = bet0LivenessPolicy{
			minPublished: bet0MinPublishedForHealth,
			minRatio:     bet0MinLiveFraction,
			minBalance:   bet0MinQueueBalance,
			minRateRatio: 0,
		}
	}
	// Both queues at ratio 0.90; consumed-balance 90000/900000 = 0.10.
	healthyWithPublisherSpread := []bet0Sample{
		{"q1", 1000000, 900000},
		{"q2", 100000, 90000},
	}

	// The uninstrumented path must ACCEPT it: the broker is keeping up on both
	// queues, which is exactly what the rate floor is there to check.
	if err := bet0CheckLiveness(healthyWithPublisherSpread, noRacePolicy); err != nil {
		t.Fatalf("uninstrumented path must accept a healthy broker whose queues merely had "+
			"different publish rates — this is the degeneration the split exists to prevent. "+
			"Got: %v", err)
	}

	// The instrumented path must REJECT it: under -race a 10x consumed spread is
	// the starvation signature, because there the queues are backlogged and
	// consumed measures dispatch capacity.
	err := bet0CheckLiveness(healthyWithPublisherSpread, racePolicy)
	if err == nil {
		t.Fatalf("instrumented path must reject a 0.10 consumed-balance; got a pass. "+
			"Samples: %s", bet0SummarizeSamples(healthyWithPublisherSpread))
	}
	if !strings.HasPrefix(err.Error(), "MULTI-QUEUE DELIVERY STARVATION (IMBALANCE)") {
		t.Fatalf("instrumented rejection came from the wrong term: %v", err)
	}

	// GUARD 1 — the uninstrumented pass above must not be vacuous. The same
	// sample WITH balance re-enabled has to be rejected, or "it passed" would
	// tell us nothing about balance being off.
	withBalanceRestored := noRacePolicy
	withBalanceRestored.minBalance = bet0MinQueueBalance
	if err := bet0CheckLiveness(healthyWithPublisherSpread, withBalanceRestored); err == nil {
		t.Fatalf("VACUOUS: this sample is supposed to be one that balance rejects, but it "+
			"passes even with balance enabled — so the uninstrumented pass proves nothing. "+
			"Samples: %s", bet0SummarizeSamples(healthyWithPublisherSpread))
	}

	// GUARD 2 — disabling balance uninstrumented must not open a hole. Gate
	// §11's partial case has to still be caught there, by the rate floor.
	partial := []bet0Sample{{"q1", 100000, 90000}, {"q2", 100000, 35000}}
	if err := bet0CheckLiveness(partial, noRacePolicy); err == nil {
		t.Fatalf("uninstrumented path must still catch gate §11's partial starvation " +
			"(0.35 beside 0.90) via the rate floor; got a pass")
	}

	// The premise check and the balance term must agree that this regime is out
	// of bounds — ratio 0.90 is above the backlog ceiling, which is WHY balance
	// is untrustworthy here. If these two ever disagree, one of them is wrong.
	if n := bet0PremiseNotice(healthyWithPublisherSpread, racePolicy); n == "" {
		t.Fatalf("the premise check must flag this sample: queues at ratio 0.90 are not " +
			"backlogged, which is precisely why consumed-balance misreads them")
	}
}

// TestBet0_LivenessPredicateSelfChecksItsPremise proves the premise check is
// wired and discriminating — otherwise it is a comment that costs CPU.
//
// The recorded rows are the two regimes we have actually measured: instrumented
// (premise holds, ratios 0.09-0.59) and uninstrumented (premise fails, ratios
// 0.83-0.94). The uninstrumented row is the one that matters — it is real data
// from the path where consumed-balance was measured degenerating to 0.5488.
// TestBet0_SignalNoticeDiscriminates guards the one component in this file that
// CANNOT fail loudly.
//
// bet0SignalNotice is deliberately non-fatal: it reports that a run passed below
// the volume at which the P0 has ever been demonstrated to manifest, without
// reddening the build. That design has a cost — **if it broke, it would simply
// stop printing, and every CI run would look exactly the same as before.** A
// silent detector that goes silent is indistinguishable from a silent detector
// that is working, which is the vacuous-pass class in its purest form.
//
// So it gets a test even though it asserts nothing at runtime. Caught by
// ci-verify, who noticed it had zero coverage precisely because nothing would
// have noticed.
func TestBet0_SignalNoticeDiscriminates(t *testing.T) {
	// Real CI run 116 volumes — both queues below the demonstrated floor.
	// This is the case that must fire on essentially every CI run today.
	ciVolume := []bet0Sample{{"q1", 26587, 4631}, {"q2", 21506, 4104}}
	n := bet0SignalNotice(ciVolume)
	if n == "" {
		t.Fatalf("SIGNAL NOTICE IS DEAD: CI-volume samples (21506, 26587) are both below the "+
			"%d demonstrated manifestation floor and must produce a notice; got silence. "+
			"Because this notice is non-fatal, a silent failure here is invisible in CI. "+
			"Samples: %s", bet0DemonstratedManifestationVolume, bet0SummarizeSamples(ciVolume))
	}
	if !strings.Contains(n, "NOT evidence") {
		t.Fatalf("the notice must say plainly that a pass is not evidence; got: %s", n)
	}

	// Comfortably above the floor: it must stay quiet, or the notice becomes
	// noise on every run and gets ignored — which is the same failure by a
	// slower route.
	ampleVolume := []bet0Sample{{"q1", 137134, 14937}, {"q2", 107521, 20000}}
	if n := bet0SignalNotice(ampleVolume); n != "" {
		t.Fatalf("notice must stay silent above the floor, or it becomes noise and stops "+
			"being read; got: %s", n)
	}

	// PER QUEUE, not aggregate: one queue below the floor is enough for the run
	// to prove less than it appears to.
	mixedVolume := []bet0Sample{
		{"q1", 137134, 14937},
		{"q2", bet0DemonstratedManifestationVolume - 1, 5000},
	}
	if bet0SignalNotice(mixedVolume) == "" {
		t.Fatalf("notice must evaluate per queue: q2 published below the floor beside a " +
			"healthy-volume q1, and that alone limits what the run proves")
	}

	// Boundary: exactly at the floor is ON the demonstrated evidence, so silent.
	atFloor := []bet0Sample{
		{"q1", bet0DemonstratedManifestationVolume, 5000},
		{"q2", bet0DemonstratedManifestationVolume, 5000},
	}
	if n := bet0SignalNotice(atFloor); n != "" {
		t.Fatalf("exactly at the demonstrated floor is covered by the evidence and must be "+
			"silent; got: %s", n)
	}
}

func TestBet0_LivenessPredicateSelfChecksItsPremise(t *testing.T) {
	balanceActive := bet0LivenessPolicy{minBalance: bet0MinQueueBalance}
	balanceOff := bet0LivenessPolicy{minBalance: 0}

	// Instrumented, backlogged: the premise holds, no notice.
	backlogged := []bet0Sample{{"q1", 137134, 14937}, {"q2", 107521, 20000}}
	if n := bet0PremiseNotice(backlogged, balanceActive); n != "" {
		t.Fatalf("premise check fired on a backlogged run it must accept: %s", n)
	}

	// Uninstrumented reality: ratios 0.83/0.91, consumed tracking published.
	// This is the regime where consumed-balance degenerates, and the check must
	// see it.
	notBacklogged := []bet0Sample{{"q1", 478298, 398978}, {"q2", 802005, 727017}}
	n := bet0PremiseNotice(notBacklogged, balanceActive)
	if n == "" {
		t.Fatalf("PREMISE CHECK IS VACUOUS: queues at ratios 0.83/0.91 are not backlogged "+
			"and consumed-balance is degenerating into published-balance there, but the "+
			"check passed. Samples: %s", bet0SummarizeSamples(notBacklogged))
	}
	if !strings.HasPrefix(n, "PREMISE ERODING") {
		t.Fatalf("premise notice has the wrong class prefix: %s", n)
	}

	// PER-QUEUE, not aggregate: one queue going publish-limited beside a
	// backlogged neighbour must still fire. This is the case that would
	// otherwise red a healthy broker via balance.
	oneQueueLimited := []bet0Sample{{"q1", 400000, 40000}, {"q2", 12001, 12001}}
	if n := bet0PremiseNotice(oneQueueLimited, balanceActive); n == "" {
		t.Fatalf("premise check must evaluate PER QUEUE: q2 consumed everything it "+
			"published while q1 stayed backlogged, and that alone breaks the term. "+
			"Samples: %s", bet0SummarizeSamples(oneQueueLimited))
	}

	// Moot when balance is not active — no noise on the uninstrumented path,
	// where the premise is expected to fail and balance is already disabled.
	if n := bet0PremiseNotice(notBacklogged, balanceOff); n != "" {
		t.Fatalf("premise check must stay silent when balance is disabled, got: %s", n)
	}
}

// TestBet0_LivenessPredicateRacePolicyMatchesBuild guards against the race
// policy silently becoming the uninstrumented one (or vice versa) — a
// mis-wired build tag would leave CI evaluating a predicate nobody intended,
// and it would look identical to a green run.
func TestBet0_LivenessPredicateRacePolicyMatchesBuild(t *testing.T) {
	p := bet0Policy()
	if bet0RaceEnabled {
		if p.minRateRatio != 0 {
			t.Fatalf("race build must DISABLE the rate floor (healthy and broken rate bands "+
				"interleave under instrumentation), got %v", p.minRateRatio)
		}
		if p.minBalance != bet0MinQueueBalance {
			t.Fatalf("race build must ENFORCE consumed-balance at %v — it is the only "+
				"cross-queue detector CI ever evaluates, got %v",
				bet0MinQueueBalance, p.minBalance)
		}
	} else {
		if p.minRateRatio != bet0MinDeliveredFraction {
			t.Fatalf("uninstrumented build must enforce the %v rate floor, got %v",
				bet0MinDeliveredFraction, p.minRateRatio)
		}
		if p.minBalance != 0 {
			t.Fatalf("uninstrumented build must DISABLE consumed-balance — the backlog "+
				"premise it rests on fails when the broker keeps up, and it degenerates "+
				"into published-balance (worst healthy 0.5488 against a 0.50 floor). The "+
				"rate floor subsumes it here. Got %v", p.minBalance)
		}
	}
	// Terms that run on BOTH paths.
	if p.minRatio != bet0MinLiveFraction {
		t.Fatalf("liveness must be enforced on both paths, got %v", p.minRatio)
	}
	if p.minPublished != bet0MinPublishedForHealth {
		t.Fatalf("the publish-health floor must be enforced on both paths, got %d", p.minPublished)
	}
	// Exactly one of the two path-exclusive terms must be live, never both and
	// never neither — that is the whole shape of this policy.
	if (p.minBalance > 0) == (p.minRateRatio > 0) {
		t.Fatalf("exactly one of balance/rate must be active; got balance=%v rate=%v",
			p.minBalance, p.minRateRatio)
	}
}
