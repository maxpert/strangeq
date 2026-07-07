.PHONY: build build-server build-perftest test bench-baseline bench-throughput bench-durable bench-large clean bench-gate bench-baseline-refresh

AMQP_URL ?= amqp://guest:guest@localhost:5672/

build: build-server build-perftest

build-server:
	go build -o amqp-server ./cmd/amqp-server

build-perftest:
	go build -o perftest ./cmd/perftest

test:
	go test ./...

# 1P-1C, 1KB messages, 30s — establishes baseline latency and throughput
bench-baseline: build-perftest
	./perftest -url $(AMQP_URL) -producers 1 -consumers 1 -duration 30s -size 1024

# 50P-50C saturation test — measures peak throughput under load
bench-throughput: build-perftest
	./perftest -url $(AMQP_URL) -producers 50 -consumers 50 -duration 60s -size 1024

# Durable queue + persistent messages + publisher confirms — measures durability overhead
bench-durable: build-perftest
	./perftest -url $(AMQP_URL) -producers 10 -consumers 10 -duration 60s -confirm -durable

# 64KB messages — measures fragmentation and large-message handling overhead
bench-large: build-perftest
	./perftest -url $(AMQP_URL) -producers 1 -consumers 1 -duration 30s -size 65536

clean:
	rm -f amqp-server perftest

# ----------------------------------------------------------------------------
# Perf-gate: no-regression referee for Wave 2 (design doc "W0 — Perf-gate
# harness"). Every feature must cost at most one atomic load + one not-taken
# branch on the hot path when unset; bench-gate enforces <2% mean regression
# (p<0.05) on the benchmarks below, comparing HEAD against the committed
# testdata/bench-baseline.txt.
#
# -benchtime is a FIXED iteration count, never the default time-based mode:
# a time-based -benchtime lets b.N float run-to-run, which pollutes
# benchstat's variance estimate (two runs of the "same" code would differ in
# N, not just timing). -count=10 gives benchstat's two-sample test enough
# samples to separate a real regression from run-to-run noise.
#
# Each in-scope package gets its own `go test <pkg> -bench=<regex>`
# invocation, appended in sequence into ONE output file. This is what the
# regex lists below curate deliberately, rather than "-bench=." over an
# entire package: extending the gate to a new benchmark is a reviewed edit
# here, not automatic drift as unrelated benchmarks accumulate in these
# packages. cmd/benchgate further splits that file back into per-package
# sections before invoking benchstat, so same-named benchmarks in different
# packages are never compared against each other (see cmd/benchgate/split.go).
BENCH_COUNT      ?= 10
BENCH_TIME       ?= 50000x
BENCH_OUT_DIR    := build/bench
BENCH_NEW        := $(BENCH_OUT_DIR)/new.txt
BENCH_BASELINE   := testdata/bench-baseline.txt

BENCH_ROOT_REGEX   := ^BenchmarkVersus_(AutoAck|ManualAck|MultiAck1000)$$
BENCH_BROKER_REGEX := ^Benchmark(QueueDispatch_(Claim|Publish|PublishClaimRoundtrip|ConcurrentPublishClaim|Requeue|AckAdvance)|PublishMessage_(SingleQueue|MultiQueue|MultiQueueTopic)|AcknowledgeMessage|PublishConsumeAck_Roundtrip|DeliverMessage_ManualAck)$$
BENCH_SERVER_REGEX := ^BenchmarkProcessCompleteMessage$$

# BenchmarkProcessCompleteMessage's mean is far smaller than its siblings
# (~500ns vs. broker's multi-microsecond publish benches), so the shared
# BENCH_TIME leaves each of its -count samples too short (~25ms) for the
# noise floor described above to average out — it structurally could not
# resolve the <2% delta it exists to defend (the always-on SQ-12
# alarmState.Load() this benchmark was added to catch). BENCH_TIME_SERVER
# gives the server package its own, larger iteration count so each sample is
# comfortably >=300ms; root/broker keep BENCH_TIME (see w0-report.md "Fix
# pass" for the measured per-sample timing/CV this was tuned against).
BENCH_TIME_SERVER ?= 1500000x

# Runs the in-scope benchmarks against the CURRENT worktree and gates the
# result against the committed baseline. Fails (non-zero) on any benchmark
# whose mean regresses >2% at p<0.05, or whose allocs/op (or other
# non-latency /op metric) increases at all, significantly.
bench-gate:
	@mkdir -p $(BENCH_OUT_DIR)
	@rm -f $(BENCH_NEW)
	go test . -run='^$$' -bench='$(BENCH_ROOT_REGEX)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) >> $(BENCH_NEW)
	go test ./broker -run='^$$' -bench='$(BENCH_BROKER_REGEX)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) >> $(BENCH_NEW)
	go test ./server -run='^$$' -bench='$(BENCH_SERVER_REGEX)' -benchmem -benchtime=$(BENCH_TIME_SERVER) -count=$(BENCH_COUNT) >> $(BENCH_NEW)
	go run ./cmd/benchgate -baseline $(BENCH_BASELINE) -new $(BENCH_NEW)

# Regenerates the committed baseline from the CURRENT worktree. Run this
# deliberately — with features unset/absent — and review the diff in the PR;
# never as a way to silence a failing bench-gate.
bench-baseline-refresh:
	@mkdir -p $(BENCH_OUT_DIR) testdata
	@rm -f $(BENCH_BASELINE)
	go test . -run='^$$' -bench='$(BENCH_ROOT_REGEX)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) >> $(BENCH_BASELINE)
	go test ./broker -run='^$$' -bench='$(BENCH_BROKER_REGEX)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) >> $(BENCH_BASELINE)
	go test ./server -run='^$$' -bench='$(BENCH_SERVER_REGEX)' -benchmem -benchtime=$(BENCH_TIME_SERVER) -count=$(BENCH_COUNT) >> $(BENCH_BASELINE)
