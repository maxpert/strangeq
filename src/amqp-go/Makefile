.PHONY: build build-server build-perftest test bench-baseline bench-throughput bench-durable bench-large clean bench-gate bench-baseline-refresh conformance conformance-strangeq conformance-rabbitmq

AMQP_URL ?= amqp://guest:guest@localhost:5672/

VERSION ?= 0.1.0-dev
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS = -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

build: build-server build-perftest

build-server:
	go build $(LDFLAGS) -o amqp-server ./cmd/amqp-server

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
# RabbitMQ conformance suite (design doc "W7 — RabbitMQ conformance suite").
# ONE set of amqp091-go assertions, behind the `conformance` build tag, runs
# against whichever broker AMQP_TARGET selects, so both are held to
# byte-identical wire expectations. `conformance` runs the embedded-StrangeQ
# side (the default CI path — no external broker needed). `conformance-rabbitmq`
# runs the SAME suite against a real RabbitMQ 4.x on $(AMQP_URL) to re-lock the
# expected bytes; start one with:
#   docker run -d --rm --name sq-conformance-rmq -p 5672:5672 -p 15672:15672 \
#       rabbitmq:4-management
# and remove it (docker rm -f sq-conformance-rmq) when done. Documented
# StrangeQ-vs-RabbitMQ divergences are target-aware assertions (green on both);
# a `-v` run enumerates them via DIVERGENCE[...] / LOCK[...] log lines.
CONFORMANCE_PKGS ?= . ./server
CONFORMANCE_RUN  ?= TestConformance

conformance: conformance-strangeq

conformance-strangeq:
	go test -tags=conformance -run '$(CONFORMANCE_RUN)' $(CONFORMANCE_PKGS) -count=1 -v

conformance-rabbitmq:
	AMQP_TARGET=rabbitmq go test -tags=conformance -run '$(CONFORMANCE_RUN)' $(CONFORMANCE_PKGS) -count=1 -v -timeout 300s

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

# ---- storage: the package the gate used to be blind to --------------------
#
# All 21 storage benchmarks were ungated while this loop's largest fixes land
# in exactly this package. The four below are the ones whose noise was
# MEASURED on the gating machine at -count=10 and found resolvable at the 2%
# threshold; every other storage benchmark is excluded for a stated reason
# (see .notes/loop-2/step0-report.md "D3"), not because nobody looked:
#
#   GetConsumerPendingAcks_Indexed  sec/op +-2%   B/op +-0%   allocs/op +-0%
#   SegmentWriteReadWithMetadata    sec/op +-1%   B/op +-0%   allocs/op +-0%
#   WALReadWithCache                sec/op +-3%   B/op +-0%   allocs/op +-0%
#   SharedWAL_Acknowledge           sec/op +-3%   B/op +-0%   allocs/op +-0%
#
# Excluded, with the reason recorded rather than left implicit:
#   * the fsync-bound write path (SharedWAL_SingleQueueWrites,
#     _MultiQueueWrites, _MessageSizes/*, _QueueScaling/*, WALWrite_Sequential,
#     PublishDurable1P1C, AppendMessageRecord_NoProps) sits at ~2.7 ms/op on
#     macOS -- one F_FULLFSYNC per operation. A 2% sec/op gate over those
#     measures the disk, not the diff, and their un-measured fsync'd setup
#     makes a -count=10 capture cost minutes per benchmark.
#   * the concurrency-bound set (SharedWAL_ConcurrentWrites, WALWrite_Parallel
#     at SetParallelism(16), Single/MultiQueueConcurrentPublish,
#     ConcurrentGetAndDelete) is goroutine-scheduling bound.
#   * DeleteMessage and GetMessageRingBuffer index a fixed 1000/10000-message
#     fixture with `i % numMessages`, so once b.N exceeds the fixture the
#     operation being measured changes character.
#   * GetConsumerPendingAcks_FullScanBaseline is an intentional O(n) control
#     that exists only to be compared against the Indexed variant.
#   * SharedWAL_Read pre-populates 10000 fsync'd messages per sample; its CV
#     could not be resolved within the measurement budget.
#
# NOTHING here covers WAL RECOVERY: no storage benchmark exercises the
# recovery path at all, so a green bench-gate after this loop's recovery
# fixes says nothing about them. That is a coverage statement, not a caveat.
BENCH_STORAGE_REGEX := ^Benchmark(GetConsumerPendingAcks_Indexed|SegmentWriteReadWithMetadata|WALReadWithCache|SharedWAL_Acknowledge)$$
BENCH_TIME_STORAGE  ?= 200000x

# BENCH_SUITE is the single definition of what the gate measures and how.
# bench-gate and bench-baseline-refresh BOTH expand it, so the run being
# gated and the run the baseline was captured from cannot drift apart in
# package set, -bench regex, -benchtime or -count. They used to be two
# hand-maintained copies of the same four facts; benchstat compares fixed
# iteration counts, so a -benchtime that differed between the two would have
# produced a meaningless comparison that nothing could detect.
# $(1) is the output file to append to.
define BENCH_SUITE
go test . -run='^$$' -bench='$(BENCH_ROOT_REGEX)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) >> $(1)
go test ./broker -run='^$$' -bench='$(BENCH_BROKER_REGEX)' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) >> $(1)
go test ./server -run='^$$' -bench='$(BENCH_SERVER_REGEX)' -benchmem -benchtime=$(BENCH_TIME_SERVER) -count=$(BENCH_COUNT) >> $(1)
go test ./storage -run='^$$' -bench='$(BENCH_STORAGE_REGEX)' -benchmem -benchtime=$(BENCH_TIME_STORAGE) -count=$(BENCH_COUNT) >> $(1)
endef

# Runs the in-scope benchmarks against the CURRENT worktree and gates the
# result against the committed baseline. Fails (non-zero) on any benchmark
# whose mean regresses >2% at p<0.05, or whose allocs/op (or other
# non-latency /op metric) increases at all, significantly.
#
# It also fails, WITHOUT reporting any percentage, when the baseline was
# captured on different hardware from the machine running the gate -- the CPU
# model and count are part of what makes two runs comparable. If that is what
# you are seeing, re-capture with bench-baseline-refresh on this machine and
# review the diff; do not reach for a way to normalise it away.
bench-gate:
	@mkdir -p $(BENCH_OUT_DIR)
	@rm -f $(BENCH_NEW)
	$(call BENCH_SUITE,$(BENCH_NEW))
	go run ./cmd/benchgate -baseline $(BENCH_BASELINE) -new $(BENCH_NEW)

# Regenerates the committed baseline from the CURRENT worktree. Run this
# deliberately — with features unset/absent — and review the diff in the PR;
# never as a way to silence a failing bench-gate.
bench-baseline-refresh:
	@mkdir -p $(BENCH_OUT_DIR) testdata
	@rm -f $(BENCH_BASELINE)
	$(call BENCH_SUITE,$(BENCH_BASELINE))
