# StrangeQ Wave 2 — Design Spec

_Date: 2026-07-07 · Module: `github.com/maxpert/amqp-go` · Status: **awaiting owner review**_

Wave 2 is the "table-stakes RabbitMQ drop-in" wave: message/queue TTL, dead-letter
exchanges, max-length limits, and connection-level resource alarms — plus the
regression harness that guards them. It rides the Wave 1 scaffolding (SQ-6 single
router, SQ-7 `QueuePolicy`, SQ-8 atomic WAL tx, SQ-5 publisher-confirm batching)
that already landed.

This design was produced by a 12-agent staff-engineer review panel (1 lead
architect → 5 spec authors → 5 adversarial reviewers → 1 principal synthesis) and
every finding below survived an independent refutation pass.

---

## 1. Scope & fixed decisions

**In scope:** SQ-9 (TTL), SQ-10 (DLX), SQ-11 (max-length), SQ-12 (blocked/alarms),
a regression harness, and one durability prerequisite (WAL format bump) the review
surfaced as a hard blocker.

**Explicitly out of scope this pass:** SQ-21 (async durability barrier), quorum-queue
`delivery_limit` dead-lettering, `x-last-death-*` headers, and the redeclare-with-
different-args 406 fix (all deferred to a later pass).

**Fixed decisions (owner-approved 2026-07-07):**

| Decision | Choice | Rationale |
|----------|--------|-----------|
| RabbitMQ fidelity | **Wire-contract-identical; internals may be better** | True drop-in on the wire; free to improve where a conforming client can't observe it. |
| No-regression gate | **Automated benchstat gate + RabbitMQ conformance suite** | Objective per-PR perf gate + provable wire parity. |
| SQ-12 memory metric | **Process RSS / cgroup** (not Go heap) | A `HeapInuse`-only alarm misses off-heap WAL mmap and never fires before real OOM. |
| SQ-9 TTL reclaim | **Delivery-time head-check + bounded coarse sweep** | Contract-identical to RabbitMQ, internally better (no unbounded head-of-line retention). |
| SQ-12 backpressure | **Block only publishing connections** (reader-pause gated on `HasPublished`; consumer/ack/control traffic stays live) | REVISED during W6 review: the original "full per-connection reader-pause" was found *not* RabbitMQ-faithful and could livelock a memory alarm (throttled acks can't drain the queue that would relieve memory). RabbitMQ blocks only publishers and keeps consumers/acks flowing. Still handles the three reader-pause correctness bugs (see W6). |
| Fidelity extras | **Deferred** | `x-last-death-*` + redeclare-406 held for a later pass. |

**Standing performance gate (non-negotiable):** every feature **unset** must cost at
most **one atomic load + one not-taken branch** on each hot path (publish, deliver,
ack, reject). Any per-message allocation, map lookup, or `Policy()` double-load when a
feature is unset is a spec defect. `<2%` regression on the versus + broker benchmarks
with x-args absent.

---

## 2. Work items

Critical path: **W1 → W2 → W3 → {W4, W5}**. **W0 and W6 run fully parallel from day 1.**
W7 (conformance) lands incrementally as each feature merges.

| ID | Title | Pri | Effort | Depends on |
|----|-------|-----|--------|-----------|
| W0 | Perf-gate harness (benchstat <2%, features unset) | P0 | M | — |
| W1 | Foundations: DLX seam + x-death + blocked/unblocked frames + AlarmMonitor skeleton + byte counter | P0 | L | — |
| W2 | WAL/segment format bump (persist Headers, Expiration, enqueue-time) | P0 | M–L | W1 |
| W3 | SQ-10 DLX — `deadLetter` body + reject/nack wiring | P0 | L | W1, W2 |
| W4 | SQ-9 TTL — per-msg/queue TTL, x-expires, recovery replay | P1 | L | W1, W2, W3 |
| W5 | SQ-11 max-length — drop-head / reject-publish + x-overflow | P1 | L | W1, W3 |
| W6 | SQ-12 blocked + memory/disk alarms | P1 | L | W1 |
| W7 | RabbitMQ conformance suite (dual-target) | P1 | M–L | W0, W3–W6 |

---

### W0 — Perf-gate harness (P0, M, no deps)

Stand up `make bench-gate` + a committed `testdata/bench-baseline.txt`. Run the
in-scope benchmarks at **fixed `-benchtime=50000x`** (never default time-based — b.N
would vary and pollute benchstat), pipe HEAD-vs-baseline through a **pinned** benchstat,
fail on any mean delta `>+2%` at `p<0.05`.

Acceptance criteria:
- `bench-gate` + `bench-baseline-refresh` Makefile targets; fixed Nx benchtime.
- Pin `golang.org/x/perf/cmd/benchstat` via a tool directive; verify the chosen output
  format actually emits delta% + p-value (the rewritten `-format=csv` emits raw
  measurements, not deltas — use the classic comparison table or matching flags).
- **Add a low-variance server-package micro-benchmark that drives `processCompleteMessage`
  / the SQ-12 gate at `basic_handlers.go:314` directly** — `broker/bench_test.go` calls
  `broker.PublishMessage()` and never reaches the alarm gate, so otherwise the always-on
  `alarmState.Load()` is defended only by the noisy versus benches.
- Add a fanout/topic **N-queue publish** benchmark (policy unset) and a **manual-ack
  delivery** benchmark (no per-message expiration) — the per-target `Policy()` multiplier
  and the delivery-path load are unmeasured by single-queue benches.
- Add a **many-TTL-queues periodic-sweep** benchmark for the SQ-9 reaper cost.
- Gate `allocs/op` via the two-sample test (p<0.05) or a small integer tolerance, not a
  raw 0-delta (existing benches jitter ±1 alloc); capture allocs in the baseline.
- Intermediate output to a scratch/ignored path (`new.txt` is not gitignored today);
  only `testdata/bench-baseline.txt` is committed.
- Per-package benchstat separation so same-named benches across root/broker/storage/
  protocol don't collide.

Risks: CI noise may make a <2% delta on a single atomic load unresolvable at count=10
over a TCP round-trip → document the need for a quiesced/dedicated runner and higher
`-count`. Baseline drift → regenerate deliberately, review in-PR.

---

### W1 — Foundations (P0, L, no deps) — the single freeze point

Everything downstream compiles against these seams. Lands in **one PR with a working
`deadLetter` stub** so consumers develop in parallel. Create `broker/dead_letter.go`.

Acceptance criteria:
- **DLX seam signature frozen:**
  `func (b *StorageBroker) deadLetter(policy *QueuePolicy, srcQueueName string, msg *protocol.Message, reason DeadLetterReason) error`
  — `policy` passed in, **never re-loaded inside**; caller does the single `qs.Policy()`
  load and the `if p != nil && p.HasDeadLetterExchange` guard before fetching the body.
  Returns `nil` on drop (no DLX / absent DLX / cycle) — dropping is normal, not an error.
- **Republish re-enters ONLY through `routeMessage` via a NON-BLOCKING enqueue** — must
  **not** call `queueState.WaitForCapacity` (`queue_dispatch.go:75`). A DLX target at its
  high-water mark (dead-letter queues typically have no consumers) must never block
  `basic.reject`/`nack` on the connection's frame goroutine. Provide drop-on-full or the
  target's own SQ-11 overflow policy at the seam so all three consumers inherit it.
- **Cycle detection is REASON-CONDITIONAL** (matches `rabbit_dead_letter.erl`):
  **skip entirely when `reason=="rejected"`** (RabbitMQ's `detect_cycles` first clause
  returns all targets — an A↔B reject loop cycles forever, does NOT drop); for
  `expired`/`maxlen` match on **queue name only** (not queue+reason), ideally as
  **post-`routeMessage` per-target filtering** so a fanout DLX with one cyclic + one
  non-cyclic bound queue delivers to the non-cyclic and drops only the cyclic.
- **Strip `Expiration` from the clone only when `reason=="expired"`** (record
  `original-expiration` then); preserve on `rejected`/`maxlen` clones.
- **`annotateXDeath`** emits `x-death` as an AMQP **field array** of field tables with
  exact names/types: `count`(int64), `reason`(longstr), `queue`(longstr), `time`(timestamp
  'T', seconds), `exchange`(longstr), `routing-keys`(field-array-of-longstr),
  `original-expiration`(longstr, expired only). Aggregation by `(queue,reason)` pair,
  `count`++, **head-insertion** (`x-death[0]` = newest). `x-first-death-{reason,queue,
  exchange}` set **once** (gated on absence of `x-first-death-reason`). `x-last-death-*`
  deferred.
- Clone deep-copies `Headers` (reuse the fanout invariant at `storage_broker.go:1241-1247`),
  resets `DeliveryTag` to 0; annotation on the copy, never the live pointer. Clone
  allocation allowed only on this cold path.
- `methods.go` gains `ConnectionBlocked=60` (`ConnectionBlockedMethod{Reason string}`,
  one shortstr), `ConnectionUnblocked=61` (empty), and `EncodeMethodFrame(10,60/61,…)`;
  today `methods.go` stops at `ConnectionCloseOK=51`.
- `s.alarmState atomic.Uint32` added to `*Server`, **padded away from the hot write
  counters** `messagesPublished`/`bytesReceived` (`server.go:50-53`) to avoid false sharing.
- `QueueState` gains a **byte counter** (new atomic) for SQ-11 `ReadyBytes` accounting.
- Capability advertisement flips `connection.blocked: false → true`
  (`connection_handlers.go:43`), AND a **`ClientProperties`/capabilities field is added to
  `protocol.Connection`** (`structures.go:29` has none) populated from
  `ParseConnectionStartOK` (`connection_handlers.go:263`) so emission can be gated on the
  **client's** opt-in.
- `gofmt`/`go vet` clean; stub `deadLetter` returns nil.

Risk: getting the cycle-key / expiration-strip / non-blocking-republish contract wrong in
the frozen text means all three consumers inherit the wrong behavior — highest-leverage
correction, cheap now, expensive after parallel work starts.

---

### W2 — WAL/segment format bump (P0, M–L, deps: W1)

**Promoted from open-question to hard blocker.** `serializeMessageVersioned`
(`wal_manager.go:821`) persists only queue/offset/exchange/routingKey/deliveryMode/body;
`deserializeMessagePayload` (`899-968`) reconstructs with **empty Headers** and
`Redelivered:true`. So durable dead-lettered messages lose `x-death` (and all headers) on
recovery, and durable per-message TTL cannot survive restart.

Acceptance criteria:
- `serializeMessageVersioned` persists `Headers` (via `EncodeFieldTable`), `Expiration`,
  and a new `EnqueueUnixMilli`/precomputed absolute deadline; `deserializeMessagePayload`
  reconstructs them faithfully; **segment_manager mirror updated**.
- `x-death` round-trips through **both** the per-message storage encode/decode **and** the
  durable-metadata JSON path: `int64 count`, `timestamp time`, field-array `routing-keys`
  survive; verified by a durable-roundtrip conformance test.
- Old-version records still read correctly (Headers default empty) — existing durable
  stores recover without rewrite; explicit old→new upgrade test.
- `deserializeMessagePayload` no longer forces empty Headers; recovered dead-lettered
  messages retain a valid `x-death`.
- Recovery replay re-evaluates TTL against **wall clock** using the persisted deadline so
  messages that expired while the broker was down are dead-lettered/dropped on boot, not
  delivered (matches RabbitMQ).

Risk: record-size growth on every persistent message — prove no measurable publish
regression against the W0 gate. Format-version bugs corrupt recovery — explicit upgrade test.

---

### W3 — SQ-10 Dead-letter exchanges (P0, L, deps: W1, W2)

Owns the `deadLetter` body + reject/nack wiring. Highest fidelity-risk item.

Acceptance criteria:
- `reason=rejected` wired at `finishNack` requeue==false branch (`storage_broker.go:527-532`,
  insert `deadLetter` **before** `DeleteMessage`+`AckAdvance`), `RejectGetDelivery` (2081),
  `NackGetDelivery` (2106); `qs.Policy()` loaded **strictly inside the requeue==false branch**
  so requeue==true redelivery, ack, and publish pay zero.
- Routing key = `DeadLetterRoutingKey` when set, else the message's **current RoutingKey**
  (RabbitMQ preserves original RK). Absent/misconfigured DLX (`ErrExchangeNotFound`) or 0
  routed queues = silent drop (`return nil`); source still deleted by caller.
- Cycle detection reason-conditional per W1: **reject-cycle conformance test asserts NO
  drop** against real RabbitMQ (message keeps looping); the cycle-drop test uses
  `reason=expired`.
- **Multi-nack requeue=false** (`NacknowledgeMessage` multiple, `1876-1890`) does NOT
  serialize N synchronous WAL group-commit fsyncs on the connection goroutine — **batch**
  the DLX republishes into a single WAL commit (or async on the cold path); hoist a single
  `Policy()` load before the loop.
- Reject/nack into a DLX target at its high-water mark **returns promptly** (proves the W1
  non-blocking republish) — a test asserts the reject doesn't stall.
- At-least-once: republish `StoreMessage` happens **before** source `DeleteMessage`+
  `AckAdvance`; a crash between them re-dead-letters on recovery (duplicates OK, matches
  RabbitMQ).
- Durable dead-letter recovery test (after W2): a persistent dead-lettered message retains
  `x-death`/`x-first-death-*` across restart.

Risk: reason-conditional per-target fanout cycle filter is MEDIUM-confidence on exact
current-RabbitMQ wording — lock against real RabbitMQ first. Multi-nack fsync serialization
is a real latency cliff if batching is skipped.

---

### W4 — SQ-9 Message & queue TTL (P1, L, deps: W1, W2, W3)

Effective TTL = `min(per-message Expiration, queue x-message-ttl)`. Stamp an absolute
deadline at publish (persisted via W2). Dead-letter expired messages (`reason=expired`) at
the delivery head-check + a bounded periodic reaper. Implement `x-expires` queue
auto-delete + recovery replay.

Acceptance criteria:
- `deliverMessage` expiry-drop uses `b.storage.DeleteMessage` + `queueState.AckAdvance`
  for a present-but-expired message — **NOT** `GapSkipAdvance` (which is for tags never in
  storage); reconciled with the reaper so a reaper-deleted tag later Claimed does not
  double-advance `Depth()`.
- **Reaper/Claim linearization for undelivered (waiting) messages** specified using the
  ring **`Delete`-returns-bool** (`atomic_ring.go:86`) coordinated with the tail cursor as
  the linearization point — **not** `settle()`/`deliveryIndex` (which only exists
  post-deliver). Proven: at most one of {reaper drop, consumer claim} decrements depth per
  tag.
- Reaper holds **no lock across the `deadLetter` republish**: snapshot expired tags,
  release, then republish+`DeleteMessage` per tag; bounded per-sweep batch; driven primarily
  off a per-queue head timer. Defended by the W0 many-TTL-queues sweep benchmark.
- `PublishMessageTx` (`storage_broker.go:1281`): deadline stamped **inside the post-commit
  visibility closure**, not at buffer time, so tx-published TTL counts from commit.
- **`x-message-ttl=0`** behavior pinned by a conformance assertion against real RabbitMQ
  first: StrangeQ has no publish-time direct-handoff, so a ttl=0 message with a parked-ready
  consumer may be dropped rather than delivered — replicate deliver-to-ready-consumer **or
  document the divergence precisely** (no hand-wave).
- Malformed/non-numeric per-message `expiration` behavior locked by a conformance assertion
  before relying on "treat as no-TTL".
- Recovery: `TestRecovery_DurableQueueTTL_ExpiredWhileDown` (green only after W2).
- Zero-cost: shared `p := queueState.Policy()` in `deliverMessage` guards
  `(p != nil && p.HasMessageTTL) || msg.Expiration != ""`;
  `BenchmarkDeliver_NoTTL_ZeroCost` publishes with no per-message expiration so the branch
  is genuinely not-taken (assert 0 `time.Now()` calls).

Risk: reaper-vs-Claim race on waiting messages is the trickiest correctness area. ttl=0 may
be an unavoidable documented divergence.

---

### W5 — SQ-11 max-length / max-length-bytes + x-overflow (P1, L, deps: W1, W3)

Enforce at enqueue in `PublishMessage` (`1218-1262`): drop-head evicts oldest and
dead-letters it (`reason=maxlen`); reject-publish refuses the new publish. Uses the W1
`QueueState` byte counter + a persisted `PendingAck.Bytes` for claim-time decrement.

Acceptance criteria:
- **reject-publish check is AT-OR-OVER-BEFORE-ADD:** reject iff
  `WaitingCount() >= MaxLength` OR `ReadyBytes() >= MaxLengthBytes` — **NOT**
  `ReadyBytes()+len(body) > MaxLengthBytes`. With limit 250 / 100-byte bodies: msgs 1–3
  accepted (readyBytes→300), msg 4 rejected; a single message larger than the byte limit to
  an empty queue is **accepted**. (The draft spec's own unit-test expectation was wrong.)
  Lock on real RabbitMQ first.
- **reject-publish nack composes with the SQ-5 contiguous confirm watermark WITHOUT
  double-confirming the nacked tag:** add a `protocol.Channel` helper (`SettleConfirmNack`)
  that force-flushes durable confirms `< N` (multiple ack up to N-1), emits
  `basic.nack(N, multiple=false, requeue=false)`, then advances `confirmAcked` AND
  `confirmDurable` past N so a later `multiple=true` ack starts at N+1. "Do not advance the
  watermark" alone is **insufficient** — the watermark can't represent "N nacked, N+1
  acked". Conformance test asserts exactly one nack for N and no ack covering N.
- On a non-confirm, non-mandatory channel, reject-publish **silently drops**; with mandatory
  it integrates with `basic.return`.
- Under `OverflowRejectPublish` the incoming publish is refused — the incoming message is
  **not** dead-lettered.
- **drop-head eviction picks the globally-oldest ready message across BOTH the tail cursor
  AND the requeue ring** (`queue_dispatch.go:209-262`) — or the divergence is explicitly
  bounded + tested. Must prove: (a) the limit cannot be **permanently** violated when excess
  sits in the requeue ring and `tail>=head` (`PopReadyTag` returns false today), and (b)
  drop-head never evicts the just-published **newest** message ahead of an older requeued one.
- **Concurrent-publisher overshoot** addressed: either a per-`QueueState` lightweight guard
  serializes the enqueue+limit decision so ready count/bytes never exceed the cap, or the
  transient overshoot (≤ concurrent publishers) is documented + tested with tolerance.
- **maxlen enforced on the requeue path**: a nack/reject requeue=true pushing a full queue
  over the cap drop-heads like RabbitMQ (current `finishNack` hook only restores
  `readyBytes`) — or divergence documented + tested.
- **tx + maxlen** specified: drop-head wired into the `PublishMessageTx` deferred visibility
  closure (`storage_broker.go:1345`); reject-publish-inside-tx behavior documented + tested.
- Eviction's `ClaimInflight`+`AckAdvance` scratch usage (`264-289`) verified not to mis-set
  `minAckCursor` (ring-overwrite safety) while real inflight deliveries are outstanding.
- `PendingAck.Bytes int64` storage codec change (`StorePendingAck`, `407`) adds no
  measurable per-delivery cost; keep the gated-`GetMessage` cold-path fallback if invasive;
  benchmark both ways.
- The shared `deliverMessage` `Policy()` load is a **hard merge dependency** on whoever lands
  first (W4); if SQ-11 lands alone it owns the single load + nil branch + its <2% proof.

Risk: the SQ-5 confirm-watermark interlock is the single most dangerous item — a wrong fix
is a silent confirm-protocol violation. Needs SQ-5 author review. Requeue-ring eviction gap
can permanently violate the limit — not "minor ordering".

---

### W6 — SQ-12 connection.blocked + memory/disk alarms (P1, L, deps: W1)

Fully independent of the DLX seam (disjoint files except the `basic_handlers.go` gate +
`connection_handlers.go` capability, both coordinated in W1). Add an `AlarmMonitor` (global
`atomic.Uint32` bitmask: bit0 memory, bit1 disk) on a dedicated faster ticker; emit
blocked/unblocked on edges to opted-in clients; gate publishes at the top of
`processCompleteMessage`; pause the per-connection reader as TCP backpressure while alarmed —
**but only for connections that have published** (`Connection.HasPublished`), so consumer-only
connections keep acking and a memory alarm can drain and clear (RabbitMQ blocks only publishers;
revised from the original "full reader-pause" during W6 review — see the decisions table).

Acceptance criteria:
- **All emission gated on the CLIENT's advertised `connection.blocked` capability** (stored
  on `protocol.Connection` via W1) — never emit to a client that omitted it. Conformance test
  asserts a non-advertising client receives no frame.
- Publish gate at `basic_handlers.go:314` is **one `atomic.Uint32` load + not-taken branch**
  when `alarmState==0`; flip the informational check at `basic_handlers.go:135` to read the
  alarm, not `conn.Blocked`.
- When `ResourceAlarmsEnabled=false` (or thresholds zero), the `AlarmMonitor` goroutine and
  its `ReadMemStats`/`Statfs` ticker are **entirely dormant** (never spawned) so the unset
  zero-cost contract holds under sustained-throughput benches.
- **Reader-pause does NOT trip the heartbeat false-close:** after un-parking, `SetReadDeadline`
  is reset (or park moves before the deadline block and recomputes on resume) so a park longer
  than `2*hb` doesn't hit the "heartbeat missed" close at `server.go:662-676`.
- **Lost-wakeup-free park/unpark** with explicit ordering: clear stores `alarmState=0` →
  `publishBlocked=false` → `close(wakeCh)`; reader re-checks after acquiring the wake channel
  before blocking.
- A parked reader **unwinds on client death when heartbeats are disabled** (`hb==0`,
  `sendHeartbeats` returns early at `server.go:902-904`) via bounded park re-arm or
  socket-death detection.
- Frame emission uses `WriteFrameToConnection`/`conn.WriteMutex` — no new synchronization
  primitive; the alarm goroutine serializes against frame processor + heartbeat writer via
  the existing lock.
- **Memory trigger measures process RSS / cgroup** (decision), not `HeapInuse` — a Go-heap
  alarm misses off-heap WAL mmap + fragmentation and can miss real OOM.
- Reason strings (`low on memory` / `low on disk` / combined) **locked against real RabbitMQ**
  in the conformance suite (forced via `rabbitmqctl`) before hard-coding; resolve the
  combined-string-vs-no-re-emit inconsistency (RabbitMQ does not re-emit when the active alarm
  set changes while already blocked).
- SQ-5 interaction: the in-flight publish that trips the gate **completes and is confirmed**;
  only subsequent unread publishes are deferred. Document the one-publish-plus-in-flight slack
  from the `processFrames`/`readFrames` goroutine split.
- `conn.Blocked` reconciliation: it is currently **written nowhere** (only read at
  `basic_handlers.go:135`), so a `DepthBackpressure` rename is a behavior-preserving no-op.
- New config fields in `config/config.go` + `interfaces/config.go`
  (vm_memory_high_watermark-equivalent default **0.4 of RAM**, disk_free_limit-equivalent
  default **50MB**); named distinctly from the auth-list `Security.Blocked*` fields.
- `alarmState` padded away from hot write counters; benchstat proof on
  `BenchmarkVersus_ManualAck` and `_MultiAck1000` (the per-frame reader-loop load at
  `server.go:658` runs on every inbound frame incl. acks/heartbeats), not just AutoAck.

Risk: the reader-pause interacts with heartbeat deadline + wake races + heartbeat-disabled
liveness — three distinct correctness bugs that each spuriously kill or wedge a blocked
connection. Highest correctness risk in the wave.

---

### W7 — RabbitMQ conformance suite (P1, M–L, deps: W0, W3–W6)

Reuse the versus URI-resolution pattern (`versus_bench_test.go:34-58`): one set of
`amqp091-go` assertion functions run against whichever broker `AMQP_TARGET` points to
(embedded StrangeQ, or `rabbitmq:4.x` in docker), so both are held to byte-identical
expectations. Lands incrementally, per strict TDD.

Acceptance criteria:
- Every spec's failing conformance assertion is written **first against real RabbitMQ** to
  lock the expected bytes, then StrangeQ is implemented until the embedded broker matches.
- The `connection.blocked` case **forces real RabbitMQ into alarm** in the harness
  (`rabbitmqctl set_vm_memory_high_watermark 0.0001` / `set_disk_free_limit`) and captures
  the actual reason shortstr + combined string — do not self-assert StrangeQ's guess.
- Negative capability case: StrangeQ does NOT emit to a client that omitted the capability
  (amqp091-go always advertises it — construct this case explicitly).
- `confBroker` retains the storage path + config and exposes `restart()` reopening the same
  path/port so the x-death durable-roundtrip case is implementable.
- Embedded broker uses a dynamic port counter + correct `Stop()`/`TempDir` cleanup ordering
  on restart to avoid fixed-port collisions.
- Cycle cases match corrected semantics: reject-cycle asserts **no drop**; expired/maxlen-cycle
  asserts drop on queue-name match; fanout mixed asserts per-target behavior.
- `x-death` field-by-field assertions: count aggregation, most-recent-first ordering,
  `x-first-death-*` set once, `original-expiration` only for expired. Run under
  `-tags=conformance` locally against both brokers + in CI against a RabbitMQ service container.

Risk: forcing real RabbitMQ into a stable alarm in CI is fiddly; reason-string bytes are the
least-verified wire contract. Durable-roundtrip case is blocked on W2 + the restart hook.

---

## 3. Frozen shared interfaces (must not change once parallel work starts)

1. **DLX seam** — `func (b *StorageBroker) deadLetter(policy *QueuePolicy, srcQueueName string, msg *protocol.Message, reason DeadLetterReason) error`. Policy passed in, never re-loaded. Returns nil on drop. Re-enters **only** through `routeMessage` via a **non-blocking** republish (never `WaitForCapacity`).
2. **Cycle detection** — reason-conditional: **skip for `rejected`**; for `expired`/`maxlen` match on **queue name only**, as post-`routeMessage` per-target filtering.
3. **Expiration strip** — only when `reason=="expired"` (record `original-expiration`); preserve on `rejected`/`maxlen`.
4. **x-death format** — field array of field tables: `count`(int64), `reason`(longstr), `queue`(longstr), `time`(timestamp seconds), `exchange`(longstr), `routing-keys`(field-array-of-longstr), `original-expiration`(longstr, expired only). Aggregate by `(queue,reason)`, count++, head-insertion. `x-first-death-{reason,queue,exchange}` once. `x-last-death-*` deferred.
5. **`DeadLetterReason` constants** — `rejected`, `expired`, `maxlen` (`delivery_limit` reserved, not emitted).
6. **WAL record format (W2)** — bumped `WALFormatVersion` persisting `Headers`, `Expiration`, `EnqueueUnixMilli`/deadline; backward-compat read of old version. Shared by SQ-9 (deadline) and SQ-10 (x-death durability) — **one** bump.
7. **Confirm-nack settle (W5)** — `protocol.Channel.SettleConfirmNack(N)`: flush durable confirms `<N`, emit `basic.nack(N, multiple=false)`, advance `confirmAcked` + `confirmDurable` past N. Frozen before SQ-11 reject-publish work; reviewed by the SQ-5 author.
8. **Alarm gate** — `s.alarmState atomic.Uint32` (bit0 memory, bit1 disk); publish gate = one Load at `basic_handlers.go:314`; emission gated on the per-connection stored client capability; frames via `WriteFrameToConnection`/`WriteMutex`. `ConnectionBlocked=60` / `ConnectionUnblocked=61`.

---

## 4. Sequencing & agent split

**Tier 0 (day 1, parallel):** W0 perf-gate + W1 Foundations. W0 must exist before any
hot-path edit merges (the referee). W1 freezes all seams in one PR with a working stub.

**Tier 1 (after W1):** W2 WAL bump (shared blocker for durable TTL + x-death durability —
lands before durable-path work to avoid two incompatible bumps). W3 SQ-10 fills the seam
body. W6 SQ-12 runs **fully parallel** from the moment W1 lands its method IDs / alarmState /
capability field.

**Tier 2 (after W3 body, or against the W1 stub):** W4 SQ-9 + W5 SQ-11 — pure consumers,
different call sites, no conflict beyond the frozen seam. W5 carries the SQ-5 interlock
(needs SQ-5 author review before merge).

**Continuous:** W7 conformance cases as each feature merges (durable-roundtrip gated on W2,
connection.blocked on W6).

**Agent split (5 engineers + lead, git worktrees, strict TDD — no green, no commit):**
- **Lead** → W1 solo (defines the seam) + W2 WAL bump (shared storage blocker).
- **Agent-B** → W3 SQ-10 (seam owner; body merges before A/C integrate).
- **Agent-A** → W4 SQ-9 · **Agent-C** → W5 SQ-11 (consumers; A touches deliver/get/reaper/tx,
  C touches publish enqueue + confirm interlock — disjoint). C loops in the SQ-5 author.
- **Agent-D** → W6 SQ-12 (fully parallel, disjoint files).
- **Agent-E** → W0 perf-gate (day 1), then W7 conformance as features land.

Shared-file contention is limited to `basic_handlers.go` (D's gate) and
`connection_handlers.go` (capability, landed by lead in W1) — coordinated in W1.

---

## 5. Top risks

1. **Frozen-text corrections (blocker):** cycle detection reason-conditional, expiration strip
   conditional, non-blocking republish — all three corrected **in W1** before consumers start.
2. **WAL durability gap (broker-wide):** no Headers/Expiration/enqueue-time persisted today →
   every durable message loses x-death + TTL on recovery. Land as W2 before durable-path work.
3. **SQ-11 reject-publish vs SQ-5 confirm watermark (protocol violation):** needs
   `SettleConfirmNack` + SQ-5 author review — most dangerous correctness item.
4. **SQ-12 reader-pause (three bugs):** heartbeat false-close, lost-wakeup wedge,
   heartbeat-disabled leak — each independently breaks a blocked connection.
5. **SQ-11 requeue-ring eviction gap:** drop-head can't evict requeued messages → limit can be
   permanently violated / newest evicted instead of oldest.
6. **Perf-gate coverage hole:** the SQ-12 alarm gate isn't reached by low-variance benches →
   W0 must add a server-package micro-benchmark; use `-benchtime=50000x`.

---

## 6. Remaining open questions (mostly resolved by the fixed decisions)

- **ttl=0 with a parked-ready consumer** (W4): StrangeQ has no publish-time direct-handoff, so
  this may be an unavoidable **documented divergence** — pin exact behavior against real
  RabbitMQ before deciding.
- **Alarm ticker cadence** (W6): `system_metrics.go` runs a 60s ticker (too coarse). A
  dedicated 1–5s alarm ticker is assumed; confirm the `ReadMemStats`/`Statfs` overhead is
  acceptable (dormant when alarms disabled, so unset cost is zero).
- **RSS measurement mechanism** (W6): `/proc`/cgroup read vs a small dependency (e.g.
  `gopsutil`) — pick during W6 implementation; prefer no new dep if a direct read is clean.

---

_Produced by a 12-agent staff-engineer review panel; every finding survived an independent
refutation pass. Source workflow: `strangeq-wave2-planning` (run `wf_8d7a31e3-0ce`)._
