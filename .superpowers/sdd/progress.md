# Wave 2 — Progress Ledger

Branch: wave2 (base: main @ ddc933a)
Spec: docs/superpowers/specs/2026-07-07-wave2-design.md

## Dependency tiers
- Tier 0 (parallel): W0 (perf harness), W1 (foundations)
- Tier 1: W2 (WAL bump, needs W1) ∥ W6 (alarms, needs W1)
- Tier 2: W3 (SQ-10 DLX, needs W1+W2)
- Tier 3 (parallel): W4 (SQ-9 TTL) ∥ W5 (SQ-11 maxlen), both need W3
- Continuous: W7 (conformance)

## Status
- (none complete yet)

## Log
- W1: complete (merged ff into wave2, now 31d48e9; review clean, spec ✅ quality ✅, minor-only). Frozen seams live: deadLetter stub sig, DeadLetterReason{rejected,expired,maxlen}, annotateXDeath (field-array x-death), ConnectionBlocked=60/Unblocked=61, QueueState.readyBytes + ReadyBytes/Add/Sub, Server.alarmState (padded) + AlarmMemory/AlarmDisk, Connection.ClientProperties + ClientSupportsBlocked(), connection.blocked advertised true.
  - NOTE for W3: annotateXDeath takes an internal xDeathEvent struct (in-package, W3 may adapt); only deadLetter's signature is frozen.
- Tier 1 started: W2 (wave2-w2) ∥ W6 (wave2-w6), both from 31d48e9.
- W0 still in progress on wave2-w0 (branched from ddc933a, pre-W1; disjoint files, merge later + re-baseline).
- W2: complete (merged ff into wave2, now 3b67a2a; review clean, spec ✅ quality approved, minor-only). WALFormatVersion 2→3; protocol.Message.EnqueueUnixMilli int64 (durable, unset=0, W4 stamps); protocol.DecodeFieldTable added; x-death round-trips (binary WAL+segments, no JSON path for msg headers); backward-compat + mixed-version recovery verified by reviewer.
  - MINOR (for final review): appendMessageExtensions writes empty table if EncodeFieldTable errors → headers with a Decimal 'D' type get dropped on persist (pre-existing, narrow; x-death's own types all survive). Optional fix: add `case Decimal:` to writeFieldValue in protocol/methods.go.
- Tier 2 started: W3 (wave2-w3) DLX body — seam owner — from 3b67a2a.
- Still running: W0 (wave2-w0), W6 (wave2-w6).
- W6: reviewed, NOT merged. Spec ✅ / code approved-with-changes. 3 reader-pause bugs verified correct + tested.
  - C1 (Critical, DESIGN decision — escalated to user): full reader-pause throttles acks → memory alarm can livelock; RabbitMQ blocks only publishers. Awaiting user decision (block-only-publishers recommended).
  - I1 (Important, code fix): handshake-join vs alarm-clear race (server.go:561-563) → re-check alarmState==0 after notifyConnectionBlocked. Fix pending (bundle with C1 fix).
  - I2 (Important, verification): benchstat proof for per-frame alarmState.Load() on ManualAck + MultiAck1000 — W0 gate job at integration.
  - Minors: reason strings (W7), disk-probe slow-poll (benign).
  - C1 RESOLVED (user AFK → best-judgment, flagged for review): switch to RabbitMQ-faithful "block only publishers" — pause reader only for connections that have published; consumer-only connections keep acking so memory alarms can clear. Fix sent to impl-w6. UPDATE SPEC W6 + the "connection.blocked backpressure" decision when confirmed.
- W0: reviewed, NOT merged. Spec ✅ / code approved. benchstat CSV format confirmed emits delta+p (spec warning outdated for the pin); both pre-existing bug fixes (silent zap level, startDrainer WaitGroup race) verified safe + test-only.
  - Important#1 (code fix, sent to impl-w0): BenchmarkProcessCompleteMessage under-sized at 50000x (~30ms/sample, 64% CV) → per-package BENCH_TIME override (~500000x). + Minor: hard-fail when a baselined bench is absent from new run (gate-evasion hole).
  - Important#2 (CONTROLLER job): re-capture baseline on quiesced runner vs integrated wave2 (features unset) at final integration; authoritative gate needs a dedicated CI box (this machine noisy). 
  - Minors (final review): "?" delta warning, CSV-comma parse robustness, offline benchstat pre-warm.
- W6: complete (fix pass re-reviewed clean — C1 block-only-publishers ✅, I1 reconcile-at-publish-site ✅, 3 bug fixes intact). Merged --no-ff into wave2 → bd47171 (structures.go auto-merged, no conflict). Build+vet green. I2 (benchstat proof for per-frame load) tracked under W0 gate. Reason strings + memory-alarm recovery → W7.
  - SPEC UPDATE PENDING: change W6 "full reader-pause" → "block only publishers (HasPublished-gated)".
- W3: reviewed, NOT merged. Spec ✅ / code approved. Seam contracts all verified (non-blocking republish, reason-conditional per-target cycle, zero-cost hot paths, at-least-once, durable x-death recovery).
  - I1 (Important, fix sent): dead-lettered clone keeps original RK+Exchange; RabbitMQ rewrites → clone.RoutingKey=resolved, clone.Exchange=DLX after annotateXDeath (also fixes chained x-death.exchange). Extend the misleading RoutingKeyRewrite test to assert delivered props.
  - M1 (fix sent): bound discardNackedBatch goroutine fan-out (semaphore).
  - M2 (fix sent): log/metric the StoreMessage-failure drop (real loss, make observable).
  - Fixing in the seam so W4/W5 inherit correct behavior. W7-lock list: drop-on-full, store-fail drop, cycle-filter wording, routing-keys no CC/BCC.
- W3: complete (fix re-reviewed clean — I1 RK/exchange rewrite ✅, M1 bounded fan-out ✅, M2 observable drop ✅). Merged --no-ff into wave2 → a66b5de. Build+vet green.
  - Final-review nit: log.Printf in broker/dead_letter.go → route through zap or metrics-only (broker had no logger; non-blocking).
- Tier 3 (FINAL feature tier): W4 (wave2-w4) SQ-9 TTL ∥ W5 (wave2-w5) SQ-11 maxlen, both from a66b5de, both consume the deadLetter seam.
  - OVERLAP WATCH: both edit PublishMessage in storage_broker.go (W4 stamps EnqueueUnixMilli; W5 maxlen check). Guide each to distinct regions; resolve merge conflict on 2nd merge if any.
- W0: complete (fix re-reviewed clean — bench sizing resolved CV 2.39%, storage-constructor latent-bug fixed test-only, parser hard-fail gate-correctness verified vs real benchstat). Merged --no-ff into wave2 → d3978ab (builder.go auto-merged w/ W6). Build+vet green.
  - CONTROLLER TODO (final integration): re-capture testdata/bench-baseline.txt on quiesced runner vs integrated wave2 (features unset) before the gate is authoritative.
  - Final-review pile: broker.createTestBroker has same config-ignoring NewDisruptorStorageWithDataDir pattern (latent test-only trap); log.Printf in broker/dead_letter.go → zap; benchgate "?" delta + comma-in-name (minor).
- Tier 3 in progress: W4 (TTL) running; W5 (maxlen) — first impl (impl-w5) died mid-work on transient API-overload with UNCOMMITTED partial work (max_length.go, confirm_nack_test.go, edits to 4 files) intact in wave2-w5 worktree. Re-dispatched impl-w5b (opus) to assess + finish in-place. No progress lost.
- API OVERLOAD (transient, external): impl-w5, impl-w4, impl-w5b all failed on "API Error: Overloaded" ~18:15-18:26. Partial work for BOTH W4 and W5 is intact + uncommitted in their worktrees. Holding W5 re-dispatch to avoid hammering; impl-w4b in flight as capacity probe. Re-dispatch W5 once capacity returns. NO WORK LOST.
- W4: complete impl (e063f07), reviewed. Spec ✅ (ReapDrop deviation + zero-cost guard both confirmed correct; reaper/Claim linearization verified sound). Quality needs-changes → fix sent to impl-w4b:
  - I-1 (real bug): durable tx+TTL lost on restart (tx path persists EnqueueUnixMilli=0 before the post-commit stamp) → stamp in tx commit path before durable WAL write + add durable-tx-TTL recovery test.
  - I-2 (doc + defer to W7): >64K-unacked spill depth-leak also hits delivery/get head-check; correct note, record as known limitation.
  - M-1 (race): Recover() store waiting before head. M-2: DRY dead messageExpired. M-3: don't stamp anchor for malformed expiration.
  - M-4 (reaper 250ms poll) → final review. W7 divergences: ttl=0 dropped-not-delivered, malformed=no-TTL, spill-counter leak.
  - W5 merge coord (from W4 author): PublishMessage deadline stamp ~L1446; queue-TTL stamp block ~L1506-1513 (maxlen below it); tx closure stamp first ~L1615.
- W4: complete (fix re-reviewed clean — I-1 durable tx-TTL persisted+commit-timed & tested; M-1/M-2/M-3 done; zero-cost intact). Merged --no-ff into wave2 → 70e8312. Build+vet green.
  - Final-review note: durable-tx-TTL test drives PublishMessageTx directly, not end-to-end via ExecuteAtomic (informational). Spill-counter leak → W7.
- W5: complete impl (264fd3f), reviewed CLEAN (spec ✅ / code approved). SettleConfirmNack interlock verified correct+wired (no double-confirm, traced end-to-end). No fix loop. Overshoot=serialize via maxLenMu (only on reject-publish queues, zero-cost when unset). Cross-ring eviction correct.
  - W7-lock: D1 mandatory+reject returns (312), D2 requeue-over-cap not trimmed till next publish (persists), D3 reject-publish not enforced in tx, D4 fanout any-full nacks whole publish (priority). M1 concurrent drop-head can transiently over-evict; M5 DLX-target overflow not enforced.
  - MERGE: resolving W4∩W5 overlap in storage_broker.go — DEDUP deliverMessage Policy() to one shared load; combine PublishMessage TTL-stamp + maxlen-check; tx closure.
