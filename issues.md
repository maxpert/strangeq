# StrangeQ — Issues & Remediation Backlog

_Generated 2026-07-03 — replaces the removed `docs/plan.md`._

This document is the authoritative issue backlog for the StrangeQ AMQP 0.9.1 broker (`github.com/maxpert/amqp-go`). Every issue below was **independently verified against source, then adversarially debated** by a second engineer who tried to refute it and challenge the fix, and finally **prioritized by a council** into P0–P3 tiers with dependency-aware sequencing.

**How to read an entry:** _Impact_ and _Root cause_ describe the defect; _Proof_ cites the exact code; _Fix_ is the investigator's proposed remediation; **_Debate & agreed resolution_ is the authoritative final word** — where the fix verdict is `needs-revision`, the agreed resolution supersedes/amends the fix approach.

> **Note on completeness:** 36 candidates were reviewed → **34 confirmed, 2 rejected/by-design** (I3, I20). The priority council explicitly ranked 32 of the 34; **I17 and I29 were confirmed real but omitted from the council's ranking**, so they are included below placed by their agreed severity and marked accordingly.

## Overall assessment

StrangeQ is not yet a "usable broker" by its own durability/security claims. Two independent classes of ship-blockers dominate: (1) durable-message loss, checkpointed segment-resident messages are permanently orphaned on restart because recovery only scans the WAL (I1), and any startup WAL/segment construction failure silently degrades storage to in-memory while still acking publishers (I4); and (2) trivially-triggered, pre-auth remote DoS via an unbounded client-controlled frame allocation (I8/I10). Security is also fail-open in the exact config an operator would enable: auth is inert through the public builder (I5) and, once wired naively, ANONYMOUS grants remote administrator with no loopback restriction (I6). Beyond those, a cluster of real RabbitMQ-compat correctness gaps (I9 field-table codec, I22 non-atomic/leaky transactions, I19 lifecycle, I15 partial qos, I21 redelivered flag, I13 unknown-exchange black-holing) will each bite a real user. The rest are lower-impact correctness/perf (I23/I24/I27/I28) and a low-risk hygiene sweep (I31-I35). I independently verified the load-bearing source facts for the entire P0/P1 set: no RecoverFromSegments and no ReadDir in segment_manager; error-discarding storage constructor; make([]byte,size+1) on raw untrusted uint32 in both frame readers; builder Build() never references the authenticator; anonymous.go admin identity with no LoopbackOnly; field-table codec handling only 'S'/'I'; no ConnectionTuneOKMethod.Deserialize; TransactionManager.Close called only on graceful channel.close, never on connection teardown; two ~6.9MB binaries tracked in git. The debate's severities and dependency calls are sound; I concur with all confirmations, the I2 downgrade-but-bundle-with-I1 call, the I12/I27 adjustments, and the two by-design rejections (I3, I20 protocol-core).

## Methodology

- **36 candidate issues** consolidated from a prior 7-domain codebase map + 3-reviewer debate.
- **Stage 1 (Investigator):** each issue verified against real source, with proof, root cause, impact, and a detailed fix.
- **Stage 2 (Skeptic):** adversarially challenged validity, severity, and the fix; converged on an agreed resolution.
- **Council:** ranked confirmed issues into tiers, flagged quick wins, and produced a dependency-ordered sequence.
- **Result: 34 confirmed, 2 rejected/by-design.** Confidence is `high` on every confirmed item.

## Priority tiers

- **P0** — Ship-blocker: silent/deterministic durable-data loss, fail-open auth an operator would actually enable, or a trivially-triggered pre-auth remote crash/OOM DoS. Must be fixed before any usable/durable/secure broker claim.
- **P1** — Serious correctness/compat/security gap a real RabbitMQ-client user or operator hits in normal use: broken typed-field interop, non-atomic transactions, missing lifecycle/qos/redelivered semantics, unenforced resource limits, dead observability, spoofable identity.
- **P2** — Real but lower-impact correctness/perf, partial features, and protocol nuances with a narrow or rare trigger: exchange/binding cleanup, backpressure inflation, connection-close linger, nowait/count reporting, capability false-advertisement.
- **P3** — Cleanup, dead code, dead config/interfaces, committed binaries, latent-only defects, and undocumented invariants. No runtime impact on live paths.

## Priority summary

| Rank | ID | Tier | Severity | Effort | Quick win | Depends / blocks | Title |
|---:|---|:--:|---|---|:--:|---|---|
| 1 | I1 | P0 | 🔴 Critical | large |  | blocks I2,I21 | Segment-resident durable messages are lost on restart (recovery scans WAL only) |
| 2 | I4 | P0 | 🟠 High | medium | ✅ |  | Silent storage degradation to nil on startup disk/permission failure |
| 3 | I8 | P0 | 🟠 High | small | ✅ | blocks I10 | Pre-auth remote OOM DoS: unbounded inbound frame size |
| 4 | I5 | P0 | 🟠 High | medium |  | blocks I6 | Authentication/authorization is inert through the public ServerBuilder |
| 5 | I6 | P0 | 🟠 High | small | ✅ |  | ANONYMOUS mechanism grants remote administrator with no loopback restriction |
| 6 | I9 | P1 | 🟠 High | medium |  |  | Field-table codec is not AMQP-compliant (only S and I types) -> handshake desync with many clients |
| 7 | I10 | P1 | 🟠 High | medium |  |  | connection.tune-ok ignored; ChannelMax/FrameMax/Heartbeat + MaxConnections/MaxChannels unenforced (resource exhaustion) |
| 8 | I22 | P1 | 🟠 High | medium |  |  | Transaction commit is neither atomic nor rollback-safe; duplicates on retry; global txMutex |
| 9 | I19 | P1 | 🟡 Medium | large |  |  | auto-delete and exclusive queue/exchange lifecycle not enforced (core 0.9.1) |
| 10 | I28 | P1 | 🟡 Medium | medium | ✅ |  | Metrics pipeline largely non-functional (collector never wired into storage) |
| 11 | I15 | P1 | 🟡 Medium | large |  |  | basic.qos only partially implemented (prefetch_size, global, post-register resize all unhonored) |
| 12 | I21 | P1 | 🟡 Medium | medium |  |  | redelivered flag always false on requeued/nacked/rejected/recovered messages |
| 13 | I11 | P1 | 🟡 Medium | medium |  |  | No receive-side heartbeat/idle timeout; heartbeat hardcoded 5s ignoring negotiation |
| 14 | I7 | P1 | 🟡 Medium | small | ✅ |  | No user-id message-property validation (identity spoofing in metadata) |
| 15 | I13 | P2 | 🟡 Medium | small | ✅ |  | No exchange-type validation -> unknown type silently black-holes messages |
| 16 | I23 | P2 | 🟡 Medium | small | ✅ |  | DeleteExchange leaves dangling queue/exchange bindings (stale match on recreate) |
| 17 | I27 | P2 | 🟡 Medium | small | ✅ |  | Client-initiated connection.close not fully honored (connection lingers) |
| 18 | I24 | P2 | 🟡 Medium | medium |  |  | Fanout/mixed-workload backpressure inflation from sparse global tags + two-writer minAckCursor |
| 19 | I12 | P2 | 🟡 Medium | large | ✅ |  | Advertised capabilities connection.blocked and consumer_cancel_notify are never implemented on the wire |
| 20 | I26 | P2 | ⚪ Low | small | ✅ |  | queue.declare-ok / queue.delete-ok report hardcoded counts |
| 21 | I16 | P2 | ⚪ Low | trivial | ✅ |  | nowait not honored in exchange.declare / queue.declare (always send declare-ok) |
| 22 | I14 | P2 | ⚪ Low | small | ✅ |  | basic.immediate flag deserialized then silently ignored (not rejected) |
| 23 | I18 | P3 | ⚪ Low | small | ✅ |  | Overlong short-string names silently truncated instead of frame-error |
| 24 | I30 | P3 | ⚪ Low | small | ✅ |  | LifecycleManager GetStats/GetConnections return placeholder/zeroed data |
| 25 | I35 | P3 | ⚪ Low | trivial | ✅ |  | generateID math/rand fallback can collide on crypto/rand failure |
| 26 | I2 | P3 | ⚪ Low | medium |  | after I1 | globalDeliveryTag not persisted -> tag reuse/collision after restart |
| 27 | I31 | P3 | ⚪ Low | small | ✅ |  | Dead code bundle: message_cache, message_index (+makeCacheKey collision), storage/codec, inmem_transaction_store, segment checkpointLoop no-op |
| 28 | I32 | P3 | ⚪ Low | small | ✅ |  | Dead interface contracts: interfaces.Broker / interfaces.Server + brokerWrapper wrap-then-unwrap |
| 29 | I33 | P3 | ⚪ Low | medium |  |  | ~18 dead config fields + errors package ~90% dead + duplicate error constants |
| 30 | I36 | P3 | ⚪ Low | medium |  |  | requeueTags slice never compacts under sustained partial requeue churn |
| 31 | I25 | P3 | ⚪ Low | small | ✅ |  | Load-bearing pooled-object aliasing invariant is undocumented and untested |
| 32 | I34 | P3 | ⚪ Low | trivial | ✅ |  | Two ~6.9MB compiled crash_test_tool binaries committed to git |
| — | I17 | P3 | ⚪ Low | large |  |  | Multi-step SASL (connection.secure/secure-ok) unimplemented |
| — | I29 | P3 | ⚪ Low | small |  |  | Double system-metrics collection when LifecycleManager is used |

## Recommended sequence

- Batch 0 (P0 durability, first and together): Land I4 (constructor returns error + builder fails closed + StoreMessage rejects durable-when-wal==nil) as the trust gate, then I1 (segment discovery + RecoverFromSegments + WAL-wins dedupe merge, run even when wal==nil), and BUNDLE I2 into the I1 PR as prerequisite hardening implemented as a persisted delivery-tag counter (option 2) with a true fresh-process restart test. This is the durable-broker foundation.
- Batch 1 (P0 pre-auth DoS + fail-open auth, parallelizable with Batch 0): Ship I8 (MaxInboundFrameSize ceiling + capped default frame reader) first; then I5 (wire the authenticator, build registry from Security.AuthMechanisms, fail-closed, no auto-created creds) and I6 (LoopbackOnly:true + PLAIN-only/opt-in registry) together so auth is not re-opened. Closes the two remaining ship-blocker classes.
- Batch 2 (P1 protocol/compat foundation): I9 (full field-table codec) as the interop cornerstone, then I10 (tune-ok Deserialize + FrameMax/ChannelMax/MaxConnections enforcement + BodySize bound) building on I8's ceiling, and I11 (negotiated heartbeat + SetReadDeadline) reusing I10's tune-ok deserialize and the read-deadline plumbing.
- Batch 3 (P1 correctness/semantics): I22 (atomic/rollback-safe tx + close-on-connection-teardown), I19 (auto-delete/exclusive lifecycle + owner tracking, fix the self-deadlocking sketch), I15 (qos: ship Fix C reject-prefetch_size and Fix A resizable gate under -race; stage Fix B global gate later), I21 (redelivered flag via the race-free requeue-ring carry, benefits from I1 already landed), then I7 (user-id validation, now that auth is enforced).
- Batch 4 (P1 observability + P2 quick wins): I28 Part A (bridge storage metrics, quick, benchmark the fsync path) then Part B instrumentation. Alongside: I13 (reject unknown exchange type with connection 503 + 406 re-declare mismatch), I23 (DeleteExchange binding teardown), I27 (honor client connection.close + reply-text fix), I26+I16 together (queue.declare/delete counts + NoWait gates, same handlers), I12 stopgap (flip false-advertised capabilities to false).
- Batch 5 (P2 perf + follow-ups): I24 (per-queue depth backpressure fix with pinned waiting/inflight convention + Recover init, -race gated), I14 (reject basic.immediate 540 with corrected import), and I12 Part A (server-initiated basic.cancel with full teardown idempotency) as a properly-scoped follow-up.
- Batch 6 (P3 hygiene sweep, low risk, batch together): I31 (dead-file + checkpointLoop deletion), I32 (dead interface/brokerWrapper collapse), I33 (dead config + errors dedup + doc corrections + CI grep-guard, folding in I20's dead MessageTTL field), I34 (git rm binaries + path-anchored .gitignore), I25 (document the pooled-aliasing invariant incl. Body + regression tests), I35 (counter-based generateID fallback), I18 (clampShortString + ReplyText bounds), I30 (real GetStats/GetConnections data, Username first), I36 (index-ring requeueTags with mandatory idle-shrink). Run gofmt/go vet/go test -race after each to prove nothing referenced removed symbols.

## Rejected / accepted-by-design

These candidates were investigated and **deliberately not actioned** (the debate rejected them or deemed the behavior acceptable). Listed for the record so they aren't re-raised.

### I3 — WAL ackBitmap not persisted -> already-acked durable messages redelivered after crash

**Disposition:** Rejected / acceptable-by-design. WAL ackBitmap not being persisted yields at-least-once redelivery (Redelivered=true) after a crash, the only guarantee AMQP 0.9.1 makes. Redelivery is bounded to offsets sharing a WAL file with still-unacked offsets, documented, and pinned by existing tests. The proposed durable ack-log is a large future enhancement that still cannot deliver exactly-once, so it is not a defect fix. No change unless a concrete reduced-duplicate requirement emerges.

### I20 — RabbitMQ extension features entirely absent (TTL/DLX/max-priority/max-length/alternate-exchange)

**Disposition:** Rejected at the protocol level / acceptable-by-design. TTL/DLX/max-priority/max-length/alternate-exchange are RabbitMQ vendor extensions outside the AMQP 0.9.1 core spec the project targets, and README honestly documents them as unsupported (no false advertising). The ONLY actionable residue is the undisclosed dead MessageTTL config field, which should be deleted as trivial hygiene folded into the I33 config cleanup; keep ExpiredMessageCheckIntervalMS with its honest disclaimer and its validation. The extensions themselves are features to track separately, not bug fixes.

---

## Issue details (in priority order)

# P0

## [I1] Segment-resident durable messages are permanently lost on restart (recovery scans WAL only, never segments)

**Rank 1 · P0** · Severity: 🔴 Critical · Category: durability · Effort: large

**Blocks:** I2, I21

### Impact
Any durable (DeliveryMode=2) message that survives a WAL checkpoint but is not yet acked is silently and permanently lost across a broker restart. Checkpointing happens both periodically (default every 5 minutes) and on graceful shutdown ("Final checkpoint before exit"), so this is a deterministic outcome, not a rare race: after a WAL file rolls to oldFiles and performCheckpoint runs, the unacked durable message is written to a segment file, the WAL file is deleted (os.Remove), and its offset index is purged. On restart, recovery (recoverPersistentMessages -> GetRecoverableMessages -> RecoverFromWAL) scans ONLY dataDir/shared/*.wal files; the segments/ directory is never scanned, no queue dispatch cursor is seeded for those tags, and SegmentManager builds fresh empty QueueSegments (opening a brand-new segment file) so even a point-lookup cannot find them. The messages become unreachable to consumers forever, violating the core AMQP durability guarantee for persistent messages on durable queues. Data volume affected grows with checkpoint frequency and retention: a broker running longer than one checkpoint interval loses most of its durable backlog on every restart.

### Root cause
The persistence design has two storage tiers — WAL (hot) and segments (cold, checkpointed) — but the recovery path only knows about the WAL tier. SegmentManager has no startup discovery/scan of on-disk segment files: NewSegmentManagerWithConfig merely creates the segments directory, and getOrCreateQueueSegments initializes an empty sealedSegments map plus a fresh openNextSegment. There is no RecoverFromSegments equivalent to RecoverFromWAL, and GetRecoverableMessages never consults segments. performCheckpoint deletes the source WAL file after moving messages to a segment (correct for space reclamation) but nothing on the read/recovery side compensates, so the moved messages fall out of every recovery-reachable index once the process restarts.

### Proof

- **`src/amqp-go/storage/wal_manager.go` : 1288-1319** — performCheckpoint moves unacked durable messages to segments then os.Remove()s the WAL file and purges its offset index; after this the message exists only in a .seg file.

```go
if err := segMgr.CheckpointBatch(queueName, queueMsgs); err != nil {...}
...
_ = os.Remove(info.path)
```

- **`src/amqp-go/storage/disruptor_storage.go` : 871-889** — GetRecoverableMessages only calls wal.RecoverFromWAL(); ds.segments is never consulted during recovery.

```go
recoveredMessages, err := ds.wal.RecoverFromWAL()
```

- **`src/amqp-go/storage/wal_manager.go` : 311-332** — RecoverFromWAL enumerates only dataDir/shared/*.wal; the segments directory is never scanned.

```go
sharedDir := filepath.Join(wm.dataDir, "shared")
files, err := os.ReadDir(sharedDir)
```

- **`src/amqp-go/storage/segment_manager.go` : 134-144** — SegmentManager construction does no on-disk segment discovery, so pre-restart segment contents are never loaded.

```go
return &SegmentManager{ dataDir: segDir, cfg: cfg }, nil
```

- **`src/amqp-go/storage/segment_manager.go` : 422-425** — getOrCreateQueueSegments starts with empty sealedSegments and opens a brand-new segment file, orphaning any pre-restart .seg files and making them unreachable even via point-lookup Read.

```go
// Create first segment
if err := segments.openNextSegment(); err != nil { return nil }
```

- **`src/amqp-go/storage/log_compaction_integration_test.go` : 110-119** — The only test asserting segment durability uses in-process point-lookup GetMessage in the same running process (segments still in memory); it never restarts and never checks recovery/re-enqueue, so this data-loss path is untested.

```go
msg, err := ds.GetMessage(queue, uint64(i))
require.NoError(t, err, "...readable... (via WAL or segments)")
```


### Fix

Add a segment recovery path so checkpointed durable messages are re-discovered and re-enqueued on restart, mirroring the existing WAL recovery. Three coordinated changes: (1) Segment discovery + scan in storage/segment_manager.go: add a loadExistingSegments step invoked when a QueueSegments is first created for a queue that has existing .seg files on disk, so it opens each old .seg read-only, rebuilds its index from the persisted .idx file (writeIndexToDisk already emits these) or by scanning frames if the .idx is missing/corrupt, sets minOffset/maxOffset/messageCount, and registers them in sealedSegments instead of only opening a fresh empty segment. Then add SegmentManager.RecoverFromSegments() (map[string][]*RecoveryMessage, error) that walks sm.dataDir per-queue with os.ReadDir, calls getOrCreateQueueSegments (which now loads existing segments), and iterates the in-memory sealed+current segment indexes to build RecoveryMessages by reading each body via readSegmentMessageAt. Every message physically present in a .seg is unacked-at-checkpoint (performCheckpoint filters acked via ackBitmap before writing, and compaction physically removes acked messages), so all present messages should be recovered; a message acked AFTER checkpoint but before restart will be redelivered, which is the AMQP-correct at-least-once behavior (WAL recovery already behaves this way for post-checkpoint acks since the ackBitmap is in-memory only). (2) Merge segment recovery into GetRecoverableMessages (storage/disruptor_storage.go:871-889): after collecting WAL-recovered messages, if ds.segments != nil call ds.segments.RecoverFromSegments() and append its results into messagesByQueue, de-duplicating by (queueName, DeliveryTag) with the WAL copy winning to defend against a crash between segment write and WAL delete; also run segment recovery even when ds.wal == nil (currently returns early). (3) Preserve DeliveryTag/DeliveryMode: verify serializeSegmentMessage/readSegmentMessageAt round-trip DeliveryTag and DeliveryMode; if DeliveryTag is not persisted, use the stored segment offset as the tag (offset==DeliveryTag is the existing convention in performCheckpoint) or extend the record format. This makes both recovery re-enqueue and point-lookup Read correct and prevents orphaned segment files after restart.

**Files to change:**
- `src/amqp-go/storage/segment_manager.go` — Add loadExistingSegments logic invoked from getOrCreateQueueSegments that enumerates existing .seg files for the queue dir, opens them read-only, rebuilds each segment index from its .idx (or by scanning frames), and registers them in sealedSegments with correct minOffset/maxOffset/messageCount before opening a new active segment. Add SegmentManager.RecoverFromSegments() that walks sm.dataDir per-queue and returns map[string][]*RecoveryMessage built from the loaded in-memory segment indexes. Verify DeliveryTag/DeliveryMode round-trip in serializeSegmentMessage/readSegmentMessageAt; fall back to offset==DeliveryTag if not persisted.
- `src/amqp-go/storage/disruptor_storage.go` — In GetRecoverableMessages (871-889), after RecoverFromWAL, call ds.segments.RecoverFromSegments() and merge into messagesByQueue with (queueName,DeliveryTag) dedupe (WAL wins). Ensure segment recovery runs even when ds.wal == nil instead of returning an empty map early.
- `src/amqp-go/storage/log_compaction_integration_test.go` — Add TestSegmentResidentMessagesRecoveredAfterRestart (see test field) that checkpoints WAL->segments, closes, reopens a fresh DisruptorStorage over the same dataDir, and asserts GetRecoverableMessages returns all unacked segment-resident messages with correct bodies and DeliveryMode==2.

**Code sketch:**

```go
// storage/segment_manager.go
func (sm *SegmentManager) RecoverFromSegments() (map[string][]*RecoveryMessage, error) {
    out := make(map[string][]*RecoveryMessage)
    dirs, err := os.ReadDir(sm.dataDir)
    if err != nil { if os.IsNotExist(err) { return out, nil }; return nil, err }
    for _, d := range dirs {
        if !d.IsDir() { continue }
        queue := d.Name()
        qs := sm.getOrCreateQueueSegments(queue) // now loads existing .seg into sealedSegments
        if qs == nil { continue }
        qs.sealedMutex.RLock()
        for _, seg := range qs.sealedSegments {
            seg.mutex.RLock()
            for offset, pos := range seg.index {
                msg, err := readSegmentMessageAt(seg.file, pos)
                if err != nil || msg == nil { continue }
                if msg.DeliveryTag == 0 { msg.DeliveryTag = offset }
                out[queue] = append(out[queue], &RecoveryMessage{QueueName: queue, Offset: offset, Message: msg})
            }
            seg.mutex.RUnlock()
        }
        qs.sealedMutex.RUnlock()
    }
    return out, nil
}

// disruptor_storage.go GetRecoverableMessages, after WAL merge
seen := map[string]map[uint64]bool{} // populated from WAL results
if ds.segments != nil {
    segMsgs, err := ds.segments.RecoverFromSegments()
    if err != nil { return nil, fmt.Errorf("segment recovery failed: %w", err) }
    for q, msgs := range segMsgs {
        for _, m := range msgs {
            if seen[q][m.Message.DeliveryTag] { continue }
            messagesByQueue[q] = append(messagesByQueue[q], m.Message)
        }
    }
}
```


**Test (TDD):** Add TestSegmentResidentMessagesRecoveredAfterRestart in storage/log_compaction_integration_test.go: (1) create DisruptorStorage over a temp dataDir with a durable queue and StoreMessage N durable (DeliveryMode=2) messages; (2) force WAL roll (sharedWAL.rollFile) and performCheckpoint so messages migrate WAL->segments and WAL files are deleted; assert oldFiles is empty; (3) ds.Close() to flush/seal segments; (4) construct a NEW DisruptorStorage over the SAME dataDir (simulating restart); (5) call GetRecoverableMessages() and assert it returns all N unacked messages for the queue with correct bodies and DeliveryMode==2. Before the fix this returns an empty/short map (loss); after the fix it returns all N. Add a follow-up assertion that RecoveryManager.recoverPersistentMessages seeds the queue cursor (RecoverQueue called) so a consumer can claim the recovered tags.

**Regression risk:** Medium-to-high. Changing getOrCreateQueueSegments to load existing .seg/.idx on first access alters startup I/O and must be lazy per-queue and must not double-open the active segment. The segment record format must round-trip DeliveryTag and DeliveryMode; if serializeSegmentMessage does not persist DeliveryTag, recovery would assign wrong tags, so this must be verified/extended (a compatibility concern for already-written segments). Dedupe between WAL and segment recovery is required to avoid double-delivery when a crash leaves a message in both tiers. Post-checkpoint acks are tracked only in the in-memory WAL ackBitmap (lost on restart today), so recovering segment messages will correctly redeliver some messages acked after their checkpoint — a behavior change from silent loss to at-least-once redelivery, which is AMQP-correct but should be called out. Segment compaction (compactSegment) relocates/rewrites segment files and regenerates indexes, so the loader must tolerate compacted files. Mitigate with the restart test above plus an acked-after-checkpoint-then-restart characterization test and a compacted-segment test.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged whether this is dead/unreachable code (would downgrade to low). Refuted: NewDisruptorStorageWithEngineConfig unconditionally builds the SegmentManager (disruptor_storage.go:178) and wires it to the WAL (184), and GetMessage/DeleteMessage both consult ds.segments (312-313, 340-341). The two-tier design is live in production, so the loss path is reached by default.
- Challenged the fix's reliance on the .idx file for index rebuild. Found a real completeness hole the fix understates: writeIndexToDisk is called ONLY from sealSegment (segment_manager.go:579). On graceful close() (784-794) the CURRENT (unsealed) segment is synced+closed but its index is neither flushed to a .idx nor moved to sealedSegments. So the most-recently-checkpointed messages live in a .seg with NO .idx. The fix's 'rebuild from .idx OR scan frames if .idx missing' must be hardened: frame-scanning is the mandatory primary path (idx is only an optimization) or the last active segment is silently dropped again. RecoverFromSegments must also iterate the current segment, not just sealedSegments as the code sketch does.
- Challenged the fix's DeliveryTag-persistence regression concern (it rated this medium-high risk). Partially refuted: serializeSegmentMessage does NOT store DeliveryTag, but readSegmentMessageAt reconstructs DeliveryTag=offset (line 944) and performCheckpoint writes msg.Offset which equals the tag, and DeliveryMode round-trips (836/929). So the offset==DeliveryTag fallback is already the actual behavior — tag/mode correctness is not a real regression risk; that part of the risk assessment is overstated.
- Challenged whether any existing test already covers this (would weaken the bug). Refuted and strengthened: TestLogCompaction_FullLifecycle Step 8 (lines 172-203) DOES reopen a fresh ds2 but only asserts acked messages are NOT recovered — it never asserts unacked segment-resident messages ARE recovered, so it is blind to the loss. TestLogCompaction_RetentionWithCheckpoint reads via the same in-memory sm (line 272), never a fresh SegmentManager, so its 'survive a restart' claim is false. The proof's test-gap point is accurate and if anything understated.

**Fix concerns:**
- Code sketch iterates only qs.sealedSegments; it must also read the current (unsealed) segment, because the last checkpoint's messages land in the active segment and, on graceful shutdown, are never sealed.
- The loader must NOT depend on the .idx being present: writeIndexToDisk only runs at seal time, so the active .seg on a graceful shutdown has no .idx. Frame-scanning of the .seg must be the mandatory rebuild path; .idx is an optional fast path. Reuse the existing serializeSegmentMessage framing ([CRC][len][queue_len][queue][offset][exchange][rk][delivery_mode][body]) to walk frames and rebuild offset->position.
- Must reconcile with compaction: compactSegment rewrites .seg files and regenerates indexes (segment_manager.go ~715); the loader must tolerate compacted files and must not double-count. Add a compacted-segment-then-restart characterization test.
- Dedupe between WAL and segment recovery is required (WAL wins) to avoid double-delivery for a message left in both tiers by a crash between segment write and WAL delete — the fix calls this out; keep it.
- Behavior change to call out (AMQP-correct): post-checkpoint acks are tracked only in the in-memory segment ackBitmap and lost on restart, so recovered segment messages acked after their checkpoint will be redelivered (at-least-once). This is correct but should be documented and covered by an acked-after-checkpoint-then-restart test.
- loadExistingSegments must be lazy per-queue and must not double-open the freshly-created active segment; ordering (load sealed .seg files, THEN open a new active segment) must be exact to avoid segmentNum/time.Now() collisions and orphaned handles.

**Agreed resolution (authoritative):**

CONFIRMED real, critical durability bug. I independently verified every proof point against source. Production wiring always creates a SegmentManager (disruptor_storage.go:178-193), the read/ack paths use it (312-313, 340-341), performCheckpoint moves unacked durable msgs to segments then os.Remove()s the WAL file and purges its offset index (wal_manager.go:1290-1319), and this runs both on the 5-min timer and on graceful shutdown ("Final checkpoint before exit", 1242-1243). Recovery only ever reads the WAL: GetRecoverableMessages (disruptor_storage.go:871-889) calls only RecoverFromWAL, which scans only dataDir/shared/*.wal (wal_manager.go:319-320). There is no RecoverFromSegments/loadExistingSegments anywhere (grep confirms os.ReadDir is never used for segments), and getOrCreateQueueSegments (410-425) starts with an empty sealedSegments map and opens a fresh segment, so pre-restart .seg files are orphaned and unreachable even via point-lookup Read (166-168 returns not-found because the queue was never loaded into queueSegments). Net effect: any unacked DeliveryMode=2 message that survived a checkpoint is permanently lost on restart. Severity stays critical (silent, deterministic, unbounded durable data loss violating the core AMQP persistence guarantee). The proposed fix (segment discovery + frame/idx scan on first getOrCreateQueueSegments, a new RecoverFromSegments, merge into GetRecoverableMessages with WAL-wins dedupe, and run even when wal==nil) is the correct architectural direction and, with two required refinements below, is sound.

---

## [I4] Storage managers silently degrade to nil on disk/permission failure; durable publishes accepted but not persisted

**Rank 2 · P0** · Severity: 🟠 High · Category: durability · Effort: medium · ⚡ Quick win

> **Severity adjusted by debate:** claimed `medium` → agreed `high`.

### Impact
If the WAL (and/or segment) manager fails to construct at startup due to a disk/permission/ENOSPC/fd error, NewDisruptorStorageWithEngineConfig sets ds.wal = nil and returns a fully-constructed *DisruptorStorage with no error. The builder never checks for nil. The broker then starts normally and accepts publishes with DeliveryMode=2 (persistent). In StoreMessage the WAL write is guarded by `if ds.wal != nil`, so with wal==nil a durable publish is written only to the in-memory AtomicRing and StoreMessage returns nil (success). The publisher receives an ack/confirm, but nothing is on disk. On any restart or crash the message is gone -- classic silent durability violation for exactly the messages the client asked to be durable. This is worse than a hard failure because operators get no signal (no fatal error, no log at construction since err is discarded) until they lose data after a restart. It can occur even when metadata dirs were created successfully (e.g. a pre-existing wal/ dir owned by another user, disk filling up between the metadata MkdirAll and the WAL file OpenFile, or fd exhaustion in openNextFile), so the metadata-store nil guards that would otherwise error out on a durable-queue declare do not necessarily fire. The one place wal==nil surfaces an error (line 286-288) only triggers when the ring is ALSO full; a non-full ring silently swallows the message.

### Root cause
NewDisruptorStorageWithEngineConfig treats construction errors from the four persistence managers as recoverable by discarding the error and assigning nil (lines 160-181). The function signature returns no error, so the caller (server/builder.go) cannot detect the degraded state. Downstream, StoreMessage's durability write is behind `if ds.wal != nil`, turning a missing WAL into a silent no-op for persistent messages instead of a hard failure, and the fatal-only-when-ring-full branch (line 286) does not cover the common non-full case.

### Proof

- **`src/amqp-go/storage/disruptor_storage.go` : 160-197** — All four construction errors are discarded (err never logged or returned) and the manager set to nil. The constructor returns only *DisruptorStorage, no error, so the degraded state is invisible to callers.

```go
metadataStore, err := NewPersistentMetadataStore(dataDir)
 if err != nil {
     metadataStore = nil
 }
 ...
 walManager, err := NewWALManagerWithConfig(dataDir, walCfg)
 if err != nil {
     walManager = nil
 }
 ...
 return &DisruptorStorage{ ... wal: walManager, segments: segmentManager, ... }
```

- **`src/amqp-go/storage/disruptor_storage.go` : 257-294** — With ds.wal==nil the DeliveryMode==2 WAL write at line 260 is skipped entirely. For a non-full ring the message goes only into the in-memory ring and StoreMessage returns nil (success). The only wal==nil error path (line 286) requires the ring to be full (spilled), so ordinary durable publishes are silently accepted and lost on restart.

```go
func (ds *DisruptorStorage) StoreMessage(queueName string, message *protocol.Message) error {
 ring := ds.getOrCreateQueueRing(queueName)
 if ds.wal != nil && message.DeliveryMode == 2 {
     if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil { ... }
 }
 ...
 _, spilled, err := ring.ring.Store(message.DeliveryTag, message)
 ...
 if spilled {
     ... if ds.wal == nil { return fmt.Errorf("ring full and WAL unavailable ...") }
     return nil
 }
 ring.ack.OnPublish(message.DeliveryTag)
 return nil
}
```

- **`src/amqp-go/storage/wal_manager.go` : 155-173** — Confirms WAL construction really fails on disk/permission errors (MkdirAll, and createSharedWAL -> openNextFile os.OpenFile). These are exactly the errors silently swallowed into wal=nil.

```go
func NewWALManagerWithConfig(dataDir string, cfg WALConfig) (*WALManager, error) {
 walDir := filepath.Join(dataDir, "wal")
 if err := os.MkdirAll(walDir, 0755); err != nil { return nil, ... }
 sharedWAL, err := wm.createSharedWAL()
 if err != nil { return nil, ... }
```

- **`src/amqp-go/storage/wal_manager.go` : 489-513** — createSharedWAL can fail on file creation (openNextFile -> os.OpenFile with O_CREATE, line 742) independently of metadata directory creation, so WAL can be nil even when metadataStore constructed fine -- meaning the metadata-store nil guards on StoreQueue/StoreExchange won't necessarily catch the durable-declare and the loss stays silent.

```go
sharedDir := filepath.Join(wm.dataDir, "shared")
 if err := os.MkdirAll(sharedDir, 0755); err != nil { return nil, ... }
 ...
 if err := wal.openNextFile(); err != nil { return nil, fmt.Errorf("failed to open WAL file: %w", err) }
```

- **`src/amqp-go/server/builder.go` : 201-219** — The sole production call site never inspects the returned storage for a nil WAL/segment/metadata/offset manager and cannot (no error returned). The broker starts and logs a normal success message even when persistence is fully or partially broken.

```go
var storageImpl interfaces.Storage
 if b.storage != nil { storageImpl = b.storage } else {
     storageImpl = storage.NewDisruptorStorageWithEngineConfig(b.config.Storage.Path, checkpointInterval, b.config.GetEngine())
     ... logger.Info("Using disruptor-based storage" ...) }
```


### Fix

Make persistence-construction failures visible and fail-fast when persistence is expected. Two coordinated changes:

1) Return the error from the constructor instead of silently nil-ing managers. Change NewDisruptorStorageWithEngineConfig (and its wrappers NewDisruptorStorage / NewDisruptorStorageWithDataDir / NewDisruptorStorageWithCheckpointInterval) to return (*DisruptorStorage, error). On any of the four construction errors, decide behavior by policy: by default treat WAL, segment, and metadata construction failure as fatal (return the wrapped error) because a durable AMQP broker cannot honor its durability contract without them. Keep offsetStore optional (checkpointing is a best-effort optimization) but log a warning when it fails. Wrap each error with context (which manager, which dataDir) so operators see the real cause (EACCES/ENOSPC/etc.).

2) Update server/builder.go to propagate that error: `storageImpl, err = storage.NewDisruptorStorageWithEngineConfig(...); if err != nil { return nil, fmt.Errorf("failed to initialize durable storage at %q: %w", b.config.Storage.Path, err) }`. This turns silent degradation into a hard startup failure, which is the correct behavior for a broker that has been asked (via config) to persist.

3) Defense-in-depth in StoreMessage: if ds.wal == nil and message.DeliveryMode == 2, return an explicit error (`fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)`) instead of silently storing only in the ring. This guarantees that even if some future code path constructs a WAL-less storage, a durable publish can never be silently acknowledged. The publisher then gets a channel/connection error (or a nack under publisher confirms) rather than a false success.

Optionally, gate the fatal-vs-degraded decision on config: if the deployment is explicitly configured as in-memory-only (e.g. Storage.Path empty or an InMemory flag), allow nil managers and skip the durable-write error; otherwise fail fast. This preserves the intentional in-memory mode while closing the accidental-degradation hole.

Update all existing internal callers/tests of the four constructors to handle the new error return.

**Files to change:**
- `src/amqp-go/storage/disruptor_storage.go` — Change NewDisruptorStorageWithEngineConfig and the three wrapper constructors to return (*DisruptorStorage, error); return wrapped errors for metadata/WAL/segment failures (fatal) and log+continue for offsetStore (optional). In StoreMessage, return an explicit error for DeliveryMode==2 when ds.wal==nil instead of falling through to ring-only storage.
- `src/amqp-go/server/builder.go` — Capture and propagate the new error from NewDisruptorStorageWithEngineConfig (lines 208-212): assign to (storageImpl, err) and return nil, wrapped error on failure so the broker refuses to start with broken durable storage.
- `src/amqp-go/storage/*_test.go and any other internal callers` — Update all call sites of the four constructors to handle the added error return (e.g. `s, err := NewDisruptorStorage...; require.NoError(t, err)`).

**Code sketch:**

```go
// disruptor_storage.go
func NewDisruptorStorageWithEngineConfig(dataDir string, checkpointInterval time.Duration, engineCfg interfaces.EngineConfig) (*DisruptorStorage, error) {
    // ... ringBufferSize validation unchanged ...
    metadataStore, err := NewPersistentMetadataStore(dataDir)
    if err != nil {
        return nil, fmt.Errorf("metadata store init failed at %q: %w", dataDir, err)
    }
    walManager, err := NewWALManagerWithConfig(dataDir, walCfg)
    if err != nil {
        return nil, fmt.Errorf("WAL init failed at %q: %w", dataDir, err)
    }
    segmentManager, err := NewSegmentManagerWithConfig(dataDir, segCfg)
    if err != nil {
        return nil, fmt.Errorf("segment store init failed at %q: %w", dataDir, err)
    }
    offsetStore, err := NewOffsetCheckpointStoreWithInterval(dataDir, checkpointInterval)
    if err != nil {
        // best-effort: log and continue with nil offsetStore
        offsetStore = nil
    }
    walManager.SetSegmentManager(segmentManager)
    return &DisruptorStorage{ /* ... */ }, nil
}

// StoreMessage defense-in-depth:
if message.DeliveryMode == 2 {
    if ds.wal == nil {
        return fmt.Errorf("cannot persist durable message: WAL unavailable for queue %s", queueName)
    }
    if err := ds.wal.Write(queueName, message, message.DeliveryTag); err != nil {
        return fmt.Errorf("failed to write durable message to WAL: %w", err)
    }
}
```


**Test (TDD):** Add storage/disruptor_storage_test.go tests:
1) TestNewDisruptorStorage_FailsWhenWALDirUnwritable: create a temp dataDir, pre-create dataDir/wal as a regular file (or chmod the dir to 0500 on a non-root runner) so os.MkdirAll/OpenFile for the WAL fails while metadata dirs still succeed; assert NewDisruptorStorageWithEngineConfig returns a non-nil error and nil storage. Skip on root where perms are ignored.
2) TestStoreMessage_DurableWithoutWAL_ReturnsError: construct a DisruptorStorage with ds.wal forced to nil (via a test constructor or by injecting nil) and call StoreMessage with a DeliveryMode==2 message on a non-full ring; assert it returns an error and that GetMessage does NOT return it as persisted-recoverable (i.e., the publish was rejected rather than silently acknowledged).
3) TestBuilder_FailsToStartOnStorageInitError: point builder config Storage.Path at an unwritable path and assert Build() returns an error rather than a running server.

**Regression risk:** Medium. Changing the constructor signatures to return an error is a source-breaking API change that touches every caller and test in the storage/server packages; all must be updated in the same change or the build breaks. There is a legitimate in-memory/degraded mode use case (tests, ephemeral deployments) that relied on nil managers being tolerated -- must preserve it via an explicit opt-in (empty Storage.Path or an InMemory flag) so we do not turn intentional in-memory runs into startup failures. The StoreMessage durable-without-WAL error change could surface as new publish failures in any deployment that was silently running WAL-less; that is the intended correction but should be called out in release notes. Low risk of runtime regressions since the change is fail-fast at startup, not on the hot path (except the added nil check in StoreMessage, which is a single branch).

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether the metadata-store nil guard already catches this on a durable declare (making it non-silent). Resolved AGAINST the challenge: I verified NewPersistentMetadataStore creates <dataDir>/metadata/* (persistent_metadata.go:54-69) while the WAL creates <dataDir>/wal/* and its own files (wal_manager.go:156-171, createSharedWAL 489-513). These are independent subtrees, and the metadata store is constructed FIRST (disruptor_storage.go:160) before the WAL (line 173). So a partial failure hitting only the wal/ subtree or fd exhaustion in openNextFile leaves metadataStore non-nil (StoreQueue succeeds) but wal==nil — the loss stays silent. Challenge does not hold; bug confirmed.
- Challenged the fix's premise that there is a legitimate 'intentional in-memory mode via empty Storage.Path' to preserve. Resolved: config.Validate() at config/config.go:158-159 REJECTS an empty Storage.Path, so production always has a durable path and there is no empty-path in-memory mode to protect at the builder call site. The fail-fast change is therefore unambiguously correct for production; the in-memory concern only applies to direct test constructors (NewDisruptorStorage/WithDataDir), which are updated as part of the API-change churn. This slightly narrows the fix's optional config-gating rationale but does not weaken the fix.
- Challenged whether the proposed StoreMessage DeliveryMode==2 early-return regresses transient behavior. Resolved: verified the change is scoped to DeliveryMode==2 only; the transient ring-only path, the transient-spill-to-WAL path (lines 266-273), and the spill branch (280-289) all key off DeliveryMode!=2 and remain intact. No regression on the transient path.
- Challenged whether the publisher truly gets a false success. Confirmed via broker/storage_broker.go:859-862 (StoreMessage nil -> PublishMessage nil) and server/basic_handlers.go:375-376 (nil error -> sendBasicAck on confirm channels). The false positive-confirm is real.

**Fix concerns:**
- Constructor signature change to (*DisruptorStorage, error) is source-breaking across ~40 call-site references in 12+ test files plus production; all must be updated in the same change or the build breaks. Regression risk is medium but mechanical.
- The fix's suggestion to gate fatal-vs-degraded on empty Storage.Path is largely moot for the production call path because config.Validate() forbids empty Storage.Path (config/config.go:158-159). Recommend instead: keep the direct test constructors tolerant (or add a test-only WAL-less constructor) rather than adding a production InMemory flag that does not currently exist.
- Making NewPersistentMetadataStore failure fatal is defensible but is a behavior change vs today's tolerant nil metadataStore (methods already error cleanly). Recommend treating metadata failure as fatal for consistency but confirm no existing test/flow relies on a nil-metadata degraded broker before flipping it.
- The StoreMessage durable-without-WAL error will surface as new publish/nack failures in any deployment that was silently running WAL-less; that is the intended correction but should be called out in release notes as the colleague noted.

**Agreed resolution (authoritative):**

Real bug, confirmed at HIGH severity (durability/silent-data-loss). Verified against source at /Users/zohaib.hassan/repos/strangeq/src/amqp-go: NewDisruptorStorageWithEngineConfig (disruptor_storage.go:160-197) discards all four construction errors and nils the managers, returning only *DisruptorStorage with no error; the sole production caller (server/builder.go:208-212) cannot and does not detect this. StoreMessage (disruptor_storage.go:257-294) guards the durable WAL write behind `if ds.wal != nil` (line 260), so with wal==nil a DeliveryMode==2 publish on a non-full ring is stored only in the in-memory AtomicRing and returns nil; the only wal==nil error path (line 286-288) requires the ring to be spilled/full, so ordinary durable publishes are silently swallowed. PublishMessage returns nil, and basic_handlers.go:375-376 then sends basic.ack to the publisher — a positive publisher-confirm for a message never written to disk. GetRecoverableMessages (disruptor_storage.go:872-874) returns empty when wal==nil, so on restart the acked durable message is gone. The metadata and WAL directories are independent subtrees (persistent_metadata.go:54 creates <dataDir>/metadata/*; wal_manager.go:156 creates <dataDir>/wal/*, createSharedWAL at 489-513 opens files independently), so WAL can fail (pre-existing wal/ owned by another user, fd exhaustion in openNextFile, transient ENOSPC between the two MkdirAll calls) while metadata succeeds — meaning the metadataStore nil guards on StoreQueue/StoreExchange (which DO error cleanly, disruptor_storage.go:510-511) will not necessarily fire on a durable declare, keeping the loss fully silent. Agreed fix direction: (1) change the constructor(s) to return (*DisruptorStorage, error) and treat WAL/segment (and metadata) construction failure as fatal with wrapped context; (2) propagate the error in server/builder.go so the broker refuses to start; (3) add defense-in-depth in StoreMessage returning an explicit error for DeliveryMode==2 when ds.wal==nil rather than falling through to ring-only. The fix is sound and the StoreMessage change correctly scopes only to DeliveryMode==2, leaving transient ring-only and transient-spill-to-WAL paths (lines 266-273, 280-285) untouched.

---

## [I8] Pre-auth remote OOM DoS: unbounded inbound frame size in ReadFrameOptimized / ReadFrame

**Rank 3 · P0** · Severity: 🟠 High · Category: security · Effort: small · ⚡ Quick win

**Blocks:** I10

### Impact
Any unauthenticated remote peer that can open a TCP connection to the broker can send a single 7-byte AMQP frame header declaring a payload size of up to ~4 GiB (uint32). The server immediately executes make([]byte, size+1), allocating up to ~4 GiB before reading a single payload byte and before authentication. A handful of such connections exhausts host memory and OOM-kills the broker. Because the very first post-protocol-header read (connection.start-ok at server.go:233) uses the same unbounded reader, the allocation happens PRE-AUTH — no credentials required. The negotiated/advertised FrameMax (131072) is never enforced on inbound frames; it is only used to chunk OUTBOUND bodies (frame_helpers.go:232). This is a trivially exploitable, low-cost, high-impact denial of service.

### Root cause
ReadFrameOptimized and ReadFrame decode the 32-bit frame size directly from untrusted network bytes and pass it straight to make() with no upper-bound check. The server advertises a FrameMax in connection.tune and stores Config.Server.MaxFrameSize, but this limit is never plumbed into the frame reader; the read path has no knowledge of any cap. There is also no read deadline set on the socket during the handshake, so a slow/large read is not time-bounded either.

### Proof

- **`src/amqp-go/protocol/frame_optimized.go` : 38-46** — size is read from the untrusted 4-byte header and used directly as the make() length with no maximum. size can be up to 4294967295, so make allocates ~4 GiB before any payload validation. This is the exact hot-path reader used by the server.

```go
frameType := header[0]
channel := binary.BigEndian.Uint16(header[1:3])
size := binary.BigEndian.Uint32(header[3:7])

payload := make([]byte, size+1)
_, err = io.ReadFull(reader, payload)
```

- **`src/amqp-go/protocol/frame.go` : 83-90** — The non-optimized ReadFrame has the identical unbounded-allocation flaw, so any code path using it is equally vulnerable.

```go
size := binary.BigEndian.Uint32(header[3:7])

// Read the payload + end-byte
payload := make([]byte, size+1) // +1 for end-byte
_, err = io.ReadFull(reader, payload)
```

- **`src/amqp-go/server/server.go` : 232-237** — The FIRST frame read after the 8-byte protocol header uses the unbounded reader, and it happens BEFORE handleConnectionStartOK (line 251) authenticates the client. This makes the OOM reachable pre-auth.

```go
// Wait for connection.start-ok from client
frame, err := protocol.ReadFrameOptimized(conn.Conn)
```

- **`src/amqp-go/server/server.go` : 354-356** — The steady-state readFrames loop also uses the unbounded reader; every subsequent frame is likewise uncapped.

```go
for {
	// Read frame from TCP connection with optimized pooling (never blocks on processing)
	frame, err := protocol.ReadFrameOptimized(conn.Conn)
```

- **`src/amqp-go/server/frame_helpers.go` : 232-236** — MaxFrameSize is consumed ONLY for outbound body chunking. It is never passed to the inbound reader, confirming the negotiated limit is not enforced on reads.

```go
maxFrameSize := uint32(s.Config.Server.MaxFrameSize)
maxBodyPerFrame := int(maxFrameSize) - 8
```

- **`src/amqp-go/server/connection_handlers.go` : 67-71** — The server advertises FrameMax=131072 to the client, but the tune-ok frame read at server.go:267 never parses or enforces the client's FrameMax, and nothing enforces the advertised value on inbound reads.

```go
tuneMethod := &protocol.ConnectionTuneMethod{
	ChannelMax: 65535,
	FrameMax:   131072, // Maximum frame size in bytes
	Heartbeat:  60,
}
```

- **`src/amqp-go/server/server.go` : 136-144** — No SetReadDeadline/SetDeadline is set on the socket before the handshake (a repo-wide grep for SetReadDeadline/SetDeadline returns no non-test hits), so the oversized read is neither size-bounded nor time-bounded pre-auth.

```go
func (s *Server) handleConnection(conn net.Conn) {
	protoHeader := make([]byte, 8)
	_, err := conn.Read(protoHeader)
```

- **`src/amqp-go/protocol/frame_comprehensive_test.go` : 128-146** — Existing test comment claims a size limit is 'enforced at protocol level' but only exercises MarshalBinary (outbound). There is no test that a large inbound size is rejected by ReadFrame/ReadFrameOptimized, confirming the gap is untested and unmitigated.

```go
// This should still work (size limit enforced at protocol level)
data, err := frame.MarshalBinary()
```


### Fix

Enforce a hard upper bound on inbound frame size in the frame readers. Add a package-level constant MaxInboundFrameSize (e.g. 128 MiB or the AMQP-advertised FrameMax plus a small margin) and a variant reader that takes an explicit cap. Concretely: (1) In protocol/frame_optimized.go, add ReadFrameOptimizedWithLimit(reader io.Reader, maxSize uint32) that, after decoding `size`, returns an error if `size > maxSize` (and rejects size such that size+1 would overflow) BEFORE calling make(). Keep ReadFrameOptimized as a thin wrapper that passes a safe default cap (do NOT leave the uncapped behavior as the default the server uses). (2) Apply the identical guard to ReadFrame in protocol/frame.go. (3) In server.go, thread the negotiated/configured limit into the reader: at handshake reads (lines 233, 267, 279) use a conservative default cap (e.g. the advertised FrameMax of 131072 plus overhead, or a global MaxInboundFrameSize); in readFrames (line 356) use max(Config.Server.MaxFrameSize, advertised FrameMax) as the cap. Since the server advertises FrameMax=131072 and clients must honor it, capping inbound at that value (plus 8-byte overhead) is spec-compliant; use a slightly larger safety ceiling (e.g. MaxInboundFrameSize = 128*1024*1024) if you want to tolerate clients that ignore FrameMax while still preventing OOM. (4) Additionally set a read deadline on the socket during the handshake in handleConnection to bound slow-loris style pre-auth reads (defensive, secondary). When the cap is exceeded, return a descriptive error so readFrames/processConnectionFrames closes the connection (per AMQP, the server may send a connection.close with reply-code 501 FRAME_ERROR / 505, but simply closing is acceptable for an abusive oversized frame). Overflow note: computing size+1 in uint32 when size==0xFFFFFFFF wraps to 0, producing a zero-length payload rather than a huge alloc, but any size between the cap and 0xFFFFFFFE still triggers a multi-GiB alloc; the cap check must use `size > maxSize` on the raw uint32 before any +1 arithmetic.

**Files to change:**
- `src/amqp-go/protocol/frame_optimized.go` — Add MaxInboundFrameSize const and ReadFrameOptimizedWithLimit(reader, maxSize); reject size>maxSize before make(); make ReadFrameOptimized delegate to it with a safe default cap.
- `src/amqp-go/protocol/frame.go` — Add the same size>maxSize guard to ReadFrame (or a ReadFrameWithLimit variant) before make([]byte, size+1).
- `src/amqp-go/server/server.go` — In readFrames (line 356) and the three handshake reads (lines 233, 267, 279), call the limited reader passing a cap derived from Config.Server.MaxFrameSize clamped to protocol.MaxInboundFrameSize (use the tighter advertised FrameMax for the pre-auth handshake reads).
- `src/amqp-go/server/connection_handlers.go` — Optional/defensive: parse client FrameMax from tune-ok and store the negotiated value on the Connection so readFrames can use min(advertised, client) as the cap; also consider setting a read deadline during handshake in handleConnection.

**Code sketch:**

```go
// protocol/frame_optimized.go
const MaxInboundFrameSize uint32 = 128 * 1024 * 1024 // 128 MiB hard ceiling

func ReadFrameOptimizedWithLimit(reader io.Reader, maxSize uint32) (*Frame, error) {
    headerPtr := getFrameHeader()
    header := *headerPtr
    defer putFrameHeader(headerPtr)
    if _, err := io.ReadFull(reader, header); err != nil {
        return nil, err
    }
    frameType := header[0]
    channel := binary.BigEndian.Uint16(header[1:3])
    size := binary.BigEndian.Uint32(header[3:7])
    if size > maxSize {
        return nil, fmt.Errorf("frame size %d exceeds max %d", size, maxSize)
    }
    payload := make([]byte, size+1)
    if _, err := io.ReadFull(reader, payload); err != nil {
        return nil, err
    }
    if payload[size] != FrameEnd {
        return nil, fmt.Errorf("invalid frame end-byte: expected 0x%02X, got 0x%02X", FrameEnd, payload[size])
    }
    frame := GetFrame()
    frame.Type = frameType; frame.Channel = channel; frame.Size = size; frame.Payload = payload[:size]
    return frame, nil
}

func ReadFrameOptimized(reader io.Reader) (*Frame, error) {
    return ReadFrameOptimizedWithLimit(reader, MaxInboundFrameSize)
}

// server/server.go readFrames:
//   cap := uint32(s.Config.Server.MaxFrameSize)
//   if cap == 0 || cap > protocol.MaxInboundFrameSize { cap = protocol.MaxInboundFrameSize }
//   frame, err := protocol.ReadFrameOptimizedWithLimit(conn.Conn, cap)
```


**Test (TDD):** Add a TDD unit test in protocol/frame_optimized_test.go: TestReadFrameOptimizedRejectsOversizedFrame — construct a 7-byte header with type=FrameMethod, channel=0, and size=0xFFFFFFF0 (near-max) via binary.BigEndian, feed it through a bytes.Reader that ONLY contains the 7 header bytes (no payload), call ReadFrameOptimized, and assert it returns a non-nil error containing 'exceeds max' WITHOUT attempting the multi-GiB allocation (the test completes instantly and does not OOM; before the fix this call would attempt make([]byte, ~4GiB) and either OOM or hang on io.ReadFull). Add a paired positive test that a size at exactly the cap and a normal small frame still parse correctly. Also add a server-level test in server that opens a real net.Pipe/loopback connection, writes the 8-byte protocol header followed by a frame header declaring size=0x7FFFFFFF, and asserts the server closes the connection with an error rather than allocating; assert peak allocation via runtime.ReadMemStats stays bounded (e.g. < 16 MiB delta).

**Regression risk:** Low. The change only adds a rejection path for frames larger than a generous cap; well-behaved clients honoring the advertised FrameMax (131072) are unaffected. Main risk is setting the cap too low and rejecting legitimate large body frames — mitigate by defaulting the ceiling well above Config.Server.MaxFrameSize (e.g. 128 MiB) and clamping to the configured value only where you are confident clients honor FrameMax. If a read deadline is added to the handshake, ensure it is cleared after handshake completes so it does not prematurely close idle authenticated connections; keeping the deadline change optional/separate reduces risk. No changes to serialization or wire format, so protocol compliance and interop tests remain valid.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether MaxFrameSize might already be plumbed into the reader somewhere I missed — grepped all non-test uses; it appears ONLY at frame_helpers.go:232 for outbound chunking, config default/validation, and builder setters. No inbound read path references it. Challenge resolved in favor of the finding.
- Challenged whether the ConnectionTuneOK handler negotiates or stores a client FrameMax that could later bound reads — read connection_handlers.go:320-323 directly; it only logs and returns nil, storing nothing. Confirms the negotiated limit is truly unenforced.
- Challenged the overflow claim by running Go arithmetic: confirmed size=0xFFFFFFF0 produces make([]byte, ~4GiB) (real 4 GiB alloc) and size=0xFFFFFFFF wraps size+1 to 0. This validates the fix's insistence on checking size>maxSize on the raw uint32 before +1.
- Challenged whether the fix's cap check placement matters given the wrap — confirmed it does: without a raw-uint32 cap, any size in [cap, 0xFFFFFFFE] still allocates multi-GiB. The colleague correctly specified the check before arithmetic.
- Additional finding NOT in the colleague's report: there is no recover() anywhere in the server package (grepped). With size=0xFFFFFFFF, make(len 0) succeeds then payload[size] indexes a zero-length slice -> panic in the read goroutine that crashes the whole broker process. This is a second, even-cheaper crash vector (no memory pressure needed). The same size>maxSize cap fixes it, so it does not change the fix, but it reinforces the HIGH severity.

**Fix concerns:**
- The pre-auth handshake reads (server.go:233,267,279) should use the TIGHT cap (advertised FrameMax 131072 + 8 byte overhead), NOT the generous 128 MiB ceiling. A start-ok/tune-ok/open method frame is tiny; there is no legitimate reason to accept a 128 MiB pre-auth frame. Using the tight cap here shrinks the pre-auth allocation budget to KB-scale and closes the OOM window much harder for unauthenticated peers. The colleague mentions this as an option ('use the tighter advertised FrameMax for the pre-auth handshake reads') but it should be treated as required, not optional.
- The size=0xFFFFFFFF wrap-to-zero case must also be covered by the test matrix, not just size=0xFFFFFFF0. Since the cap check is size>maxSize on the raw value, 0xFFFFFFFF > maxSize is true and gets rejected correctly, but a regression test should assert this specific value is rejected so a future refactor that moves the check after +1 arithmetic (where 0xFFFFFFFF wraps to 0 and passes) is caught.
- Config.Server.MaxFrameSize is validated as >0 (config.go:188) but not validated as <= MaxInboundFrameSize. When clamping in readFrames, ensure cap = min(config value, ceiling) so a misconfigured very-large MaxFrameSize cannot re-open the OOM. The colleague's codeSketch does clamp (if cap==0 || cap>MaxInboundFrameSize { cap = MaxInboundFrameSize }) — this is correct; just make sure it is actually implemented, not left as a comment.
- Consider sending a proper connection.close with reply-code 501 (FRAME_ERROR) rather than a bare TCP close for the oversized-frame case on an already-handshaked connection, for protocol politeness/interop; a bare close is acceptable for a clearly abusive pre-auth frame. Minor, non-blocking.

**Agreed resolution (authoritative):**

Real bug, severity HIGH, confirmed by direct source reading. protocol/frame_optimized.go:42 (ReadFrameOptimized) and protocol/frame.go:86 (ReadFrame) both execute make([]byte, size+1) where size is a uint32 read directly from untrusted network bytes with zero upper-bound check, allowing an unauthenticated peer to trigger a ~4 GiB allocation per malicious 7-byte frame header. Pre-auth reachability confirmed: server.go:233 reads the first post-protocol-header frame via ReadFrameOptimized BEFORE handleConnectionStartOK (server.go:251) performs authentication. The advertised FrameMax=131072 (connection_handlers.go:69) is never enforced on inbound frames — the ConnectionTuneOK handler (connection_handlers.go:320-323) merely logs and returns nil without parsing or storing the client's FrameMax, and MaxFrameSize (default 131072) is consumed only for OUTBOUND body chunking at frame_helpers.go:232. No SetReadDeadline/SetDeadline exists in any non-test code (verified by grep). Agreed fix: add package-level MaxInboundFrameSize (128 MiB hard ceiling) and a ReadFrameOptimizedWithLimit/ReadFrameWithLimit variant that rejects size>maxSize on the RAW uint32 BEFORE any +1 arithmetic (overflow-safe: verified 0xFFFFFFF0 yields a ~4 GiB make and 0xFFFFFFFF wraps size+1 to 0); make ReadFrameOptimized/ReadFrame delegate with a safe default cap so the uncapped path is never the server default; thread Config.Server.MaxFrameSize (clamped to the ceiling) into readFrames (server.go:356) and use a conservative cap on the three handshake reads (server.go:233,267,279). Read-deadline addition is a reasonable secondary defense against slow-loris but should be kept separate and cleared after handshake. TDD test that feeds a header declaring size near-max and asserts an instant 'exceeds max' error without OOM is the correct verification.

---

## [I5] Authentication and authorization are inert through the public ServerBuilder: enabling auth silently admits every client as guest

**Rank 4 · P0** · Severity: 🟠 High · Category: security · Effort: medium

**Blocks:** I6

### Impact
Any server constructed through the public ServerBuilder (including cmd/amqp-server/main.go, the shipped binary) never authenticates clients. Build() never assigns Server.Authenticator or Server.MechanismRegistry and never constructs a FileAuthenticator/registry from the config. So even when an operator explicitly turns on auth — via WithFileAuthentication(userFile) or a config file / AMQP_* env with Security.AuthenticationEnabled=true and an auth.json defining users/passwords — handleConnectionStartOK finds s.Authenticator==nil / s.MechanismRegistry==nil and falls through to the else branch that sets conn.Username="guest" and returns nil, admitting the client with no credential check. The offered credentials (user/password in the PLAIN response) are ignored entirely; wrong or absent passwords are accepted. Authorization is doubly broken: authorize() requires conn.User (an *interfaces.User), but the guest fallback never sets conn.User, so if an operator also enables AuthorizationEnabled every operation is rejected with 403 (denial of service) rather than being correctly evaluated. Net effect: the security features exposed by the public API are a no-op that fails open for authentication and fails closed for authorization, with no error or warning logged at startup. Only hand-wired Server structs (as done in the test files) actually enforce auth.

### Root cause
ServerBuilder.Build() was never wired to translate the authentication configuration/authenticator into the Server. It reads b.storage, b.broker, b.metrics, b.logger but ignores b.authenticator, and the Server struct literal omits Authenticator and MechanismRegistry (leaving them nil). Additionally Build() never reads config.Security.AuthenticationEnabled / AuthenticationBackend / AuthenticationConfig["user_file"] / AuthenticationFilePath to construct a FileAuthenticator + mechanism registry. The runtime handler compounds this by treating a missing authenticator as an implicit allow-as-guest instead of a misconfiguration error, so the omission fails silently and open.

### Proof

- **`src/amqp-go/server/builder.go` : 182-267** — Build() constructs the Server. It uses b.storage, b.broker, b.metrics, b.logger, but the field b.authenticator is never referenced anywhere in Build(), and the Server struct literal (fields Addr, Connections, Log, Config, Broker, TransactionManager, MetricsCollector, StartTime) omits both Authenticator and MechanismRegistry, leaving them nil. Build() also never reads b.config.Security.AuthenticationEnabled / AuthenticationConfig["user_file"] to construct an authenticator.
- **`src/amqp-go/server/builder.go` : 126-141** — WithAuthenticator stores auth into b.authenticator and WithFileAuthentication sets AuthenticationEnabled=true, AuthenticationBackend="file", and AuthenticationConfig["user_file"]=userFile. None of these values are consumed by Build(), so both the fluent authenticator and the file-auth config are dead.
- **`src/amqp-go/server/connection_handlers.go` : 267-308** — handleConnectionStartOK: 'if s.Authenticator != nil && s.MechanismRegistry != nil { ...authenticate... } else { conn.Username = "guest"; ...allowing connection as guest...; }'. Because Build() leaves both fields nil, execution always takes the else branch: the client is admitted as guest regardless of the credentials in startOK.Response, even when AuthenticationEnabled is true.
- **`src/amqp-go/cmd/amqp-server/main.go` : 161-171** — The shipped binary builds the server via server.NewServerBuilder().WithConfig(cfg) (+ optional WithMetrics) and calls Build(). It never calls WithAuthenticator, and Build() ignores the config's auth settings, so a config with Security.AuthenticationEnabled=true yields a server that admits everyone as guest. main.go even prints 'Authentication: Enabled (file)' (lines 195-197, 211-213) while enforcing nothing.
- **`src/amqp-go/server/authz.go` : 19-36** — authorize() returns AccessRefused when conn.User==nil. The guest fallback in handleConnectionStartOK sets conn.Username but never conn.User, so if AuthorizationEnabled is turned on together with the builder, every operation is denied with 403 — authorization also cannot function through the builder.
- **`src/amqp-go/auth_integration_test.go` : 38-41** — Auth only works when the fields are set by hand: 'srv.Authenticator = authenticator; srv.MechanismRegistry = server.NewMechanismRegistryAdapter(registry)'. A repo-wide grep shows these two fields are assigned ONLY in test files (auth_integration_test.go, authz_integration_test.go, server/vhost_authz_test.go, server/authz_handler_test.go) and never in production code.
- **`src/amqp-go/config/config.go` : 44-49** — DefaultConfig ships with AuthenticationEnabled=false and AuthorizationEnabled=false, so the out-of-box default is 'no auth' (not itself the bug). The defect is that turning these on via the public API has no effect — the security controls are unreachable through the builder.

### Fix

Wire authentication into ServerBuilder.Build() so that (a) an explicitly provided authenticator (WithAuthenticator) is honored, and (b) when Security.AuthenticationEnabled is true and no authenticator was supplied, Build() constructs one from config (backend "file" -> auth.NewFileAuthenticator using AuthenticationConfig["user_file"], falling back to Security.AuthenticationFilePath). In both auth-enabled cases, also construct a mechanism registry (auth.DefaultRegistry, or a registry filtered to Security.AuthMechanisms) wrapped with server.NewMechanismRegistryAdapter, and assign both Server.Authenticator and Server.MechanismRegistry in the struct literal. If AuthenticationEnabled is true but authenticator construction fails or yields nil, Build() must return an error (fail closed) rather than producing a server that admits guests. Second, harden handleConnectionStartOK: when AuthenticationEnabled is true but s.Authenticator==nil or s.MechanismRegistry==nil, do NOT fall through to allow-as-guest — send connection.close 403 ACCESS_REFUSED (or, since this is a server misconfiguration, 541 INTERNAL_ERROR) and reject the connection, so a wiring regression can never silently fail open again. Also set conn.User for the guest path when auth is disabled is not needed, but ensure the authenticated path continues to set conn.User (it already does at connection_handlers.go:290-292) so authorization works. Keep the mechanisms advertised in sendConnectionStart consistent by preferring s.MechanismRegistry.String() (already implemented at connection_handlers.go:32-34).

**Files to change:**
- `src/amqp-go/server/builder.go` — In Build(), after creating the logger and before/at the Server struct literal, resolve the authenticator and mechanism registry. Add: var authenticator interfaces.Authenticator = b.authenticator; var mechReg MechanismRegistry; if b.config.Security.AuthenticationEnabled { if authenticator == nil { switch b.config.Security.AuthenticationBackend { case "file", "": userFile := b.config.Security.AuthenticationFilePath; if uf, ok := b.config.Security.AuthenticationConfig["user_file"].(string); ok && uf != "" { userFile = uf }; fa, err := auth.NewFileAuthenticator(userFile); if err != nil { return nil, fmt.Errorf("failed to initialize file authenticator: %w", err) }; authenticator = fa; default: return nil, fmt.Errorf("unsupported authentication backend: %s", b.config.Security.AuthenticationBackend) } }; registry := auth.DefaultRegistry(); mechReg = NewMechanismRegistryAdapter(registry); if authenticator == nil { return nil, fmt.Errorf("authentication enabled but no authenticator could be constructed") } } else if b.authenticator != nil { authenticator = b.authenticator; mechReg = NewMechanismRegistryAdapter(auth.DefaultRegistry()) }. Then add Authenticator: authenticator and MechanismRegistry: mechReg to the Server struct literal. Add the auth and interfaces imports if not already present (interfaces is imported; add "github.com/maxpert/amqp-go/auth").
- `src/amqp-go/server/connection_handlers.go` — In handleConnectionStartOK, replace the final 'else { conn.Username = "guest"; ...allowing as guest... }' so that when authentication IS enabled but s.Authenticator==nil || s.MechanismRegistry==nil it returns s.sendConnectionClose(conn, 541, "INTERNAL_ERROR", "authentication misconfigured") instead of admitting the connection. Only fall through to guest when Security.AuthenticationEnabled is false (that branch already exists at lines 259-265). Concretely: reorder so the enabled-but-unconfigured case is an explicit reject, never an allow.

**Code sketch:**

```go
// builder.go, inside Build() before the Server{} literal
var authenticator interfaces.Authenticator = b.authenticator
var mechReg MechanismRegistry
if b.config.Security.AuthenticationEnabled {
    if authenticator == nil {
        switch b.config.Security.AuthenticationBackend {
        case "file", "":
            userFile := b.config.Security.AuthenticationFilePath
            if uf, ok := b.config.Security.AuthenticationConfig["user_file"].(string); ok && uf != "" {
                userFile = uf
            }
            fa, err := auth.NewFileAuthenticator(userFile)
            if err != nil {
                return nil, fmt.Errorf("failed to initialize file authenticator: %w", err)
            }
            authenticator = fa
        default:
            return nil, fmt.Errorf("unsupported authentication backend: %s", b.config.Security.AuthenticationBackend)
        }
    }
    mechReg = NewMechanismRegistryAdapter(auth.DefaultRegistry())
} else if b.authenticator != nil {
    authenticator = b.authenticator
    mechReg = NewMechanismRegistryAdapter(auth.DefaultRegistry())
}
// ...
server := &Server{
    // ...existing fields...
    Authenticator:     authenticator,
    MechanismRegistry: mechReg,
}

// connection_handlers.go, in handleConnectionStartOK
if s.Authenticator != nil && s.MechanismRegistry != nil {
    // existing authenticate path (unchanged)
} else {
    // auth enabled here (disabled case already returned above) but not wired -> fail closed
    s.Log.Error("Authentication enabled but not configured", zap.String("connection_id", conn.ID))
    return s.sendConnectionClose(conn, 541, "INTERNAL_ERROR", "authentication misconfigured")
}
```


**Test (TDD):** Add a TDD test in the server package (or an integration test alongside auth_integration_test.go) that does NOT hand-set srv.Authenticator: build the server purely through the public builder — server.NewServerBuilder().WithConfig(cfg).WithFileAuthentication(tmpAuthFile).Build() with an auth.json defining user 'alice'/'secret' — start it, then (a) assert amqp.Dial with alice:wrongpassword FAILS with an access-refused/403 error, and (b) assert amqp.Dial with alice:secret SUCCEEDS and a channel opens. A second test asserts that building with WithFileAuthentication but connecting with no credentials / an unknown user is rejected. A third unit test asserts Build() returns a non-nil Server.Authenticator and Server.MechanismRegistry whenever config.Security.AuthenticationEnabled==true. These fail on current code (dial with wrong password currently succeeds as guest; Authenticator is nil) and pass after the fix.

**Regression risk:** Moderate. (1) Existing integration tests that hand-wire srv.Authenticator/srv.MechanismRegistry after NewServer/Build (auth_integration_test.go, authz_integration_test.go, vhost_authz_test.go, authz_handler_test.go) continue to work because explicit assignment overrides Build's output; but NewServer now itself constructs a FileAuthenticator when AuthenticationEnabled is true, which for the default './auth.json' path will auto-create a default guest user (FileAuthenticator.createDefaultFile) — confirm this does not surprise tests or write files in unexpected directories; scope the auto-create carefully. (2) The default config keeps AuthenticationEnabled=false, so the common no-auth path is unchanged (guest fallback at the top of handleConnectionStartOK still applies). (3) The hardened reject means any deployment that set AuthenticationEnabled=true expecting the (broken) permissive behavior will now correctly refuse unauthenticated clients — this is the intended fix but is a behavior change for anyone relying on the bug. (4) Restricting advertised mechanisms to Security.AuthMechanisms could break clients if the requested mechanism is dropped; keeping DefaultRegistry (PLAIN+ANONYMOUS) is the low-risk choice.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged whether handleConnectionStartOK's return value actually gates the connection (else the fix's reject-by-error is inert). Verified at server.go:249-256: a non-nil error causes processConnectionFrames to return and defer conn.Conn.Close() to fire; the current guest path returns nil so the client proceeds. Fix approach holds.
- Challenged the fix's use of auth.DefaultRegistry(): DefaultRegistry (mechanism.go:60-65) registers ANONYMOUS, and AnonymousMechanism.Authenticate (anonymous.go:19-41) admits any client as a full-permission guest ignoring credentials. sendConnectionStart advertises MechanismRegistry.String() (connection_handlers.go:33-34). So the naive fix leaves a fail-open ANONYMOUS path. Resolved: registry must be built from Security.AuthMechanisms (default PLAIN) or exclude ANONYMOUS.
- Challenged the NewFileAuthenticator side effect: load() creates ./auth.json with a guest/guest admin when the file is missing (file_auth.go:64-73, 116-160). Resolved: a fail-closed fix must error on a missing configured user file rather than auto-create credentials.
- Challenged the `else if b.authenticator != nil` branch: because handleConnectionStartOK short-circuits to guest when AuthenticationEnabled==false (connection_handlers.go:259-265), WithAuthenticator alone still won't authenticate. Resolved: fix should also set AuthenticationEnabled=true when an authenticator is explicitly provided.
- Challenged the authorization-DoS claim location: the colleague cites authz.go (channel ops), but processConnectionOpen (connection_handlers.go:150-157) rejects the connection outright at connection.open when conn.User==nil and AuthorizationEnabled is true, so the denial manifests even earlier than channel operations. Confirms and slightly strengthens the fail-closed authorization claim.

**Fix concerns:**
- auth.DefaultRegistry() includes ANONYMOUS (anonymous.go:19-41 accepts any client as full-permission guest); combined with sendConnectionStart advertising MechanismRegistry.String() (connection_handlers.go:33-34), the proposed fix still admits credential-less clients via ANONYMOUS. Build the registry from Security.AuthMechanisms (default ['PLAIN']) instead.
- NewFileAuthenticator auto-creates ./auth.json with a guest/guest administrator on a missing file (file_auth.go:64-73,116-160); a fail-closed fix must error when the configured user file is absent rather than silently create a default admin.
- The `else if b.authenticator != nil` path leaves AuthenticationEnabled=false, so handleConnectionStartOK (connection_handlers.go:259-265) returns guest before ever authenticating; WithAuthenticator alone remains ineffective unless the fix also enables AuthenticationEnabled.
- Fix should confirm config.Validate() does not need updating to enforce that an auth backend/user file is present when AuthenticationEnabled is true (currently Validate does not gate this).

**Agreed resolution (authoritative):**

Real high-severity security bug, confirmed by direct source reading. ServerBuilder.Build() (src/amqp-go/server/builder.go:182-287) never references b.authenticator and its Server{} struct literal (lines 258-267) omits Authenticator and MechanismRegistry, so both are nil for every server built through the public API — including the shipped binary (cmd/amqp-server/main.go:161-171, which even prints "Authentication: Enabled" at 195-197/211-213 while enforcing nothing). In handleConnectionStartOK (connection_handlers.go:259-308), when AuthenticationEnabled is true the guard `if s.Authenticator != nil && s.MechanismRegistry != nil` is always false, so execution falls to the else branch (303-308) that sets conn.Username="guest" and returns nil — admitting the client with no credential check (fail-open auth). server.go:249-256 confirms only a non-nil error from the handler closes the connection, so the nil return lets the client through. Authorization is fail-closed: authorize() (authz.go:19-36) and processConnectionOpen (connection_handlers.go:150-157) reject when conn.User==nil, which the guest path never sets, so enabling AuthorizationEnabled via the builder denies/refuses every connection. Grep confirms Authenticator/MechanismRegistry are assigned only in test files. Config can be enabled via file/env (interfaces/config.go:92-96, config/builder.go:129-131), so the operator-enables-auth scenario is real. The fix DIRECTION is correct (wire b.authenticator, construct a FileAuthenticator from config when enabled, assign both Server fields, and fail-closed when auth is enabled but unconfigured — the reject-by-returning-error will work given server.go:251-256). But the fix as written needs revision on three points before it is safe: (1) It builds the mechanism registry with auth.DefaultRegistry(), which registers ANONYMOUS (mechanism.go:60-65); AnonymousMechanism.Authenticate (anonymous.go:19-41) accepts ANY client with no credentials and returns a guest user with full .* permissions. Since sendConnectionStart advertises MechanismRegistry.String() (connection_handlers.go:33-34), the "fixed" server would still advertise ANONYMOUS and admit any client that selects it as an administrator — reintroducing fail-open. The registry must be built from Security.AuthMechanisms (default ["PLAIN"]) or explicitly exclude ANONYMOUS when auth is enabled; the colleague's regressionRisk note calling DefaultRegistry the "low-risk choice" is backwards. (2) NewFileAuthenticator on a missing file calls createDefaultFile (file_auth.go:64-73, 116-160), silently writing ./auth.json (relative to CWD) with a guest/guest bcrypt administrator; a fail-closed fix must not auto-create credentials — it should error if the configured user file is absent when auth is explicitly enabled. (3) The `else if b.authenticator != nil` branch wires an authenticator when AuthenticationEnabled is false, but handleConnectionStartOK short-circuits to guest at 259-265 in that case, so WithAuthenticator alone still won't authenticate; the fix should also set AuthenticationEnabled=true when an authenticator is explicitly supplied. With these three revisions the fix is complete and regression-safe (default no-auth path unchanged; hand-wired test servers still override Build's output).

---

## [I6] SASL ANONYMOUS mechanism grants administrator-equivalent guest with no loopback restriction and no credential check

**Rank 5 · P0** · Severity: 🟠 High · Category: security · Effort: small · ⚡ Quick win

### Impact
Any client that selects the ANONYMOUS SASL mechanism receives a `guest` user with Tags=[administrator], `.*` configure/write/read permissions on vhost `/`, and no credential verification (the SASL response is ignored). Critically, `LoopbackOnly` is left at its zero value (false), so — unlike the file-auth `guest`, which is `LoopbackOnly: true` — this identity is NOT restricted to localhost. The connection handler only enforces the loopback restriction when `user.LoopbackOnly == true`, so the check is silently skipped for the ANONYMOUS guest. Whenever an embedder wires the library's shipped `auth.DefaultRegistry()` (which unconditionally registers ANONYMOUS) into `Server.MechanismRegistry`, the mechanism string advertised in connection.start becomes "PLAIN ANONYMOUS", and any remote client can pick ANONYMOUS and obtain full administrator-equivalent access to the broker over the network with no password. Note: the default `cmd/amqp-server` binary does not wire a MechanismRegistry (Build() leaves it nil) and defaults AuthMechanisms=["PLAIN"], so the stock binary is not affected out of the box; the exposure is via the documented "DefaultRegistry" library path and any deployment that enables ANONYMOUS.

### Root cause
AnonymousMechanism.Authenticate() hard-codes an administrator guest user but omits the `LoopbackOnly` field (defaulting to false) and performs no credential check, diverging from the file-auth guest which sets LoopbackOnly=true. Additionally, `auth.DefaultRegistry()` registers ANONYMOUS unconditionally rather than gating it on config (e.g. Security.AuthMechanisms), so the insecure mechanism is advertised whenever the default registry is used, independent of what the operator configured.

### Proof

- **`src/amqp-go/auth/anonymous.go` : 19-41** — Authenticate() creates an administrator guest (Username=guest, Tags=[administrator], `.*` Configure/Write/Read perms on vhost `/`). The `response []byte` parameter is never read (no credential check), and the User literal never sets LoopbackOnly, so it defaults to false. Excerpt: `user := &interfaces.User{ Username: "guest", VHostPermissions: [...Permission{Configure:".*", Write:".*", Read:".*"}], Tags: []string{"administrator"}, ... }; return user, nil`
- **`src/amqp-go/interfaces/server.go` : 72-78** — User struct field `LoopbackOnly bool // if true, user may only connect from localhost (RabbitMQ guest restriction)`. Its zero value false means no localhost restriction; the ANONYMOUS mechanism leaves it at false.
- **`src/amqp-go/auth/file_auth.go` : 126-140** — The intended/secure guest (file backend default) is an identical admin (`Tags: []string{"administrator"}`, `.*` perms) but with `LoopbackOnly: true`. The ANONYMOUS mechanism produces the same admin identity WITHOUT that restriction, proving the omission is a defect rather than intentional parity.
- **`src/amqp-go/server/connection_handlers.go` : 192-215** — Loopback enforcement is guarded by `if loopbackOnly && !isLoopbackConn(conn.Conn)` in both the authorization-enabled and authorization-disabled branches. Because ANONYMOUS leaves LoopbackOnly=false, the guard is never entered and a remote ANONYMOUS client is allowed. The code comment at line 202 explicitly names 'guest via ANONYMOUS mechanism' as a case this was meant to cover, but the mechanism never sets the flag the guard depends on.
- **`src/amqp-go/auth/mechanism.go` : 59-65** — DefaultRegistry() registers both PlainMechanism and AnonymousMechanism unconditionally, ignoring Security.AuthMechanisms (which defaults to only ["PLAIN"] per config/config.go:48). So using the library's named default registry advertises and enables ANONYMOUS regardless of operator config.
- **`src/amqp-go/server/connection_handlers.go` : 31-50** — sendConnectionStart sets `mechanisms = s.MechanismRegistry.String()` when a registry is present, so with DefaultRegistry the advertised string is 'PLAIN ANONYMOUS'. handleConnectionStartOK (line 270) then dispatches to whichever mechanism name the client supplies, so a remote client can select ANONYMOUS.

### Fix

Two coordinated changes.

1) Make the ANONYMOUS guest loopback-restricted by default so it matches the file-auth guest and RabbitMQ semantics. In auth/anonymous.go, set `LoopbackOnly: true` on the returned User. This ensures that even if ANONYMOUS is advertised, a remote (non-loopback) client is refused at connection.open by the existing check in connection_handlers.go (both the authorization-enabled branch at ~line 192 and the authorization-disabled branch at ~line 207 already read user.LoopbackOnly and enforce isLoopbackConn). This closes the remote-admin exposure with a one-line change while preserving the local-development use case ANONYMOUS is documented for.

2) Stop advertising/registering ANONYMOUS unless the operator explicitly opts in. Change auth.DefaultRegistry() so it only registers PLAIN, and add a separate helper (e.g. RegistryForMechanisms(mechs []string) or DefaultRegistryWithAnonymous()) that registers ANONYMOUS only when the configured Security.AuthMechanisms list contains "ANONYMOUS". This makes the insecure mechanism opt-in and consistent with the config field that already exists (AuthMechanisms defaults to ["PLAIN"]). Update the integration tests that rely on DefaultRegistry() including ANONYMOUS to use the explicit opt-in helper.

Optionally (defense in depth), reduce the granted privileges: the ANONYMOUS guest should arguably NOT carry Tags=[administrator]; consider dropping the administrator tag and/or narrowing perms, but at minimum (1) and (2) close the remote-admin hole. Keep the WARNING doc comment and consider logging a Warn when the ANONYMOUS mechanism authenticates a connection.

**Files to change:**
- `src/amqp-go/auth/anonymous.go` — In Authenticate(), add `LoopbackOnly: true` to the returned interfaces.User literal so the ANONYMOUS guest is restricted to localhost, matching the file-auth guest at file_auth.go:140. Optionally drop Tags=[administrator] to further reduce blast radius.
- `src/amqp-go/auth/mechanism.go` — Change DefaultRegistry() to register only PlainMechanism. Add a new constructor (e.g. func RegistryForMechanisms(names []string) *Registry) that always registers PLAIN and registers ANONYMOUS only if 'ANONYMOUS' is present in names. Document the behavior change.
- `src/amqp-go/auth_integration_test.go` — Update the DefaultRegistry() call sites (lines ~35, ~109, ~224) that expect ANONYMOUS to instead use the explicit opt-in constructor, and add an assertion that a remote (non-loopback) ANONYMOUS connection is refused.
- `src/amqp-go/authz_integration_test.go` — Update the DefaultRegistry() call site (~line 119) to the opt-in constructor if ANONYMOUS behavior is exercised.
- `src/amqp-go/auth/auth_test.go` — Update TestDefaultRegistry (line 140) to reflect that DefaultRegistry now contains only PLAIN, and add a test for the new opt-in constructor.

**Code sketch:**

```go
// auth/anonymous.go
user := &interfaces.User{
    Username: "guest",
    VHostPermissions: []interfaces.VHostPermission{{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}}},
    Tags:         []string{"administrator"},
    Groups:       []string{"guest"},
    LoopbackOnly: true, // FIX: restrict ANONYMOUS guest to localhost, matching file-auth guest
    Metadata:     map[string]interface{}{"mechanism": "ANONYMOUS"},
}

// auth/mechanism.go
func DefaultRegistry() *Registry { // now PLAIN-only
    r := NewRegistry()
    r.Register(&PlainMechanism{})
    return r
}

func RegistryForMechanisms(names []string) *Registry {
    r := NewRegistry()
    r.Register(&PlainMechanism{})
    for _, n := range names {
        if strings.EqualFold(n, "ANONYMOUS") {
            r.Register(&AnonymousMechanism{})
        }
    }
    return r
}
```


**Test (TDD):** Add auth-layer and server-layer TDD tests:
1) auth/anonymous_test.go: TestAnonymousMechanismIsLoopbackOnly — call (&AnonymousMechanism{}).Authenticate(nil, nil); assert returned user.LoopbackOnly == true (currently false — fails before fix).
2) server/vhost_authz_test.go-style: TestAnonymousRemoteConnectionRefused — build a server whose MechanismRegistry contains ANONYMOUS, drive connection.start-ok selecting ANONYMOUS over a non-loopback mockConn (reuse newNonLoopbackPipeConn), then call processConnectionOpen; assert the connection is closed with ACCESS_REFUSED 'can only connect via localhost'. Before the fix this connection succeeds.
3) auth/mechanism_test.go: TestDefaultRegistryExcludesAnonymous — assert DefaultRegistry().Get("ANONYMOUS") returns an error and String()=="PLAIN"; TestRegistryForMechanismsOptsInAnonymous — assert RegistryForMechanisms([]string{"PLAIN","ANONYMOUS"}).Get("ANONYMOUS") succeeds.

**Regression risk:** Low-to-moderate. Setting LoopbackOnly=true on the ANONYMOUS guest changes behavior for any current user relying on remote ANONYMOUS connections (they will now be refused off-localhost) — but that is precisely the vulnerable behavior being closed, and it aligns ANONYMOUS with the file-auth guest. Removing ANONYMOUS from DefaultRegistry() is an API/behavior change: callers that used DefaultRegistry() expecting ANONYMOUS advertised must switch to the opt-in constructor; the four integration/unit test call sites must be updated (they will fail-fast, making the migration obvious). No hot-path or concurrency impact.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged the impact framing that 'any deployment using DefaultRegistry' is exposed. Verified builder.go Build() never sets Authenticator or MechanismRegistry, and connection_handlers.go:268 requires BOTH to be non-nil for mechanism auth to run. Resolution: colleague already conceded the stock binary is safe; the true vulnerable path is an embedder setting both fields with an ANONYMOUS-registry over a network listener — narrower but still a real HIGH remote-admin exposure. Kept severity at high, not critical.
- Challenged whether both fix changes are required to close the hole. Verified LoopbackOnly:true alone triggers refusal at connection_handlers.go:192 and 207 regardless of registry contents, so change (1) is load-bearing and change (2) is defense-in-depth. Resolution: accepted both as the right combined resolution, but noted (2) is hardening rather than strictly necessary.
- Challenged the regression-risk description for TestAuthenticationANONYMOUS. Read auth_integration_test.go:112-114 and found it sets MechanismRegistry but leaves Authenticator nil, so the mechanism never runs (guest else-branch at 303-308) and it dials localhost. Resolution: the LoopbackOnly change does not break it; the DefaultRegistry PLAIN-only change is what changes that test — the fix's test-migration list is still correct, only the causal reasoning needed the Authenticator-nil nuance.

**Fix concerns:**
- Fix's regression note attributes the TestAuthenticationANONYMOUS breakage partly to the wrong cause; the true trigger is the DefaultRegistry PLAIN-only change, not LoopbackOnly, because that test leaves Authenticator nil and never exercises the mechanism.
- Removing ANONYMOUS from DefaultRegistry() is a public API/behavior change for library embedders; ensure it is documented in the release notes/CHANGELOG since callers relying on the old behavior will silently stop advertising ANONYMOUS until they migrate to the opt-in constructor.
- The optional 'drop administrator tag' suggestion should be scoped explicitly out of this fix or done deliberately, since ANONYMOUS local-dev users may still expect admin-equivalent local access; changing it could surprise local workflows even though it is safe post-LoopbackOnly.

**Agreed resolution (authoritative):**

Confirmed real bug at HIGH severity. Verified in source: auth/anonymous.go:19-41 hard-codes an administrator-equivalent guest (Tags=[administrator], .* configure/write/read on vhost /) and omits LoopbackOnly (defaults false), while the file-auth guest at file_auth.go:126-140 is the identical admin identity WITH LoopbackOnly:true. The loopback enforcement in connection_handlers.go is present in both the authorization-enabled branch (line 192, loopbackOnly read at 177) and the authorization-disabled branch (line 207) and its comment at line 202 explicitly names 'guest via ANONYMOUS mechanism' as the intended case — but the flag it depends on is never set, so a remote ANONYMOUS client is admitted with full admin rights. mechanism.go:60-65 DefaultRegistry() registers ANONYMOUS unconditionally, ignoring Security.AuthMechanisms (default [PLAIN]). Exposure scope is narrower than a blanket 'any DefaultRegistry deployment': the stock cmd/amqp-server is unaffected because builder.go Build() (lines 182-287) never sets Authenticator or MechanismRegistry, and connection_handlers.go:268 only runs mechanism auth when BOTH Authenticator!=nil AND MechanismRegistry!=nil. The vulnerable path is an embedder wiring both fields with an ANONYMOUS-containing registry over a network listener. Even so, that is a documented library path granting remote unauthenticated administrator access, warranting HIGH. Agreed fix: (1) set LoopbackOnly:true in auth/anonymous.go — this is the load-bearing one-line change that closes the remote-admin hole via the already-present enforcement at lines 192 and 207 (verified reads the field); (2) make ANONYMOUS opt-in by making DefaultRegistry() PLAIN-only plus a RegistryForMechanisms/opt-in constructor gated on Security.AuthMechanisms (strings is already imported in mechanism.go, so the sketch compiles) — defense-in-depth, not strictly required once (1) is in place. Optionally drop the administrator tag to reduce blast radius (not blocking). Update the four test call sites (auth_integration_test.go:35/109/224, authz_integration_test.go:119, auth/auth_test.go:140) and add the proposed TDD tests. Note the fix JSON slightly understated one regression detail: TestAuthenticationANONYMOUS (auth_integration_test.go:112-114) sets MechanismRegistry but leaves Authenticator nil, so it never actually exercises the ANONYMOUS mechanism (falls to the guest else-branch at lines 303-308) and connects via localhost — the LoopbackOnly change would not break it; the DefaultRegistry-becomes-PLAIN-only change is what alters that test's expectations. Fix verdict: sound.

---

# P1

## [I9] Field-table codec only implements S (long-string) and I (int32); all other AMQP field types desync the parser and break the handshake / header parsing

**Rank 6 · P1** · Severity: 🟠 High · Category: protocol-compliance · Effort: medium

### Impact
Any AMQP 0.9.1 client (or message) that places a fixed-width or non-S/non-I field type at the TOP LEVEL of a field table corrupts the byte cursor and fails the whole table decode. Concrete consequences: (1) connection.start-ok: a client that adds a bool/int8/int16/int64/float/double/timestamp/decimal/short-string-typed property to ClientProperties (very common — e.g. clients advertising numeric or boolean capabilities directly, Java/.NET/Python clients, or amqp091-go users who set config.Properties with a bool/int) fails ParseConnectionStartOK -> the broker rejects the handshake (503/connection drop), so those clients cannot connect at all. (2) Message headers (content.go:104) and method Arguments (exchange.declare, queue.declare, basic.publish, basic.consume, queue.bind): any top-level header/argument that is a bool, int, float, timestamp, or byte-array (e.g. x-delay:int used by the delayed-message pattern, x-max-priority, x-message-ttl as int, or any application header with a boolean/number) corrupts parsing and returns a decode error, killing the channel/connection or dropping the message. The default rabbitmq/amqp091-go handshake survives ONLY by luck: its only non-string top-level property, 'capabilities', is a nested Table (wire marker 'F') whose <uint32len><bytes> framing coincidentally matches the long-string framing the default-case decoder assumes, so the opaque bytes are skipped by exactly the right amount. Change the client's top-level properties to include any fixed-width type and the connection breaks.

### Root cause
serializeFieldTable and decodeFieldTable in protocol/methods.go implement only two of the ~15 AMQP 0.9.1 field-value types: 'S' (long-string) and 'I' (int32). Encoding falls back to fmt.Sprintf("%v", v) written as 'S' for everything else, discarding the real type and value. Decoding falls back, for any unknown type marker, to decodeLongString — which reads the byte immediately after the type marker as the first byte of a big-endian uint32 length. For any fixed-width type (e.g. 't' bool: 1 payload byte; 'b'/'B': 1; 's': 2; 'l'/'d'/'T': 8; 'f'/'I' handled or 4; 'D': 5) that assumed uint32 length is garbage, so the cursor jumps to a wrong offset and the remainder of the table (and everything after it in the method/header frame: mechanism, response, locale) is misread. The author explicitly flagged this at methods.go:657-659 ('For simplicity in this initial implementation ... In a real implementation, we'd properly serialize the map'). Nested tables ('F') and long-strings ('S') and byte-arrays ('x') happen to share the <uint32len><bytes> shape so they are skipped correctly by coincidence; every other type desyncs.

### Proof

- **`src/amqp-go/protocol/methods.go` : 686-699** — Encoder handles only string ('S') and int ('I'); default case coerces every other Go type to fmt.Sprintf %v and writes it as an 'S' long-string, losing the real type and value.
- **`src/amqp-go/protocol/methods.go` : 744-762** — Decoder switch handles only 'S' and 'I'; the default case calls decodeLongString for every other type marker, so any fixed-width type marker ('t','b','B','s','l','f','d','D','T') is misread as the start of a uint32-length long-string, desyncing the cursor.
- **`src/amqp-go/protocol/methods.go` : 789-806** — decodeLongString reads the 4 bytes after the type marker as a big-endian uint32 length; for a bool ('t' + single 0/1 payload byte) this yields a garbage multi-megabyte length that fails the 'extends beyond data' check, aborting the whole table decode.
- **`src/amqp-go/protocol/methods.go` : 226-236** — ParseConnectionStartOK decodes ClientProperties via decodeFieldTable first; a desync/error there fails the entire connection.start-ok parse, breaking the handshake (503/drop).
- **`src/amqp-go/protocol/content.go` : 99-108** — Message content-header 'headers' uses the same decodeFieldTable, so any top-level bool/int/float/timestamp/array application header corrupts header parsing, not just the handshake.
- **`github.com/rabbitmq/amqp091-go@v1.10.0/connection.go` : 997-1002** — Default reference client only survives because its sole non-string top-level property, 'capabilities', is a nested Table ('F') that shares <uint32len><bytes> framing with 'S' and is skipped as opaque bytes; a client adding any fixed-width top-level property would desync.
- **`github.com/rabbitmq/amqp091-go@v1.10.0/write.go` : 292-405** — Full compliant marker set a client can send (t,b,B,s,I,l,f,d,D,S,A,T,F,x,V) — the server recognizes only S and I; t,b,B,s,l,f,d,D,A,T,V all desync (F/x survive by framing coincidence).
- **`empirical test against decodeFieldTable` : n/a** — Hand-encoded table {durable: bool true, product:'S'"hello"} fed to decodeFieldTable produced err='failed to decode value for key durable: long string extends beyond data', table=nil — empirically confirming the desync and total-parse-failure on a top-level bool.

### Fix

Implement a fully AMQP-0.9.1-compliant field-value codec covering all standard type markers, and validate lengths defensively. Concretely:

1) Rewrite serializeFieldTable's per-value encoding into a writeFieldValue(value) helper that emits the correct 1-byte type marker + payload for each Go type, matching rabbitmq/amqp091-go's write.go marker set: bool->'t'(1 byte 0/1), int8->'b', byte/uint8->'B', int16->'s', int32/int->'I'(u)int32, int64->'l', float32->'f', float64->'d', Decimal->'D'(scale byte + int32), string->'S'(uint32 len + bytes), []interface{}->'A'(uint32 bytelen + encoded elements), time.Time->'T'(int64 unix secs), map[string]interface{}->'F'(nested table via existing serializeFieldTable), []byte->'x'(uint32 len + bytes), nil->'V'. REMOVE the fmt.Sprintf("%v") default; for a Go type you cannot map, return an explicit error rather than silently corrupting the wire. Keep 'I' encoding as int32 (matches amqp091-go which encodes both int and int32 as 'I').

2) Rewrite decodeFieldTable's value switch into a decodeFieldValue(data, offset) helper handling every marker above, each advancing offset by exactly the right amount and bounds-checking against len(data) AND tableEnd before reading. For 'A' (field-array) recurse element-by-element until the sub-length is consumed; for 'F' recurse into decodeFieldTable; for 'D' read scale(1)+value(4); 't'->1, 'b'/'B'->1, 's'->2, 'I'/'f'->4, 'l'/'d'/'T'->8, 'x'/'S'->uint32 len + bytes, 'V'->0. For an UNKNOWN marker, return an error instead of falling back to decodeLongString (fail fast rather than desync). Preserve numeric fidelity: decode 'I' as int32, 'l' as int64, 't' as bool, 'T' as time.Time, etc., so round-trips and consumer-side .(bool)/.(int32) type assertions work.

3) Defense-in-depth caps: reject any single string/byte-array/nested-table whose declared length exceeds a sane bound (e.g. the negotiated frameMax or a hard cap like 128 MiB) before slicing, and optionally cap element/recursion depth for 'A'/'F' to avoid pathological nesting. The outer tableLen<=len(data) check already exists (methods.go:714) but per-element checks are what actually prevent desync-driven huge reads.

4) Keep decodeShortString/decodeLongString for the top-level short-string keys and the S value; they are already correct.

Ensure content.go and all Arguments call sites need no changes since they call the same two functions.

**Files to change:**
- `src/amqp-go/protocol/methods.go` — Replace the value-encoding switch in serializeFieldTable (686-699) with a complete writeFieldValue covering t/b/B/s/I/l/f/d/D/S/A/T/F/x/V and returning an error for unmappable Go types (drop the fmt.Sprintf %v fallback). Replace the value-decoding switch in decodeFieldTable (744-762) with a call to a new decodeFieldValue helper that handles every marker with per-field bounds checks and returns an error (not a long-string fallback) for unknown markers. Add per-length caps for S/x/F/A. Add a Decimal struct if one is not already defined.
- `src/amqp-go/protocol/methods_test.go (new or existing test file)` — Add round-trip and desync-regression tests (see test field).

**Code sketch:**

```go
// decodeFieldValue returns (value, newOffset, error)
func decodeFieldValue(data []byte, offset, end int) (interface{}, int, error) {
    if offset >= end { return nil, offset, fmt.Errorf("missing field type") }
    t := data[offset]; offset++
    need := func(n int) error { if offset+n > end { return fmt.Errorf("field %q truncated", t) }; return nil }
    switch t {
    case 't': if err:=need(1);err!=nil{return nil,offset,err}; v:=data[offset]!=0; return v, offset+1, nil
    case 'b': if err:=need(1);err!=nil{return nil,offset,err}; return int8(data[offset]), offset+1, nil
    case 'B': if err:=need(1);err!=nil{return nil,offset,err}; return data[offset], offset+1, nil
    case 's': if err:=need(2);err!=nil{return nil,offset,err}; return int16(binary.BigEndian.Uint16(data[offset:])), offset+2, nil
    case 'I': if err:=need(4);err!=nil{return nil,offset,err}; return int32(binary.BigEndian.Uint32(data[offset:])), offset+4, nil
    case 'l': if err:=need(8);err!=nil{return nil,offset,err}; return int64(binary.BigEndian.Uint64(data[offset:])), offset+8, nil
    case 'f': if err:=need(4);err!=nil{return nil,offset,err}; return math.Float32frombits(binary.BigEndian.Uint32(data[offset:])), offset+4, nil
    case 'd': if err:=need(8);err!=nil{return nil,offset,err}; return math.Float64frombits(binary.BigEndian.Uint64(data[offset:])), offset+8, nil
    case 'D': if err:=need(5);err!=nil{return nil,offset,err}; return Decimal{Scale:data[offset], Value:int32(binary.BigEndian.Uint32(data[offset+1:]))}, offset+5, nil
    case 'T': if err:=need(8);err!=nil{return nil,offset,err}; return time.Unix(int64(binary.BigEndian.Uint64(data[offset:])),0), offset+8, nil
    case 'S', 'x':
        if err:=need(4);err!=nil{return nil,offset,err}
        n := int(binary.BigEndian.Uint32(data[offset:])); offset+=4
        if n<0 || offset+n>end { return nil,offset, fmt.Errorf("long value truncated") }
        b := data[offset:offset+n]; offset+=n
        if t=='S' { return string(b), offset, nil }; return append([]byte(nil), b...), offset, nil
    case 'F':
        // nested table: decodeFieldTable expects a uint32 len prefix at offset
        tbl, no, err := decodeFieldTable(data, offset); return tbl, no, err
    case 'A':
        if err:=need(4);err!=nil{return nil,offset,err}
        n := int(binary.BigEndian.Uint32(data[offset:])); offset+=4
        if offset+n>end { return nil,offset, fmt.Errorf("array truncated") }
        arrEnd := offset+n; var arr []interface{}
        for offset < arrEnd { var v interface{}; var err error; v,offset,err = decodeFieldValue(data, offset, arrEnd); if err!=nil {return nil,offset,err}; arr=append(arr,v) }
        return arr, offset, nil
    case 'V': return nil, offset, nil
    default: return nil, offset, fmt.Errorf("unsupported field type %q", t)
    }
}
```


**Test (TDD):** TDD in protocol/methods_test.go: (1) Round-trip: for each type build map[string]interface{}{"b":true,"i8":int8(-3),"u8":byte(200),"i16":int16(-1000),"i32":int32(70000),"i64":int64(1<<40),"f32":float32(1.5),"f64":float64(2.5),"ts":time.Unix(1700000000,0),"s":"hello","nested":map[string]interface{}{"x":true},"arr":[]interface{}{int32(1),"a",true}} , call EncodeFieldTable then decodeFieldTable, and assert every key decodes to the same typed value (bool stays bool, int32 stays int32, etc.). (2) Cross-compat test: use rabbitmq/amqp091-go's exported writeTable/Table via a small in-package fixture OR hand-encode the exact bytes amqp091-go emits for a table containing a top-level bool 't' followed by another entry, feed to decodeFieldTable, and assert BOTH entries decode correctly and the returned offset equals len(data) (this is the exact case that currently fails with 'long string extends beyond data'). (3) Handshake integration: extend the existing real-client integration test to Dial with amqp091.Config{Properties: amqp091.Table{"product":"t","custom_bool":true,"custom_int":int32(5)}} and assert the connection succeeds (currently it would fail the handshake). (4) Truncation/cap test: feed a table declaring a huge string length and assert a clean error, not a panic or over-read.

**Regression risk:** Medium. The codec is used on every connection handshake, every content header, and every method Arguments table (exchange/queue declare, bind, publish, consume), so a mistake ripples widely. Main risks: (a) 'I' must remain int32-typed to match how amqp091-go encodes both int and int32 as 'I' — decoding as Go int is fine but re-encoding must emit 'I', and consumer code that type-asserts .(int) on decoded headers may now get int32 (audit broker code that reads decoded header/argument values, e.g. x-message-ttl, x-max-priority, x-expires, x-max-length, dead-letter args). (b) Nested-table 'F' decode must consume its own uint32 length prefix exactly as decodeFieldTable already does. (c) Changing the default from 'silently coerce to string' to 'return error on unknown type' means genuinely malformed input now errors instead of being swallowed — desirable, but verify callers surface it as a connection/channel exception rather than crashing. Mitigate by landing the round-trip + amqp091-go byte-compat tests first (TDD) and running the full existing integration suite (TestSimpleQueueOperations, TestBasicPublishConsume, TestManualAcknowledgment, auth/authz/tls integration) which already exercise the real client end to end.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged 'survives by luck' — verified it: amqp091-go NewConnectionProperties() emits product/version/platform (all strings->'S') plus capabilities as a nested Table ('F'). 'F' framing is <uint32 len><bytes>, identical in shape to 'S', so decodeLongString reads the correct length and skips exactly the right bytes, storing the raw table as an opaque string. Coincidence claim holds.
- Challenged whether the impact is only theoretical — reproduced it end to end: default handshake OK, but adding a single top-level bool client property yields server error 'failed to decode value for key custom_bool: long string extends beyond data' and client-side Exception (403), connection rejected. Not theoretical.
- Challenged the int->int32 regression risk the colleague flagged as needing a broker audit — grepped for x-message-ttl/x-max-priority/x-max-length/x-expires/dead-letter reads: the broker consumes NO typed x- arguments, so no .(int) assertion breaks. Separately, the persistence codec (storage/codec.go:738-754) already handles int, int32, and int64 explicitly, so it tolerates the type change. This de-risks the concern the colleague rated Medium.
- Found an ADDITIONAL defect the colleague understated: the broker's OWN serverProperties (server/connection_handlers.go:16-29) places 'capabilities' as a map[string]interface{} of bools at the TOP level; serializeFieldTable stringifies it via fmt.Sprintf to 'map[basic.nack:true ...]' as an 'S' string. It stays wire-valid so the client doesn't desync, but the broker advertises garbage capabilities — the encode side is non-compliant on output too, strengthening the bug beyond the decode-only framing. The proposed writeFieldValue with 'F' nested-table support fixes this.
- Challenged the fix sketch's 'F' case: it calls decodeFieldTable(data, offset) which bounds-checks the nested tableLen against len(data), NOT against the current field-value 'end'. A malformed nested length could consume bytes belonging to the parent table (still within the buffer, so no memory-safety issue since the outer loop uses tableEnd), a minor robustness gap. Recommended clamping nested F/A lengths to the parent end. Not a blocker; the fix is otherwise sound.

**Fix concerns:**
- Sketch's 'F' nested-table decode delegates to decodeFieldTable which validates length against len(data), not the enclosing field-value end. Clamp nested F/A declared lengths to the parent 'end' to prevent a malformed nested length from crossing table boundaries (buffer-safe today, but can cause a downstream desync).
- Encode side must be updated in the same change (not just decode): map[string]interface{} must emit 'F' (nested table) recursively rather than fmt.Sprintf-to-'S', otherwise the broker keeps advertising a stringified capabilities table to clients even after the decoder is fixed. The colleague's fix text covers this ('map[string]interface{}->F'), just ensure it lands together.
- 'I' must be decoded as int32 and re-encoded as 'I' to match amqp091-go (which encodes both int and int32 as 'I'); verified storage/codec.go tolerates int32, but land the round-trip + amqp091-go byte-compat tests first to lock this in.
- Add per-length caps for S/x/F/A (colleague point 3) before slicing; the existing outer tableLen<=len(data) check at methods.go:714 is not sufficient to prevent per-element huge-length reads.

**Agreed resolution (authoritative):**

Confirmed real, high-severity protocol-compliance bug. The field-table codec in src/amqp-go/protocol/methods.go implements only 'S' (long-string) and 'I' (int32) of the ~15 AMQP 0.9.1 field-value types. serializeFieldTable (686-699) coerces every other Go type to fmt.Sprintf("%v") emitted as 'S'; decodeFieldTable (744-762) routes every unknown type marker to decodeLongString (789-806), which reads the 4 bytes after the marker as a uint32 length. For any fixed-width top-level type (t/b/B/s/l/f/d/D/T) that length is garbage, so the cursor desyncs and the whole table decode aborts.

I independently reproduced BOTH the unit-level desync and the end-to-end handshake break. A hand-encoded {durable:bool, product:"hi"} table returns err="long string extends beyond data", tbl=nil. Dialing the broker with amqp091.Config{Properties: {custom_bool:true, custom_int:int32(5)}} fails: server logs "failed to decode client properties: ... key custom_bool: long string extends beyond data" and the client gets Exception (403), connection rejected. The default amqp091-go handshake succeeds only because its non-string top-level property 'capabilities' is a nested Table ('F'), whose <uint32len><bytes> framing coincidentally matches 'S' and is skipped as opaque bytes (verified against amqp091-go@v1.10.0 connection.go:82-88, 997-1002; write.go/read.go marker set). Same decodeFieldTable is used for content headers (content.go:104) and all method Arguments tables, so the impact spans handshake, headers, and exchange/queue/bind/publish/consume arguments.

The proposed fix (full writeFieldValue/decodeFieldValue covering t/b/B/s/I/l/f/d/D/S/A/T/F/x/V with per-field bounds checks, dropping the fmt.Sprintf %v encode fallback and the decodeLongString unknown-marker fallback, returning errors instead) is correct and complete for interop: its marker set matches amqp091-go's write.go and read.go exactly, including 'I' as int32 and 'l' as int64. Agreed to adopt it with TDD (round-trip + amqp091-go byte-compat + handshake-with-typed-properties + truncation-cap tests). Severity stays high (not critical: it breaks connections/channels and drops messages but is not remote code execution or silent data corruption of delivered payloads; the desync fails closed with a decode error rather than mis-delivering).

---

## [I10] connection.tune-ok discarded and MaxConnections / MaxChannelsPerConnection / MaxFrameSize unenforced (unbounded resource exhaustion + memory DoS)

**Rank 7 · P1** · Severity: 🟠 High · Category: protocol-compliance · Effort: medium

### Impact
A single client can exhaust broker resources with no config-driven guard: (1) The inbound frame reader allocates make([]byte, size+1) from a client-controlled uint32 frame size with NO upper bound, so one crafted frame header advertising ~4GB payload forces a multi-GB allocation attempt per connection before io.ReadFull ever fails — a trivial memory-DoS. (2) Unlimited channel.open per connection: conn.Channels.Store grows without bound, each channel carrying maps/mutexes/consumer state. (3) Unlimited accepted connections: acceptLoop spawns a goroutine + several sub-goroutines per connection with no MaxConnections cap, enabling FD/goroutine/memory exhaustion. (4) The negotiated ChannelMax/FrameMax/Heartbeat the client returns in tune-ok are silently discarded, so the server holds no per-connection negotiated limits. Configured limits (MaxConnections=1000, MaxChannelsPerConnection=2047, MaxFrameSize=131072, MaxMessageSize=16MB) are settable and validated but dead at runtime, giving operators a false sense of protection.

### Root cause
The handshake reads connection.tune-ok purely to advance the state machine (only frame.Type is checked) and never deserializes it; ConnectionTuneOKMethod has Serialize but no Deserialize, so no negotiated ChannelMax/FrameMax/Heartbeat is ever stored on the Connection. Separately, runtime enforcement points were never wired to config: channel.open creates channels unconditionally, the accept loop never consults MaxConnections, and the optimized inbound frame reader never validates the wire size against MaxFrameSize (that config value is used only for OUTBOUND frame splitting in frame_helpers.go).

### Proof

- **`src/amqp-go/server/server.go` : 266-276** — tune-ok is read only to advance the handshake; only frame.Type is inspected. The negotiated ChannelMax/FrameMax/Heartbeat in frame.Payload is never parsed or stored.

```go
// Wait for connection.tune-ok from client
frame, err = protocol.ReadFrameOptimized(conn.Conn)
if err != nil { ... }
if frame.Type != protocol.FrameMethod { ... return }
```

- **`src/amqp-go/server/connection_handlers.go` : 320-323** — The connection-method dispatcher also treats tune-ok as a no-op, confirming the negotiated values are discarded everywhere.

```go
case protocol.ConnectionTuneOK: // Method ID 31
    s.Log.Debug("Received connection.tune-ok", ...)
    return nil
```

- **`src/amqp-go/protocol/methods.go` : 333-360** — There is a Serialize but NO Deserialize method for ConnectionTuneOKMethod, so the server cannot parse the client's negotiated tune-ok even if it wanted to.

```go
type ConnectionTuneOKMethod struct { ChannelMax uint16; FrameMax uint32; Heartbeat uint16 }
func (m *ConnectionTuneOKMethod) Serialize() ([]byte, error) { ... }
```

- **`src/amqp-go/protocol/frame_optimized.go` : 40-43** — The inbound frame size is a fully client-controlled uint32 and is used directly to allocate a buffer with no bound check against MaxFrameSize — a direct memory-exhaustion vector.

```go
size := binary.BigEndian.Uint32(header[3:7])
payload := make([]byte, size+1)
_, err = io.ReadFull(reader, payload)
```

- **`src/amqp-go/server/frame_helpers.go` : 232-236** — MaxFrameSize is used ONLY for outbound frame splitting, never for validating inbound frames — proving the config is not enforced on the read path.

```go
maxFrameSize := uint32(s.Config.Server.MaxFrameSize)
maxBodyPerFrame := int(maxFrameSize) - 8
```

- **`src/amqp-go/server/channel_handlers.go` : 13-28** — channel.open unconditionally stores a new channel with no count check against MaxChannelsPerConnection; a client can open channels unbounded.

```go
case protocol.ChannelOpen:
    newChannel := protocol.NewChannel(channelID, conn)
    conn.Channels.Store(channelID, newChannel)
    ... return s.sendChannelOpenOK(conn, channelID)
```

- **`src/amqp-go/server/server.go` : 116-164** — acceptLoop/handleConnection add connections to s.Connections and spawn goroutines with no check against s.Config.Network.MaxConnections.

```go
for { conn, err := s.Listener.Accept(); ... go s.handleConnection(conn) } ... s.Connections[connection.ID] = connection
```

- **`src/amqp-go/server/server.go` : 506-509** — Heartbeat interval is hardcoded and ignores negotiation; combined with no SetReadDeadline on the reader, the server never times out dead/silent peers.

```go
// Send heartbeats very frequently (every 5 seconds)...
// TODO: Use negotiated heartbeat value from connection.tune
heartbeatInterval := 5 * time.Second
```


### Fix

Wire the already-configured limits into the runtime paths and honor tune-ok. Four independent changes, all low-risk:

1) FrameMax enforcement on the inbound reader (highest priority — memory DoS). Change ReadFrameOptimized (and legacy ReadFrame) to accept a maxFrameSize and reject oversized frames BEFORE allocating. Thread the negotiated FrameMax through Connection: add MaxFrameSize uint32 to protocol.Connection (default from s.Config.Server.MaxFrameSize at handleConnection time). Add ReadFrameOptimizedLimited(reader, maxSize) that, after reading the 7-byte header, checks `if maxSize > 0 && size > maxSize { return nil, fmt.Errorf("frame size %d exceeds max %d", size, maxSize) }` before `make([]byte, size+1)`. Have server.go readFrames and handshake reads call the limited variant with conn.MaxFrameSize. Keep ReadFrameOptimized as a wrapper passing 0 (unbounded) for backward compat in tests.

2) Parse and store tune-ok. Add ConnectionTuneOKMethod.Deserialize([]byte) mirroring ParseConnectionTune (ChannelMax uint16, FrameMax uint32, Heartbeat uint16). In server.go after reading the tune-ok frame (~line 267), deserialize the payload (skip the 4-byte class/method id) and store negotiated values on conn: MaxChannels = min-with-0-means-unlimited of server and client ChannelMax; MaxFrameSize = min-with-0-means-unlimited of server and client FrameMax; NegotiatedHeartbeat = client Heartbeat (authoritative per spec). Change sendConnectionTune to advertise configured MaxFrameSize/MaxChannelsPerConnection/HeartbeatIntervalMS instead of hardcoded 65535/131072/60.

3) Enforce MaxChannelsPerConnection in channel.open. sync.Map has no len, so maintain an atomic ChannelCount on Connection (increment on open, decrement on channel.close and connection cleanup). In processChannelSpecificMethod ChannelOpen, before Store, if ChannelCount >= negotiated/config max, send a connection.close with 506 CHANNEL_ERROR (hard error per AMQP) and return without storing.

4) Enforce MaxConnections in the accept path. In handleConnection, under s.Mutex before adding to s.Connections, check `if s.Config != nil && s.Config.Network.MaxConnections > 0 && len(s.Connections) >= s.Config.Network.MaxConnections { s.Mutex.Unlock(); conn.Close(); return }` while holding the same lock that guards the map insert.

Optional follow-up: honor negotiated heartbeat in sendHeartbeats and add conn.Conn.SetReadDeadline based on 2x negotiated heartbeat in readFrames to detect dead peers.

**Files to change:**
- `src/amqp-go/protocol/frame_optimized.go` — Add ReadFrameOptimizedLimited(reader, maxSize uint32) that validates size against maxSize before allocation; make ReadFrameOptimized delegate with maxSize=0.
- `src/amqp-go/protocol/methods.go` — Add ConnectionTuneOKMethod.Deserialize([]byte) parsing ChannelMax/FrameMax/Heartbeat.
- `src/amqp-go/protocol/structures.go` — Add MaxFrameSize uint32, MaxChannels uint16, NegotiatedHeartbeat uint16, and an atomic ChannelCount to Connection; initialize defaults in NewConnection.
- `src/amqp-go/server/server.go` — After reading tune-ok, deserialize and store negotiated limits on conn; use ReadFrameOptimizedLimited(conn.Conn, conn.MaxFrameSize) in readFrames and handshake reads; add MaxConnections guard in handleConnection under s.Mutex.
- `src/amqp-go/server/connection_handlers.go` — In sendConnectionTune advertise configured FrameMax/ChannelMax/Heartbeat from s.Config instead of hardcoded constants.
- `src/amqp-go/server/channel_handlers.go` — In ChannelOpen case, enforce conn.MaxChannels via atomic ChannelCount; reject with 506 CHANNEL_ERROR when exceeded; decrement on channel close and connection teardown.

**Code sketch:**

```go
// frame_optimized.go
func ReadFrameOptimizedLimited(reader io.Reader, maxSize uint32) (*Frame, error) {
    headerPtr := getFrameHeader(); header := *headerPtr; defer putFrameHeader(headerPtr)
    if _, err := io.ReadFull(reader, header); err != nil { return nil, err }
    size := binary.BigEndian.Uint32(header[3:7])
    if maxSize > 0 && size > maxSize {
        return nil, fmt.Errorf("frame size %d exceeds negotiated max %d", size, maxSize)
    }
    payload := make([]byte, size+1) // safe now
    // ... unchanged
}
func ReadFrameOptimized(r io.Reader) (*Frame, error) { return ReadFrameOptimizedLimited(r, 0) }

// methods.go
func (m *ConnectionTuneOKMethod) Deserialize(data []byte) error {
    if len(data) < 8 { return fmt.Errorf("tune-ok too short") }
    m.ChannelMax = binary.BigEndian.Uint16(data[0:2])
    m.FrameMax = binary.BigEndian.Uint32(data[2:6])
    m.Heartbeat = binary.BigEndian.Uint16(data[6:8])
    return nil
}

// server.go handleConnection (under s.Mutex, before insert)
if s.Config != nil && s.Config.Network.MaxConnections > 0 && len(s.Connections) >= s.Config.Network.MaxConnections {
    s.Mutex.Unlock(); conn.Close(); return
}
```


**Test (TDD):** TDD, focused tests: (1) protocol TestReadFrameOptimizedLimited_RejectsOversized — craft a 7-byte header advertising size=100_000 with maxSize=1024; assert error is returned WITHOUT allocating/reading the body (use a reader that would block/EOF on the body so a pass proves the check happens pre-allocation). (2) protocol TestConnectionTuneOKDeserialize — Serialize then Deserialize round-trip, assert ChannelMax/FrameMax/Heartbeat match. (3) server TestMaxConnectionsEnforced — configure MaxConnections=1, complete one handshake, assert a second accepted connection is closed/rejected. (4) server TestMaxChannelsPerConnectionEnforced — configure MaxChannelsPerConnection=2; open 2 channels OK; assert 3rd channel.open triggers 506 and no extra channel is stored; close one channel then assert a new open succeeds (proves decrement). (5) server integration TestOversizedInboundFrameRejected — after handshake, send a frame header advertising a size larger than negotiated FrameMax and assert the server closes the connection with a frame error rather than allocating.

**Regression risk:** Low-to-moderate. The FrameMax check must use the NEGOTIATED value (min of server and client, with 0 meaning unlimited on the side that sent it); use s.Config.Server.MaxFrameSize as the ceiling. Setting the cap too low would break large content-body frames — but bodies are already split by maxBodyPerFrame on send, so a 128KB inbound cap is compatible. The MaxConnections check adds a lock-guarded len() read on the accept path — negligible. Channel-count decrement must be wired into ALL channel-close paths (channel.close and connection teardown) or clients hit false limits over time; cover with the decrement assertion in test 4. Existing tests call ReadFrameOptimized directly; keeping the unbounded wrapper preserves them.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged whether the frame-size DoS is the only or worst memory vector. It is NOT. basic_handlers.go:178-179 does make([]byte, 0, contentHeader.BodySize) where BodySize is a client-controlled uint64 from the content-header frame with zero bound against MaxMessageSize (16MB config, unenforced). This is a simpler, worse DoS: it needs only ONE small legal content-header frame post-handshake — the header frame itself is tiny and passes any FrameMax cap — yet advertises up to ~18EB, forcing an immediate multi-GB/OOM allocation. The colleague's fix#1 (FrameMax) does NOT close this. The fix MUST add a BodySize <= MaxMessageSize check in processHeaderFrame; without it the highest-impact allocation path stays open.
- Challenged the FrameMax type/threading: Server.MaxFrameSize is declared int (interfaces/config.go:126) while wire FrameMax is uint32 and ReadFrameOptimizedLimited takes uint32. Converting a large or negative int cap to uint32 can silently wrap; the fix must clamp/validate (config already requires >0, but the min() with client value and the int->uint32 cast still need an explicit guard).
- Challenged regression safety of the decrement path for ChannelCount. Verified there are TWO channel-teardown sites that must both decrement: channel_handlers.go:120 (channel.close -> conn.Channels.Delete) and server.go:177-205 (connection cleanup Range over Channels). The colleague called this out (regressionRisk note + test 4), and it holds — but both sites must be wired or clients hit false CHANNEL_ERROR limits over time.
- Challenged whether keeping unbounded wrappers breaks existing tests. Verified frame_test.go, fuzz_test.go, protocol_bench_optimized_test.go, channel_flow_test.go, basic_get_purge_test.go call ReadFrame/ReadFrameOptimized directly — keeping a maxSize=0 wrapper preserves all of them, so this part is regression-safe as claimed.
- Challenged severity: considered downgrading to medium since no auth bypass/data loss. Rejected the downgrade — this is a trivially-triggerable unauthenticated-or-single-connection resource-exhaustion / OOM DoS against a broker whose whole job is availability, with two independent unbounded-allocation paths; HIGH is correct, not critical only because it requires reaching the (cheap) handshake and does not corrupt data.

**Fix concerns:**
- INCOMPLETE: The fix omits the content-body allocation vector. basic_handlers.go:178-179 pre-allocates make([]byte, 0, contentHeader.BodySize) from a client-controlled uint64 BodySize with no MaxMessageSize check. This is arguably the worst memory-DoS of the set (one small legal frame -> multi-GB alloc) and is NOT covered by the FrameMax change. Must add: if s.Config != nil && contentHeader.BodySize > uint64(s.Config.Server.MaxMessageSize) { send 406 PRECONDITION_FAILED / close channel/connection and return before allocating }.
- Type-safety: Server.MaxFrameSize is int; threading it into a uint32 maxSize and doing min() with client FrameMax needs an explicit clamp so a huge/negative configured int cannot wrap to a tiny or huge uint32. Also decide the semantics: 0 on the wire means 'no limit' per AMQP, so the negotiated cap should be min of the two non-zero values, falling back to server config as the hard ceiling — the sketch's min-with-0-means-unlimited is right but must still enforce the server config ceiling even if the client sends 0.
- The optional heartbeat/read-deadline follow-up should not stay optional if dead-peer detection is a stated goal: without conn.Conn.SetReadDeadline in readFrames, a silent peer that opened channels/holds buffers is never reaped, so the resource-exhaustion story is only partially closed by the count/size caps. Recommend promoting the SetReadDeadline(2x negotiated heartbeat) piece into the main fix.
- Enforcement response codes: channel.open over-limit should be a connection-level error per AMQP (the colleague says 506 CHANNEL_ERROR via connection.close, which is correct as a hard error). Ensure the over-limit path does NOT Store the channel and does NOT leak the ChannelCount increment (increment only after the limit check passes).

**Agreed resolution (authoritative):**

Real bug, severity HIGH confirmed. All four sub-claims verified against source: (1) frame_optimized.go:40-42 allocates make([]byte, size+1) from an unbounded client-controlled uint32 frame size, and ReadFrameOptimized is the reader on every path (server.go:233,267,279,356) — real inbound memory-DoS; (2) tune-ok is discarded — server.go:266-276 checks only frame.Type, connection_handlers.go:320-323 returns nil, and methods.go:333-360 has Serialize but no Deserialize for ConnectionTuneOKMethod; (3) channel.open at channel_handlers.go:13-28 stores unconditionally with no count field on Connection/Channel; (4) acceptLoop/handleConnection (server.go:116-164) insert into s.Connections under s.Mutex but never consult MaxConnections. Config values (config.go:23,66-68) are validated but dead at runtime — the only MaxFrameSize use is outbound splitting (frame_helpers.go:232). Heartbeat is hardcoded (server.go:508-509) with no SetReadDeadline anywhere, so dead peers never time out. The proposed fix's four wired changes (FrameMax check pre-allocation, tune-ok Deserialize, ChannelCount enforcement with decrement on all close paths, MaxConnections guard under s.Mutex) are all correct and regression-safe, and the plan to keep unbounded ReadFrameOptimized/ReadFrame wrappers preserves existing tests (frame_test.go, fuzz_test.go, channel_flow_test.go, benches). The fix is SOUND but INCOMPLETE — it needs one revision: it must also bound contentHeader.BodySize against Server.MaxMessageSize in basic_handlers.go before make([]byte, 0, BodySize) at line 178-179. Note the int-vs-uint32 conversion for Server.MaxFrameSize (int) must guard against overflow when threading to the uint32 check.

---

## [I22] Transaction commit is not atomic and not rollback-safe: mid-commit failure re-applies already-executed operations on retry (duplicate publishes/acks); ExecuteAtomic serializes all transactions broker-wide

**Rank 8 · P1** · Severity: 🟠 High · Category: correctness · Effort: medium

### Impact
A tx.commit that partially applies then fails leaves the entire pending-operation slice intact and the channel still transactional. AMQP clients that retry tx.commit (a legitimate pattern after a channel/connection error) cause the broker to re-execute operations that already succeeded on the first attempt: duplicate message publishes into exchanges/queues and duplicate acks/nacks/rejects against deliveries. Because operations run directly against live storage via UnifiedBrokerExecutor.ExecutePublish -> broker.PublishMessage with no staging area and no undo/rollback, there is no atomicity: a transaction is observed half-applied by consumers between the point of first failure and any retry. Separately, DisruptorStorage.ExecuteAtomic takes a single global ds.txMutex (write lock) that is shared with BeginTransaction/AddAction/CommitTransaction/etc., so every atomic transaction commit is serialized broker-wide, throttling all concurrent transactional channels through one mutex regardless of which queues/exchanges they touch. This violates the AMQP expectation that tx.commit is an all-or-nothing operation.

### Root cause
DefaultTransactionManager.Commit executes operations against live storage/broker with no journaling, no compensating (undo) log, and no true transactional isolation. executeSequentially applies each operation immediately and irreversibly; when an operation N fails it returns an error, and Commit's early return skips the tx.operations = tx.operations[:0] cleanup, leaving all operations 1..M (including those already applied to live storage) in the pending slice. The atomicStorage path (executeAtomically -> DisruptorStorage.ExecuteAtomic) provides no real atomicity either: it merely wraps executeSequentially in a global mutex and passes the same live ds as txnStorage, so a failure mid-loop cannot be rolled back. The coarse ds.txMutex.Lock() in ExecuteAtomic additionally serializes all commits.

### Proof

- **`src/amqp-go/transaction/manager.go` : 159-178** — On any commit error, Commit returns at line 168 before line 172 clears the slice. Operations that already executed successfully remain pending, so a subsequent tx.commit retry re-runs them. There is no undo of the already-applied ops.

```go
if tm.atomicStorage != nil {
		err = tm.executeAtomically(tx.operations)
	} else {
		err = tm.executeSequentially(tx.operations)
	}

	if err != nil {
		return fmt.Errorf("failed to commit transaction for channel %d: %w", channelID, err)
	}

	// Clear operations and reset state to active (still in transactional mode)
	tx.operations = tx.operations[:0]
```

- **`src/amqp-go/transaction/manager.go` : 255-288** — Operations are applied one at a time directly to the live executor. On failure at op N, ops 1..N-1 have already been committed to live storage with no compensation; the function returns and the whole slice is preserved by the caller.

```go
func (tm *DefaultTransactionManager) executeSequentially(operations []*interfaces.TransactionOperation) error {
	for _, op := range operations {
		switch op.Type {
		case interfaces.OpPublish:
			err := tm.executor.ExecutePublish(op.Exchange, op.RoutingKey, op.Message)
			if err != nil && !errors.Is(err, broker.ErrNoRoute) {
				return fmt.Errorf("failed to execute publish: %w", err)
			}
```

- **`src/amqp-go/transaction/broker_executor.go` : 33-51** — Confirms executeSequentially applies operations to the real broker (PublishMessage / AcknowledgeMessage), i.e. live, externally-visible side effects — so re-application on retry is a genuine duplicate publish/ack, not a no-op.

```go
func (ube *UnifiedBrokerExecutor) ExecutePublish(exchange, routingKey string, message *protocol.Message) error {
	...
	return ube.broker.PublishMessage(exchange, routingKey, message)
}
```

- **`src/amqp-go/storage/disruptor_storage.go` : 896-900** — The 'atomic' storage path just grabs a global write lock and runs the callback against the live ds. There is no snapshot, no staging, no rollback — a failure inside operations() cannot be undone. The single ds.txMutex is also shared with BeginTransaction/AddAction/CommitTransaction/RollbackTransaction (lines 635-720), so all atomic commits are serialized broker-wide.

```go
func (ds *DisruptorStorage) ExecuteAtomic(operations func(txnStorage interfaces.Storage) error) error {
	ds.txMutex.Lock()
	defer ds.txMutex.Unlock()
	return operations(ds)
}
```

- **`src/amqp-go/transaction/manager_test.go` : 308-316** — The existing test codifies the buggy contract: after a failed commit the operations remain pending. Combined with handleTxCommit returning the error to the client and leaving the channel transactional, a retry re-applies them. Test currently passes (verified via go test).

```go
// Commit should fail
	err = tm.Commit(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to commit transaction")

	// Operations should still be pending
	operations, err := tm.GetPendingOperations(channelID)
	assert.NoError(t, err)
	assert.Len(t, operations, 1)
```

- **`src/amqp-go/server/tx_handlers.go` : 106-117** — The server surfaces the commit error to the caller but leaves the transaction's pending operations intact and the channel in transactional mode, so the same channel can issue tx.commit again and re-execute the retained (already-applied) operations.

```go
err = s.TransactionManager.Commit(channelID)
		if err != nil {
			s.Log.Error("Failed to commit transaction", zap.Error(err), zap.Uint16("channel_id", channelID))
			return err
		}
```


### Fix

Make tx.commit all-or-nothing with respect to the pending-operation slice, and make partial-failure recovery safe. Because the broker applies real side effects with no undo, the minimum correct fix is: (1) On commit failure, do NOT retain the operations that already executed. Track how many operations succeeded and either (a) clear the whole slice and treat commit as terminated (RabbitMQ closes the channel on tx.commit failure — closing the channel is the spec-aligned, simplest correct behavior), or (b) drop the successfully-applied prefix and only retain the not-yet-attempted suffix so a retry cannot double-apply. Option (a) is strongly preferred: on commit error, send a channel-level exception (close the channel) and discard all pending operations, matching AMQP semantics and eliminating the duplicate-on-retry vector entirely. (2) Update DefaultTransactionManager.Commit to clear tx.operations regardless of success/failure (i.e. move the tx.operations = tx.operations[:0] before the error check, or into a defer), and reset/close state so retry is impossible. (3) Update handleTxCommit to send a channel.close (or connection.close) on commit failure instead of just returning the error while leaving the channel usable. (4) Fix/replace the misleading TestTransactionManagerCommitFailure to assert the NEW contract (operations discarded on failure) — this is the TDD anchor. (5) For the concurrency concern: keep ExecuteAtomic's mutex for now (it is not incorrect, only coarse), but document it and, if a follow-up is desired, shard the transaction bookkeeping so ExecuteAtomic does not contend on the same ds.txMutex used by the per-txID map operations — e.g. introduce a dedicated atomicMutex separate from txMutex, or better, remove the fake 'atomic' wrapper entirely since it provides no real atomicity and only serializes commits. Sequence: land the atomicity/duplicate fix first (option a), then optionally the mutex-decoupling as a separate perf change.

**Files to change:**
- `src/amqp-go/transaction/manager.go` — In Commit(), unconditionally clear tx.operations (before/independent of the error check) and set tx.state to TransactionStateNone on failure so retries cannot re-apply already-executed operations. Keep TransactionStateActive only on success.
- `src/amqp-go/server/tx_handlers.go` — In handleTxCommit(), on Commit error send a channel.close (channel-level exception, e.g. InternalError/PreconditionFailed) rather than just returning the error, matching AMQP tx.commit-failure semantics and preventing a retry on the same channel.
- `src/amqp-go/transaction/manager_test.go` — Rewrite TestTransactionManagerCommitFailure to assert the corrected contract: after a failed Commit, GetPendingOperations returns 0 operations (or the channel is no longer transactional), proving no operation can be re-applied on retry.
- `src/amqp-go/storage/disruptor_storage.go` — Optional follow-up: separate ExecuteAtomic's serialization from the per-txID bookkeeping mutex (introduce a dedicated mutex or remove the no-op atomic wrapper) so atomic commits do not globally contend with BeginTransaction/AddAction. Document that ExecuteAtomic provides serialization only, not rollback.

**Code sketch:**

```go
// manager.go Commit(): make cleanup unconditional and terminate the tx on failure
var err error
if tm.atomicStorage != nil {
    err = tm.executeAtomically(tx.operations)
} else {
    err = tm.executeSequentially(tx.operations)
}
// Always discard pending ops so a retry can never re-apply already-executed side effects.
tx.operations = tx.operations[:0]
if err != nil {
    // Terminate the transaction; caller (handleTxCommit) must close the channel.
    tx.state = interfaces.TransactionStateNone
    return fmt.Errorf("failed to commit transaction for channel %d: %w", channelID, err)
}
tx.state = interfaces.TransactionStateActive
atomic.AddInt64(&tm.stats.totalCommits, 1)
return nil

// tx_handlers.go handleTxCommit(): on commit error, raise a channel exception instead of leaving channel usable
if err = s.TransactionManager.Commit(channelID); err != nil {
    s.Log.Error("Failed to commit transaction", zap.Error(err), zap.Uint16("channel_id", channelID))
    return s.sendChannelClose(conn, channelID, protocol.InternalError, "transaction commit failed")
}
```


**Test (TDD):** Add/replace a TDD test in transaction/manager_test.go: TestTransactionManagerCommitFailureDoesNotDuplicateOnRetry — Select channel, add 3 publish operations, configure MockExecutor to succeed on op1 and fail on op2 (e.g. failOn a specific exchange/routing key or a counter). Call Commit -> expect error. Assert executor.publishes has exactly 1 entry (op1 applied once). Call Commit AGAIN (simulating a client retry) and assert executor.publishes STILL has 1 entry (no re-application) and GetPendingOperations returns 0. Also add a server-level test in server/transaction_integration_test.go asserting that handleTxCommit on a failing commit closes the channel (channel.close frame) instead of leaving it transactional.

**Regression risk:** Moderate. Changing commit-failure semantics alters an established (though buggy) contract that TestTransactionManagerCommitFailure currently locks in, so that test MUST be updated in the same change. Closing the channel on commit failure changes client-visible behavior, but it aligns with RabbitMQ/AMQP 0.9.1 (tx.commit failures are channel exceptions), so real clients tolerate it. Confirm-mode interaction in handleTxCommit (lines 119-132) must be re-checked: ensure confirms are only sent for successfully-applied publishes, not for a failed/aborted commit. The optional mutex-decoupling in disruptor_storage.go carries independent concurrency risk and should be a separate, benchmarked change, not bundled with the correctness fix.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the claimed exploit mechanism 'client retries tx.commit on the same channel': FALSE for this codebase. On commit failure handleTxCommit returns the error, and processFrames (server.go:404-410) sets conn.Closed.Store(true) and returns, tearing down the whole connection (server.go:336 conn.Conn.Close()). The channel/connection is dead, so same-channel retry cannot happen. The colleague's central duplicate-on-retry narrative rests on a path that isn't reachable as described.
- Found a MORE severe, actually-reachable vector the claim missed: transaction state is a global map keyed by channelID only (manager.go:17) and is NOT cleaned up on connection teardown — TransactionManager.Close is called only from the graceful channel.close handler (channel_handlers.go:124), while the connection-cleanup path (server.go:195-218) never calls it. A failed commit that kills the connection leaks the full pending-op slice keyed by channelID, reachable/flushable by a later connection using the same channelID. This upgrades, not downgrades, the correctness concern.
- Verified the proof that mid-commit failure is realistic: PublishMessage (storage_broker.go:812-872) can fail on exchange-not-found, queue closed during backpressure (839-841), or StoreMessage error (859-862), and in a fanout it stores to queue i=0 before a later queue fails — a genuine half-applied commit with externally visible side effects.
- Refined the concurrency sub-claim: the assertion that ExecuteAtomic contends with BeginTransaction/AddAction/CommitTransaction is technically true (shared ds.txMutex) but those txID-based methods are NOT on the live channel-commit path (separate, effectively unused API). The real, valid contention is between concurrent ExecuteAtomic (commit) calls serializing broker-wide. Perf point stands, mechanism slightly restated.
- Confirmed the atomic path is the production default (DisruptorStorage implements AtomicStorage; builder.go:241-242 selects NewTransactionManagerWithStorage), so the no-real-atomicity ExecuteAtomic wrapper is what actually runs — not a dead branch.
- Found the fix code sketch does not compile: s.sendChannelClose signature is (conn, channelID, replyCode, replyText, classID, methodID uint16) returning void (authz.go:41); the sketch's 4-arg call, 'return s.sendChannelClose(...)', and 'protocol.InternalError' (InternalError is in the amqperrors package) are all wrong. Also, unless the handler returns nil after sending channel.close, processFrames will still kill the connection.

**Fix concerns:**
- Incomplete: does not fix the actually-reachable vector. Even with unconditional slice clearing in Commit, transaction state still leaks on abrupt connection teardown because server.go connection cleanup (195-218) never calls TransactionManager.Close. The fix MUST add a TransactionManager.Close(channelID) sweep for all of the connection's channels on teardown, otherwise leaked/mis-attributed state persists across connections keyed by channelID.
- Code sketch will not compile: sendChannelClose takes 6 args (conn, channelID, replyCode, replyText, classID, methodID) and returns void; the sketch uses 4 args, returns its result, and references protocol.InternalError (the constant lives in the amqperrors package). Must be rewritten to the real signature and must NOT propagate an error afterward or processFrames still tears down the connection.
- Behavior-change nuance: today a commit failure kills the ENTIRE connection (processFrames error path). Switching to a channel-level exception (channel.close) is more correct but is a client-visible behavior change beyond what the summary implies — it changes connection-kill to channel-kill. Worth an explicit test asserting the connection survives and only the channel closes.
- The claim's severity/impact prose should be corrected to reflect that same-channel tx.commit retry is not reachable; the duplicate/mis-attribution risk comes from the cross-connection channelID-keyed leak, not from a retried commit on a live channel.
- Confirm-mode interaction (tx_handlers.go:119-132) is handled acceptably today (ops captured before commit, acks only sent on success), but the revised fix must preserve that ordering and must not send confirms for an aborted commit — call this out in the test.
- Mutex-decoupling is correctly deferred, but note the current perf hit is real and default (every production commit serializes on ds.txMutex); if a follow-up removes the fake atomic wrapper, ensure it does not silently reintroduce the non-atomic behavior without a documented decision.

**Agreed resolution (authoritative):**

Real bug, severity HIGH, confirmed by source. tx.commit is neither atomic nor rollback-safe: executeSequentially (manager.go:255-288) applies each op directly to live storage via ExecutePublish -> broker.PublishMessage (storage_broker.go:812, which can partial-store across queues in a fanout then fail at 859-862), and executeAtomically (manager.go:248-252) only wraps this in ds.txMutex.Lock (disruptor_storage.go:896-900) with no snapshot/undo — this atomic path is the DEFAULT in production (DisruptorStorage implements AtomicStorage, builder.go:241-242). On failure, manager.go:172-173 cleanup is skipped so all ops stay pending (locked in by the passing TestTransactionManagerCommitFailure). ExecuteAtomic serializes every commit broker-wide through one global mutex (perf concern, not correctness). HOWEVER the colleague's stated exploit mechanism — 'client retries tx.commit on the SAME channel' — is INACCURATE for this codebase: when handleTxCommit returns the error, processFrames (server.go:404-410) sets conn.Closed and tears down the ENTIRE connection, so same-channel retry is impossible. The actually-reachable and arguably worse vector is a cross-connection state LEAK: DefaultTransactionManager.channels is a global map keyed by channelID ONLY (manager.go:17), and TransactionManager.Close is invoked only on graceful channel.close (channel_handlers.go:124), NOT on connection teardown (server.go:195-218 has no such call). So a failed/abruptly-closed transaction leaks its full operation slice keyed by channelID, which can block a future connection's tx.select (manager.go:69-73) or, if the leaked channel stays transactional, cause a later commit to flush another connection's operations (mis-attributed duplicate publishes). Agreed fix direction (option a): make cleanup unconditional (move tx.operations[:0] before the error check), set state to TransactionStateNone on failure, and raise a channel-level exception rather than silently killing the connection. But the fix as written is INCOMPLETE and must also (1) call TransactionManager.Close on connection teardown in server.go to close the leak (the real reachable vector), and (2) fix the non-compiling handleTxCommit sketch (sendChannelClose has signature (conn, channelID, replyCode, replyText, classID, methodID) and returns void — not 4 args, and InternalError lives in the amqperrors package not protocol; must not return an error afterward or the connection still dies). Keep the confirm-mode ordering (tx_handlers.go:119-132 captures ops before commit and only acks on success — correct). Mutex-decoupling is correctly scoped as a separate perf change. Update TestTransactionManagerCommitFailure to the new contract.

---

## [I19] auto-delete and exclusive queue/exchange lifecycle not enforced (core AMQP 0.9.1)

**Rank 9 · P1** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: large

### Impact
Clients that rely on core AMQP 0.9.1 lifecycle semantics silently leak entities and get broken exclusivity. (1) auto-delete queues/exchanges are never deleted when the last consumer leaves, when the last binding is removed, or when the owning connection closes, so the classic RPC-reply-queue pattern (exclusive + auto-delete amq.gen.* reply queue) accumulates forever along with its storage/WAL state, causing unbounded resource growth over the broker's lifetime. (2) exclusive queues have no owner-connection tracking: a second connection can declare/consume/delete another connection's exclusive queue with no RESOURCE_LOCKED (405) error, and the exclusive queue is not removed when its declaring connection dies. (3) the basic.consume exclusive flag is accepted and stored but never enforced, so a second consumer can attach to a queue a client asked to consume exclusively. All three violate the 0.9.1 spec and diverge from RabbitMQ behavior clients depend on.

### Root cause
The AutoDelete and Exclusive booleans are persisted on protocol.Exchange / protocol.Queue and validated on redeclare, but no code path ever acts on them. There is no owner-connection reference on the Queue or Connection structs, so exclusivity cannot be enforced or cleaned up. UnregisterConsumer, UnbindQueue/UnbindExchange, and the connection-close cleanup all lack the "if this was the last consumer/binding and the entity is auto-delete, delete it" and "delete exclusive entities owned by this connection" logic. RegisterConsumer never enforces the exclusive-consumer rule either.

### Proof

- **`src/amqp-go/broker/storage_broker.go` : 676-794** — UnregisterConsumer does exhaustive inflight-message cleanup (steps 1-10) but never inspects queue.AutoDelete. Step 9 (lines 768-785) removes the consumer from queueConsumers and even deletes the queueConsumers entry when the list becomes empty (len(newConsumers)==0), i.e. it knows when the last consumer just left, yet never triggers auto-deletion of the queue. There is no call to DeleteQueue or any auto-delete branch anywhere in the function.
- **`src/amqp-go/broker/storage_broker.go` : 354-401** — DeclareQueue only uses the exclusive flag for redeclare property validation (line 364: existing.Exclusive != exclusive). It never records an owning connection, never rejects a redeclare from a different connection, and the created protocol.Queue carries no owner reference. Exclusivity is therefore purely cosmetic.
- **`src/amqp-go/broker/storage_broker.go` : 518-521** — UnbindQueue just calls storage.DeleteBinding and returns. It never checks whether the source exchange is AutoDelete and now has zero bindings (the last-binding-gone auto-delete rule for exchanges). UnbindExchange (567-569) has the same gap.
- **`src/amqp-go/broker/storage_broker.go` : 329-352** — DeleteExchange / DeleteQueue (403+) contain no auto-delete cascade at all; there is no code path that deletes an entity because it became unused, only explicit client-requested delete.
- **`src/amqp-go/protocol/structures.go` : 124-145** — The Queue struct has Durable/AutoDelete/Exclusive booleans but NO owner-connection field (only a Channel back-reference that is runtime state, unused for exclusivity). NewQueue never records who declared it, so exclusive ownership cannot be checked or cleaned up.
- **`src/amqp-go/protocol/structures.go` : 14-30** — The Connection struct tracks Channels but has no list of exclusive/auto-delete queues or exchanges it owns. There is no way at connection-close time to know which entities must be torn down.
- **`src/amqp-go/server/server.go` : 176-208** — The only connection-close cleanup iterates channels and calls Broker.UnregisterConsumer for each consumer tag, then RequeueAllGetDeliveries. There is no deletion of exclusive queues owned by the connection and no auto-delete of queues/exchanges, so exclusive+auto-delete reply queues survive the connection that created them.
- **`src/amqp-go/server/basic_handlers.go` : 435-455** — basic.consume builds a Consumer with Exclusive: consumeMethod.Exclusive (line 440) and calls RegisterConsumer, but RegisterConsumer (storage_broker.go:602-667) never checks that flag against existing consumers on the queue. A second consumer on an exclusively-consumed queue is accepted with no error.
- **`src/amqp-go/integration_test.go` : 66-80** — Existing tests pass auto-delete=true 'to clean up' but never assert the entity is actually deleted afterward, so the missing behavior went unnoticed. This confirms the flag is treated as a no-op.

### Fix

Implement the three core 0.9.1 lifecycle rules. (A) Owner tracking for exclusive entities: add an OwnerConnID string field to protocol.Queue and record the declaring connection ID in DeclareQueue. Add per-connection ownership tracking on protocol.Connection (e.g. a sync.Map exclusiveQueues / autoDeleteEntities). Because the broker interface currently only passes primitive args, extend DeclareQueue to accept ownerConnID (or add a new broker method RecordExclusiveOwner) so the broker can store it. On redeclare/consume/delete/bind of an exclusive queue from a different connection ID, return an AMQP RESOURCE_LOCKED (405) channel error. (B) Auto-delete on last consumer: in StorageBroker.UnregisterConsumer, after step 9 detects the queueConsumers list became empty, load the queue and, if queue.AutoDelete is true and the queue has had at least one consumer, call b.DeleteQueue(queueName, false, false). Only auto-delete queues that actually had a consumer (avoid deleting a freshly declared queue that never had one). (C) Auto-delete on last binding for exchanges: in UnbindQueue and UnbindExchange, after DeleteBinding succeeds, check whether the source exchange is AutoDelete and now has zero remaining bindings (storage.GetExchangeBindings + GetExchangeBindingsFrom); if so, call DeleteExchange(source, false). Exclude the built-in default exchanges (they are AutoDelete:false so naturally skipped). (D) Connection-close cleanup: in server.go connection teardown (after unregistering consumers, ~lines 176-208), iterate the connection's tracked exclusive queues and auto-delete entities and call Broker.DeleteQueue / DeleteExchange for each. (E) Enforce basic.consume exclusive: in RegisterConsumer, if consumer.Exclusive is true and the queue already has any consumer, or an existing consumer on the queue is exclusive, reject with an error the handler maps to ACCESS_REFUSED (403) / RESOURCE_LOCKED (405). Sequence the work: land owner-tracking plumbing first, then the three deletion rules, then exclusive-consumer enforcement, each behind its own test. Exempt the recovery_manager.go startup redeclare path from owner enforcement (it re-declares with no live connection).

**Files to change:**
- `src/amqp-go/protocol/structures.go` — Add OwnerConnID string to Queue (set via NewQueue param or a setter). Add connection-level ownership tracking to Connection (e.g. exclusiveEntities sync.Map). Do NOT persist OwnerConnID in the storage codec (exclusive/auto-delete entities are runtime-scoped).
- `src/amqp-go/broker/storage_broker.go` — DeclareQueue: record owner connection for exclusive queues and reject cross-connection redeclare with a RESOURCE_LOCKED-mappable error. UnregisterConsumer: after the last consumer leaves, auto-delete the queue if AutoDelete && it had >=1 consumer. UnbindQueue/UnbindExchange: auto-delete the source exchange if AutoDelete && zero bindings remain. RegisterConsumer: enforce the exclusive-consumer rule.
- `src/amqp-go/server/server.go` — In the connection-close cleanup block (~lines 176-208), after unregistering consumers, delete exclusive queues owned by this connection and auto-delete entities the connection created, via the broker.
- `src/amqp-go/server/queue_handlers.go` — Thread the connection ID into DeclareQueue (or a follow-up RecordExclusiveOwner call) and map broker exclusivity errors to a 405 RESOURCE_LOCKED channel error using the existing authzChannelError helper pattern.
- `src/amqp-go/server/basic_handlers.go` — Map the exclusive-consumer rejection returned by RegisterConsumer to an ACCESS_REFUSED (403) channel error instead of returning a bare error.
- `src/amqp-go/interfaces/broker.go` — If DeclareQueue's signature is extended to carry ownerConnID, update the Broker interface and all adapters (server/broker_adapters.go, server/builder.go) accordingly; otherwise add a small RecordExclusiveOwner / release API.

**Code sketch:**

```go
// storage_broker.go, at end of UnregisterConsumer step 9, when list becomes empty:
if val, ok := b.queueConsumers.Load(state.queueName); !ok || len(val.([]*ConsumerState)) == 0 {
    if q, err := b.storage.GetQueue(state.queueName); err == nil && q.AutoDelete {
        _ = b.DeleteQueue(state.queueName, false, false)
    }
}

// UnbindQueue after DeleteBinding:
if ex, err := b.storage.GetExchange(exchangeName); err == nil && ex.AutoDelete {
    if bs, _ := b.storage.GetExchangeBindings(exchangeName); len(bs) == 0 {
        _ = b.DeleteExchange(exchangeName, false)
    }
}

// RegisterConsumer exclusivity check (before storing state):
if existing, _ := b.queueConsumers.Load(queueName); existing != nil {
    cs := existing.([]*ConsumerState)
    if len(cs) > 0 && (consumer.Exclusive || cs[0].consumer.Exclusive) {
        return errExclusiveConsumeInUse // handler -> 403 ACCESS_REFUSED
    }
}
```


**Test (TDD):** Add TDD-style tests, each failing against current code and passing after the fix: (1) broker: TestAutoDeleteQueueDeletedWhenLastConsumerLeaves - declare an auto-delete queue, register then unregister the only consumer, assert GetQueue returns ErrQueueNotFound. (2) TestAutoDeleteQueueNotDeletedWithoutConsumers - declare auto-delete queue with no consumer ever attached, assert it still exists. (3) TestAutoDeleteExchangeDeletedWhenLastBindingRemoved - declare auto-delete exchange, bind a queue, unbind it, assert GetExchange returns ErrExchangeNotFound. (4) TestExclusiveQueueRejectsSecondConnection - declare exclusive queue on connection A, attempt declare/consume/delete from connection B, assert 405 RESOURCE_LOCKED. (5) server integration: TestExclusiveAutoDeleteReplyQueueRemovedOnConnClose - open connection, declare exclusive+auto-delete queue, close connection, assert the queue is gone from a second connection. (6) TestExclusiveConsumerRejectsSecondConsumer - basic.consume exclusive on a queue, a second basic.consume returns ACCESS_REFUSED.

**Regression risk:** Medium-to-high. Auto-deletion touches the delicate UnregisterConsumer inflight/requeue ordering (steps 1-10 with LoadAndDelete guards); DeleteQueue must run only after all inflight is requeued (after step 9) to avoid racing in-flight ACKs. Auto-delete on last binding must never hit the built-in default exchanges (they are AutoDelete:false, so naturally excluded). Owner-tracking on redeclare risks false-positive RESOURCE_LOCKED during legitimate reconnection or the recovery_manager.go startup redeclare (which has no live connection); those paths must be exempt or use a sentinel owner. Adding fields to the hot Queue/Connection structs must not disturb the object-pooling and lock-free invariants from recent perf commits.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Tried to upgrade to high on the basis of unbounded lifetime resource leak (storage + WAL growth for every exclusive+auto-delete reply queue), but held at medium: the growth is slow and pattern-specific, and there is no acute correctness/availability failure, data corruption, or crash. Colleague's medium rating survives.
- Tried to reject the exclusive-consumer error code as wrong, but verified the colleague is correct: exclusive queue accessed by a second connection = 405 RESOURCE_LOCKED, second consumer on an exclusively-consumed queue = 403 ACCESS_REFUSED. Both cited correctly.
- Found a concrete flaw the colleague did NOT flag: the UnregisterConsumer code sketch calls b.DeleteQueue(...) while step 9's defer mu.Unlock() (storage_broker.go:771) still holds the per-queue mutex, and DeleteQueue (line 437) re-locks that SAME non-reentrant sync.Mutex (getQueueConsumersMutex, line 131-134 -> plain &sync.Mutex{}). Taken literally the sketch self-deadlocks. This forces fixVerdict to needs-revision: the deletion must run after releasing the mutex, or DeleteQueue needs a lock-free internal variant.
- Questioned the 'had >=1 consumer' guard as redundant, and confirmed it: UnregisterConsumer is only reachable when a consumer existed, so the guard is trivially satisfied there; its real purpose is ensuring freshly-declared consumerless auto-delete queues are not deleted, which UnregisterConsumer never touches anyway. Guard is harmless and correct.
- Verified owner-tracking plumbing is feasible: conn.ID is available at handleQueueDeclare/handleBasicConsume (queue_handlers.go, basic_handlers.go), so threading ownerConnID through DeclareQueue (or a RecordExclusiveOwner call) across interfaces/broker.go, unified_broker.go, builder.go, broker_adapters.go is a real but bounded change. Recovery-manager redeclare path (recovery_manager.go:166-172) must be exempted since it has no live connection.

**Fix concerns:**
- UnregisterConsumer auto-delete code sketch self-deadlocks: it calls b.DeleteQueue inside the block holding the per-queue mutex via defer mu.Unlock() (line 771), and DeleteQueue locks the same non-reentrant sync.Mutex (line 437). Must release the mutex before deleting, or add a lock-free internal delete that does not re-acquire getQueueConsumersMutex.
- Regression risk to the delicate UnregisterConsumer step 1-10 inflight/requeue ordering: DeleteQueue must run strictly AFTER all inflight has been requeued (past step 9) or it races in-flight ACKs and the LoadAndDelete guards; the fix should trigger deletion only at the very end of UnregisterConsumer, and DeleteQueue's own QueueState.Close()/PurgeQueue must not fight the requeue that just happened.
- Owner-tracking on redeclare risks false-positive RESOURCE_LOCKED on legitimate reconnection and on the recovery_manager.go durable-queue redeclare (no live connection); those paths must use a sentinel/exempt owner. Note durable+exclusive is itself an odd combination but the recovery path passes queue.Exclusive through.
- Adding OwnerConnID to Queue and an ownership map to Connection touches structs referenced by the recent object-pooling/lock-free perf commits; must confirm no pooled reset path or lock-free invariant is disturbed, and OwnerConnID must NOT be added to the storage codec (structures.go note is correct — exclusive/auto-delete entities are runtime-scoped).
- Auto-delete-on-last-binding for exchanges must never target the built-in default/system exchanges (declared with AutoDelete:false at storage_broker.go:79-116, so naturally excluded) and must correctly count BOTH queue bindings (GetExchangeBindings) AND exchange-to-exchange bindings (GetExchangeBindingsFrom) before deciding the exchange is unused.
- Exclusive-consumer enforcement in RegisterConsumer must handle the exclusive-flag check under the per-queue mutex to avoid a TOCTOU race where two exclusive consumers register concurrently; the sketch reads queueConsumers before taking the mutex at line 649-651.

**Agreed resolution (authoritative):**

Real, confirmed protocol-compliance bug at medium severity. All eight proof points verified against source: (1) UnregisterConsumer (storage_broker.go:676-794) does full inflight cleanup and step 9 (768-785) detects the last consumer leaving via b.queueConsumers.Delete, but never inspects queue.AutoDelete or calls DeleteQueue. (2) DeclareQueue (355-401) uses exclusive only for redeclare validation at line 364; NewQueue/Queue struct (structures.go:124-133) carry no owner-connection field. (3) UnbindQueue (519-521) and UnbindExchange (567-569) are pass-throughs to DeleteBinding with no AutoDelete-on-last-binding check. (4) Connection struct (structures.go:13-30) has no owned-entities list. (5) server.go connection close (176-218) only unregisters consumers + RequeueAllGetDeliveries; no exclusive/auto-delete teardown. (6) basic_handlers.go:440 stores Consumer.Exclusive but RegisterConsumer (602-667) never enforces it. (7) No test asserts an auto-delete entity is deleted. Confirmed AutoDelete/Exclusive are used ONLY by the storage codec, redeclare validation, and recovery-manager redeclare — never for enforcement. The classic exclusive+auto-delete RPC reply-queue pattern therefore leaks entities plus storage/WAL state for the broker's lifetime, and a second connection can hijack another connection's exclusive queue with no RESOURCE_LOCKED (405). Severity stays medium: unbounded-but-slow growth plus a missing isolation guard, no data corruption or crash. The fix's five-part approach (owner tracking, auto-delete on last consumer, auto-delete on last exchange binding, connection-close teardown, exclusive-consumer enforcement) is correct in scope and direction, error codes (405 RESOURCE_LOCKED for exclusive-queue cross-connection, 403 ACCESS_REFUSED for exclusive-consumer) are right, and the recovery-manager exemption is correctly identified. Verdict is needs-revision because the UnregisterConsumer code sketch self-deadlocks and must be restructured before landing.

---

## [I28] Metrics pipeline largely non-functional: storage recorder never wired + many collector methods never called

**Rank 10 · P1** · Severity: 🟡 Medium · Category: observability · Effort: medium · ⚡ Quick win

### Impact
When an operator supplies a real Prometheus collector (server.WithMetrics(metrics.NewCollector(...))), the /metrics endpoint silently under-reports. Two independent gaps: (1) All storage-durability metrics (WAL writes, WAL fsync latency, WAL write errors, segment compactions, segment read errors) are permanently no-op because DisruptorStorage.SetMetrics() is never called from the builder, so the WAL/segment recorder fields stay nil and every call site is nil-guarded out. (2) A large set of server-level metrics have zero production call sites and are therefore always zero regardless of wiring: message acks, rejects, redeliveries, all three transaction counters (started/committed/rolledback), consumer totals, per-queue depth (ready/unacked/consumers) and message-age gauges, ring-buffer utilization, and the connection/channel error + message-dropped counters. Operators relying on these dashboards/alerts (e.g. per-queue backlog, ack throughput, tx rate, fsync latency, WAL error rate) get misleadingly flat/zero series and cannot detect backlog growth, ack stalls, or storage write failures. No crash or data-loss; purely observability degradation.

### Root cause
The metrics architecture defines a rich MetricsCollector interface (server/metrics.go) and a concrete Prometheus implementation (metrics.Collector) that both fully satisfy the storage-side StorageMetrics/WALMetrics/SegmentMetrics interfaces. The storage layer has the plumbing (DisruptorStorage.SetMetrics -> WALManager.SetMetrics / SegmentManager.SetMetrics) and even active call sites (qw.metrics.RecordWALWrite(), qs.metrics.RecordSegmentReadError(), etc.), but the builder never bridges the server-side collector into storage via a SetMetrics call. Separately, the recording calls for ack/reject/redeliver/transaction/consumer/queue-depth/error metrics were never added to the corresponding request handlers (basic_handlers.go ack/reject/nack, tx_handlers.go, consumer registration, queue depth polling). The interface was designed ahead of the call-site instrumentation and the wiring was left incomplete.

### Proof

- **`src/amqp-go/server/builder.go` : 251-267** — Build() creates/selects storageImpl (lines 202-219) and the metricsCollector (defaults to NoOp), attaches the collector to the Server struct, but never calls storageImpl.SetMetrics(metricsCollector). There is no bridge between the server collector and the storage layer. Note the AtomicStorage type-assertion pattern at line 241 shows the idiomatic way to reach the concrete storage type from interfaces.Storage.
- **`src/amqp-go/storage/disruptor_storage.go` : 199-207** — DisruptorStorage.SetMetrics(metrics StorageMetrics) exists and propagates to ds.wal.SetMetrics and ds.segments.SetMetrics. grep across the module shows the ONLY production callers of .SetMetrics( are these two internal delegations — nothing in server/, examples/, or anywhere else calls DisruptorStorage.SetMetrics. So ds/wal/segment metrics fields stay nil in a real deployment.
- **`src/amqp-go/storage/wal_manager.go` : 582-617** — Active recorder call sites exist (qw.metrics.RecordWALWriteError, RecordWALWrite, RecordWALFsync) but each is guarded by `if qw.metrics != nil`. Because SetMetrics is never called, qw.metrics is always nil, so all WAL write/fsync/error metrics are permanently no-op. Same pattern for segments at segment_manager.go:509-538 (RecordSegmentReadError) and 688 (RecordSegmentCompaction).
- **`src/amqp-go/examples/metrics_server.go` : 38-41** — The canonical documented example sets amqpServer.MetricsCollector = metricsCollector but never wires storage. This confirms even the intended usage path leaves storage metrics dead — the wiring gap is systemic, not just a caller mistake.
- **`src/amqp-go/server/basic_handlers.go` : 736-819** — handleBasicAck, handleBasicReject (and handleBasicNack starting line 822) contain no MetricsCollector calls. A grep for RecordMessageAcknowledged / RecordMessageRejected / RecordMessageRedelivered across server/, broker/, and transaction/ (excluding tests and NoOp/Collector definitions) returns 0 call sites, so these counters are always zero.
- **`src/amqp-go/server/tx_handlers.go` : 31-140** — handleTxSelect/handleTxCommit/handleTxRollback contain no metric calls; RecordTransactionStarted/Committed/Rolledback have 0 production call sites. Likewise SetConsumersTotal, UpdateQueueMetrics, DeleteQueueMetrics, UpdateMessageAge, DeleteMessageAge, UpdateRingBufferUtilization, DeleteRingBufferUtilization, RecordConnectionError, RecordChannelError, and RecordMessageDropped all return 0 production call sites (verified by per-method grep). These are wholly uninstrumented.
- **`src/amqp-go/server/system_metrics.go` : 104-211** — Nuance that refines (not refutes) the claim: UpdateDiskMetrics (line 104/108), UpdateWALSize (line 161) and UpdateSegmentMetrics (line 211) ARE called directly on the server collector from the periodic collectSystemMetrics loop, so these gauges DO populate when a real collector is set. The claim that WAL/segment metrics are entirely dead is slightly overstated for the size gauges; the per-event WAL/segment counters (writes/fsyncs/errors/compactions/read-errors) are the ones that stay dead.

### Fix

Two-part fix. PART A (wire storage recorder — small): In server/builder.go Build(), after constructing metricsCollector (line 252-255) and storageImpl, bridge them via a type assertion mirroring the existing AtomicStorage pattern at line 241. Define a minimal interface in the server package (or reuse storage.StorageMetrics by importing it) and assert storageImpl to it, then call SetMetrics. Because storage.SetMetrics takes storage.StorageMetrics and the server MetricsCollector already satisfies that method set, no adapter is needed for the concrete metrics.Collector or NoOpMetricsCollector — both implement all StorageMetrics methods. Guard on the assertion so custom storage impls that lack SetMetrics are unaffected. Only wire when a non-NoOp collector is present (optional optimization) to avoid the tiny per-fsync branch overhead of nil-checked calls; simplest correct version just always wires. This immediately activates RecordWALWrite/RecordWALFsync/RecordWALWriteError/RecordSegmentCompaction/RecordSegmentReadError. PART B (instrument missing call sites — medium): Add the recorder calls at the handlers: (1) basic_handlers.go handleBasicAck success path -> s.MetricsCollector.RecordMessageAcknowledged(); handleBasicReject/handleBasicNack -> RecordMessageRejected() and, when requeue==true, RecordMessageRedelivered() at the point a requeued message is re-dispatched (better placed in the broker/delivery redelivery path so it counts actual redeliveries, not reject events); (2) tx_handlers.go -> RecordTransactionStarted() in handleTxSelect, RecordTransactionCommitted() in handleTxCommit success, RecordTransactionRolledback() in handleTxRollback; (3) consumer register/unregister paths -> maintain and SetConsumersTotal(); (4) the collectSystemMetrics loop in system_metrics.go -> add per-queue UpdateQueueMetrics(ready,unacked,consumers)/UpdateMessageAge and UpdateRingBufferUtilization by querying broker/storage queue state (this is the largest sub-task and can be phased). PART A is the highest-value, lowest-risk change and should land first; PART B can be split per metric group. Keep all calls behind the existing s.MetricsCollector (never nil — builder defaults to NoOp) so no nil checks are needed at handler sites.

**Files to change:**
- `src/amqp-go/server/builder.go` — In Build(), after metricsCollector is finalized, add a type-assertion bridge (storageMetricsSetter interface with SetMetrics(storage.StorageMetrics)) and call storageImpl.SetMetrics(metricsCollector) when the assertion succeeds. Requires importing the storage package (already imported at line 11).
- `src/amqp-go/server/basic_handlers.go` — Add s.MetricsCollector.RecordMessageAcknowledged() on the success path of handleBasicAck (and AcknowledgeGetDelivery); add RecordMessageRejected() in handleBasicReject/handleBasicNack. Route RecordMessageRedelivered() into the actual requeue/redelivery dispatch path so it counts real redeliveries.
- `src/amqp-go/server/tx_handlers.go` — Add RecordTransactionStarted() in handleTxSelect, RecordTransactionCommitted() on handleTxCommit success, RecordTransactionRolledback() on handleTxRollback.
- `src/amqp-go/server/system_metrics.go` — Extend collectSystemMetrics to emit per-queue UpdateQueueMetrics(ready,unacked,consumers), UpdateMessageAge, and UpdateRingBufferUtilization by querying broker/storage queue state; maintain SetConsumersTotal from consumer registry size. Phase-able sub-task.

**Code sketch:**

```go
// server/builder.go, after metricsCollector is finalized (~line 255):
// storageMetricsSetter mirrors storage.StorageMetrics without importing an
// internal type; MetricsCollector already satisfies these methods.
type storageMetricsSetter interface {
    SetMetrics(m storage.StorageMetrics)
}
if sm, ok := storageImpl.(storageMetricsSetter); ok {
    sm.SetMetrics(metricsCollector) // metricsCollector satisfies storage.StorageMetrics
}

// server/basic_handlers.go handleBasicAck, after successful AcknowledgeMessage (~line 773):
// s.MetricsCollector is never nil (builder defaults to NoOp)
s.MetricsCollector.RecordMessageAcknowledged()

// server/tx_handlers.go handleTxCommit success:
s.MetricsCollector.RecordTransactionCommitted()
```


**Test (TDD):** TDD, two layers. (1) Unit/integration test in server package (e.g. builder_metrics_test.go): build a Server via ServerBuilder.WithStorage(default DisruptorStorage) and WithMetrics(fakeCollector) where fakeCollector records which methods were invoked; assert that after Build() the underlying DisruptorStorage received SetMetrics with that collector (expose via a test hook or a spy StorageMetrics that flips a bool in RecordWALWrite). Then publish a durable message end-to-end and assert fakeCollector.RecordWALWrite / RecordWALFsync counters are > 0 — this fails today (always 0) and passes after PART A. (2) Handler tests: drive handleBasicAck/handleBasicReject/handleTxCommit against a Server with a spy MetricsCollector and assert RecordMessageAcknowledged / RecordMessageRejected / RecordTransactionCommitted are called exactly once — these fail today (0 calls) and pass after PART B. Prefer writing the spy-collector test first (red), then wire, then green.

**Regression risk:** Low for PART A: the change is additive wiring guarded by a type assertion; NoOpMetricsCollector already satisfies StorageMetrics so the default path is unchanged behaviorally (still no-op, just via a non-nil recorder instead of nil field). One subtlety — once SetMetrics is wired, the `if qw.metrics != nil` guards become true, so the recorder methods actually execute on the hot fsync path for every batch; with a real Prometheus Collector this adds counter increments per WAL write/fsync. This is intended but is a minor hot-path cost — recent commits (see git log: 'P1 quick wins — eliminate hot-path allocs') show perf sensitivity, so benchmark WAL throughput before/after and confirm the Collector methods are cheap atomic increments (they are, per metrics.go). Low-medium risk for PART B: added calls in ack/tx handlers are single method invocations on a never-nil collector; the redelivery counter placement is the trickiest (must avoid double-counting) and the per-queue depth polling in system_metrics.go must be O(queues) and cheap since it runs on the 60s tick that already does a filepath.Walk. No protocol/correctness behavior changes.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- duplicate-key-guard

**Fix concerns:**
- PART A adds recorder calls to the hot WAL fsync path once wired; benchmark WAL throughput before/after (fix's own regressionRisk note already calls for this). Optionally wire only when a non-NoOp collector is present to avoid touching the default path, though the always-wire version is simplest and correct.
- RecordMessageRedelivered must land in the broker/delivery redelivery dispatch path, not in handleBasicReject/handleBasicNack, or it will count reject events instead of real redeliveries and can double-count. Colleague already specified this correctly.
- PART B's per-queue UpdateQueueMetrics/UpdateMessageAge/UpdateRingBufferUtilization instrumentation in collectSystemMetrics is the largest sub-task and must stay O(queues) and cheap since it shares the 60s tick that already does a filepath.Walk; reasonable to phase this separately from PART A.
- Recommend the spy-collector TDD approach (fix's test section): assert SetMetrics propagation after Build() and non-zero RecordWALWrite/RecordWALFsync after an end-to-end durable publish (red today), plus handler tests asserting exactly-once RecordMessageAcknowledged/Rejected/TransactionCommitted.

**Agreed resolution (authoritative):**

Real observability bug, severity MEDIUM, confirmed by independent source reading. Two genuine gaps: (A) DisruptorStorage.SetMetrics is never called in production — module-wide grep for `SetMetrics(` returns ONLY the two internal delegations at disruptor_storage.go:202,205; server/builder.go Build() (lines 251-267) attaches metricsCollector to the Server struct but never bridges it into storageImpl. Result: ds/wal/segment metrics fields stay nil, so every nil-guarded recorder call (wal_manager.go:585,598,612; segment_manager.go:509,528,537,687) is permanently no-op — per-event WAL write/fsync/error counters and segment compaction/read-error counters are always zero. (B) A large set of server-level metrics have zero production call sites: RecordMessageAcknowledged/Rejected/Redelivered (handleBasicAck/Reject/Nack at basic_handlers.go:736-835 contain no metric calls), all three transaction counters (tx_handlers.go has no MetricsCollector reference), SetConsumersTotal, UpdateQueueMetrics/DeleteQueueMetrics, UpdateMessageAge/DeleteMessageAge, UpdateRingBufferUtilization/DeleteRingBufferUtilization, RecordConnectionError/ChannelError/MessageDropped — each per-method grep yields only interface def + NoOp + Collector impl. IMPORTANT SCOPING (verified, prevents overstating): storage SIZE gauges DO work — UpdateDiskMetrics/UpdateWALSize/UpdateSegmentMetrics are called from collectSystemMetrics (system_metrics.go:104,108,161,211) which is actually launched (server.go:88,652; lifecycle.go:153); and publish/deliver/latency, connection/channel/queue/exchange lifecycle metrics are all wired. So only per-event counters and the enumerated server metrics are dead. Fix is SOUND. PART A (wire storage recorder) is high-value/low-risk and verified compilable: server MetricsCollector interface (metrics.go:4-79) is a strict superset of storage.StorageMetrics (disruptor_storage.go:23-28 + WALMetrics wal_manager.go:55-60 + SegmentMetrics segment_manager.go:48-52) with identical signatures, so storageImpl.SetMetrics(metricsCollector) compiles adapter-free (storage already imported at builder.go:11; build is green today); NoOpMetricsCollector also satisfies it so the default path stays behaviorally no-op. PART B is straightforward per-metric instrumentation. Two accepted caveats: (1) PART A activates the recorder on the hot WAL fsync path — benchmark before/after per the fix's own regressionRisk note (methods are cheap atomic increments); (2) RecordMessageRedelivered MUST be placed in the broker/delivery redelivery dispatch path, NOT in reject/nack handlers, to count actual redeliveries and avoid double-counting — the colleague already flagged this correctly. Not data-loss or crash; purely observability degradation, which caps severity at medium.

---

## [I15] basic.qos partially implemented: prefetch_size ignored, global ignored, and post-consume qos never resizes the per-consumer prefetch semaphore

**Rank 11 · P1** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: large

### Impact
Three distinct basic.qos gaps, all confirmed in source. (1) prefetch_size (byte cap) is stored on the channel and logged but never enforced: the broker flow-control semaphore is strictly count-based (Acquire weight 1 per message regardless of size), so a client setting prefetch_size expecting byte-based backpressure gets none. RabbitMQ rejects non-zero prefetch_size with NOT_IMPLEMENTED; StrangeQ silently accepts and ignores it, which misleads clients into believing the limit is active. (2) global is stored (channel.GlobalPrefetch) but never read; each consumer always gets an independent semaphore, so global=true (channel-wide shared window) semantics do not exist — a channel with N consumers and global qos=10 permits up to N*10 unacked messages instead of 10. (3) Most impactful: a basic.qos issued AFTER basic.consume has no effect on a running consumer. The consumer captures channel.PrefetchCount at consume time (basic_handlers.go:442) and the semaphore is sized exactly once in RegisterConsumer (storage_broker.go:620-629); there is no code path that resizes an existing consumer's semaphore. Clients using the common consume-then-qos pattern, or adjusting qos at runtime, silently keep the old/default (unlimited=2000) window — leading to unbounded in-flight growth against explicit client intent, or a consumer stuck at a small window with no way to raise it.

### Root cause
basic.qos handling was implemented only far enough to persist the three fields on the Channel struct and to size a per-consumer count semaphore at RegisterConsumer time. handleBasicQos merely writes the fields and returns qos-ok; it never (a) validates/rejects prefetch_size, (b) branches on global, or (c) notifies the broker to reconfigure existing consumers. The broker's only enforcement primitive (semaphore.Weighted) is created once and never resized, and it counts messages, not bytes, so prefetch_size and global cannot be honored by the current design.

### Proof

- **`src/amqp-go/server/basic_handlers.go` : 72-79** — PrefetchSize and GlobalPrefetch are written here, and the comment explicitly admits global is not honored. handleBasicQos then just sends qos-ok — it never calls into the broker to reconfigure existing consumers.

```go
channel.Mutex.Lock()
	channel.PrefetchCount = qosMethod.PrefetchCount
	channel.PrefetchSize = qosMethod.PrefetchSize
	channel.GlobalPrefetch = qosMethod.Global
	channel.Mutex.Unlock()

	// If global is true, we would apply these settings to all channels
	// For now, we'll just apply to this channel
```

- **`src/amqp-go/server/basic_handlers.go` : 435-446** — The consumer snapshots channel.PrefetchCount at consume time. A later basic.qos updates channel.PrefetchCount but this snapshot (and the semaphore derived from it) is never revisited.

```go
consumer := &protocol.Consumer{
		...
		PrefetchCount: channel.PrefetchCount, // CRITICAL FIX: Must set prefetch for consumer poll loop
```

- **`src/amqp-go/broker/storage_broker.go` : 620-629** — The prefetch semaphore is sized exactly once from consumer.PrefetchCount. There is no resize path anywhere (grep for UpdatePrefetch/SetPrefetch/Resize/NewWeighted returns only these two constructor sites), so post-consume qos changes are unhonored, and prefetch_size is never consulted.

```go
prefetchCount := consumer.PrefetchCount
	if prefetchCount == 0 {
		// Unlimited prefetch: use RabbitMQ quorum queue limit (2000)
		prefetchSem = semaphore.NewWeighted(2000)
		semCtx, semCancel = context.WithCancel(context.Background())
	} else {
		// Limited prefetch: use exact prefetch count
		prefetchSem = semaphore.NewWeighted(int64(prefetchCount))
		semCtx, semCancel = context.WithCancel(context.Background())
	}
```

- **`src/amqp-go/broker/storage_broker.go` : 177-182** — The poll loop acquires weight 1 per message irrespective of message byte size. Combined with every consumer owning its own semaphore, this proves the enforcement is count-based and per-consumer only — prefetch_size (bytes) and global (channel-shared) cannot be honored by this mechanism.

```go
if state.prefetchSem != nil {
			if err := state.prefetchSem.Acquire(state.semCtx, 1); err != nil {
				return
			}
		}
```

- **`src/amqp-go/protocol/structures.go` : 53-55** — The Channel struct declares all three fields; grep confirms PrefetchSize and GlobalPrefetch are only ever assigned/read in the qos handler and never consulted by any enforcement code.

```go
PrefetchCount   uint16               // Channel-level prefetch count
	PrefetchSize    uint32               // Channel-level prefetch size (0 = unlimited)
	GlobalPrefetch  bool                 // Apply prefetch settings globally
```


### Fix

Address the three gaps in order of value. FIX A (post-consume qos resize — highest value): give ConsumerState a resizable prefetch limit and let handleBasicQos push changes to the broker. Add a broker method UpdateConsumerPrefetch(consumerTag string, prefetchCount uint16) (or channel-scoped UpdateChannelPrefetch) that, for each affected ConsumerState, atomically resizes the effective window. Since golang.org/x/sync/semaphore.Weighted has no Resize, the robust approach is to replace the fixed-size semaphore with a resizable counting gate: store the limit in an atomic (state.prefetchLimit atomic.Int64) plus a sync.Cond (or a small custom weighted gate) so the poll loop blocks while inflightCount >= limit and is woken when either an ack releases a slot or the limit is raised. On qos change, update the atomic limit and Broadcast the cond so a raised limit immediately unblocks the poll loop and a lowered limit takes effect for the next acquire (already-delivered messages are allowed to drain, matching RabbitMQ). handleBasicQos, after writing channel fields, iterates channel.Consumers and calls the broker update for each consumer tag on that channel. FIX B (global semantics): thread the global flag through to the consumer/broker. When global=true, all consumers on the channel must share ONE counting gate (channel-level), so RegisterConsumer/UpdateChannelPrefetch should attach consumers to a shared per-channel gate keyed by channel identity instead of per-consumer gates. When global=false, keep per-consumer gates. FIX C (prefetch_size): match RabbitMQ by rejecting a non-zero prefetch_size with a channel-level NOT_IMPLEMENTED (540) error via the existing channel-error path, rather than silently ignoring it; document that only count-based prefetch is supported. If B is deemed large, at minimum land A and C, which are the client-visible correctness issues.

**Files to change:**
- `src/amqp-go/broker/storage_broker.go` — Replace the fixed semaphore.Weighted in ConsumerState with a resizable counting gate (atomic limit + inflight + sync.Cond, or equivalent). Add UpdateConsumerPrefetch (and, for global support, a shared per-channel gate keyed by channel). Update RegisterConsumer sizing (620-629), the poll-loop acquire (177-182), and all release sites (188,219,267,276,1219,1249,1295,1342,1370,1610) to use the new gate. Ensure semCancel/close broadcasts wake the cond so consumer teardown is not deadlocked.
- `src/amqp-go/server/basic_handlers.go` — In handleBasicQos: after storing channel fields, reject non-zero prefetch_size with a NOT_IMPLEMENTED (540) channel error; then iterate channel.Consumers and call Broker.UpdateConsumerPrefetch for each so qos-after-consume takes effect. Thread the global flag into RegisterConsumer / the update call so global=true attaches consumers to the shared channel gate.
- `src/amqp-go/broker/broker.go (or the interface defining RegisterConsumer)` — Add UpdateConsumerPrefetch (and optionally UpdateChannelPrefetch) to the Broker interface so the server layer can invoke it; adjust any mock/other implementations accordingly.

**Code sketch:**

```go
// broker: resizable gate replacing fixed semaphore
// state.limit atomic.Int64; state.inflight atomic.Int64; state.mu sync.Mutex; state.cond *sync.Cond
func (s *ConsumerState) acquire(stop <-chan struct{}) bool {
    s.mu.Lock()
    for s.inflight.Load() >= s.limit.Load() {
        // wait, but also honor stop/cancel via a wakeChan + Broadcast on close
        s.cond.Wait()
        select { case <-stop: s.mu.Unlock(); return false; default: }
    }
    s.inflight.Add(1)
    s.mu.Unlock(); return true
}
func (s *ConsumerState) release(n int64) { s.inflight.Add(-n); s.cond.Broadcast() }
func (b *StorageBroker) UpdateConsumerPrefetch(tag string, count uint16) {
    if v, ok := b.activeConsumers.Load(tag); ok {
        st := v.(*ConsumerState)
        lim := int64(count); if count == 0 { lim = 2000 }
        st.limit.Store(lim); st.cond.Broadcast() // wake to honor a raised limit
    }
}
// server/basic_handlers.go handleBasicQos, after writing channel fields:
if qosMethod.PrefetchSize != 0 { return s.channelError(conn, channelID, 540, "prefetch_size not implemented", 60, 10) }
channel.Mutex.RLock()
for tag := range channel.Consumers { s.Broker.UpdateConsumerPrefetch(tag, qosMethod.PrefetchCount) }
channel.Mutex.RUnlock()
```


**Test (TDD):** TDD in broker/storage_broker_test.go and server/basic_handlers_test.go: (1) Post-consume resize UP: register a consumer with prefetch=1, publish 5 messages, deliver/park after 1 inflight (assert only 1 delivered), call UpdateConsumerPrefetch(tag,5), assert the remaining messages become deliverable without acking the first. (2) Resize DOWN: with several inflight, lower to 1 and assert no NEW deliveries occur until inflight drains below the new limit. (3) global=true: two consumers on one channel with global qos=2, publish 10 with no acks, assert total inflight across both consumers never exceeds 2 (vs 4 for per-consumer). (4) prefetch_size: send basic.qos with prefetch_size!=0 and assert a channel.close with reply-code 540 (NOT_IMPLEMENTED) is emitted instead of silent qos-ok. (5) Regression: consume-then-nothing still delivers up to the count limit and blocks beyond it.

**Regression risk:** High-touch: the prefetch semaphore is on the core delivery hot path with ~10 release sites and interacts with consumer cancellation/shutdown (semCtx/semCancel). Replacing semaphore.Weighted with a cond-based gate risks deadlocks (missed Broadcast on teardown), lost wakeups on limit raises, and negative-inflight underflow if a release is double-counted. The global shared-gate change alters accounting across consumers and could regress FIFO/backpressure behavior. Mitigate by keeping the count-based per-consumer path (FIX A) as a self-contained gate with thorough concurrency tests and the -race detector before layering global (FIX B). FIX C (prefetch_size reject) is low risk and independently shippable.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged severity of sub-claim 3 (post-consume qos resize): the colleague calls consume-then-qos 'the common pattern,' but the canonical AMQP 0.9.1 ordering is basic.qos BEFORE basic.consume, and THAT path IS honored correctly here (consumer snapshots channel.PrefetchCount at basic_handlers.go:442, which was already set). I verified only the runtime-adjustment / qos-after-consume case is broken. Real, but this keeps it at medium rather than high. Colleague's framing overstated commonness; the resolution is that the default-before-consume flow works, only mid-stream/after-consume changes silently no-op.
- Challenged the 'unbounded in-flight growth' language: verified at storage_broker.go:621-623 that unlimited prefetch (count 0) is capped at 2000 permits, so growth is bounded at 2000/consumer, not infinite. The genuine harm is that a client lowering qos AFTER consume silently stays at the old/2000 window (lost backpressure), or a client raising a small qos can never widen it. Confirmed real but the impact wording was slightly inflated.
- Challenged FIX C's RabbitMQ-parity premise (that RabbitMQ rejects non-zero prefetch_size with NOT_IMPLEMENTED). I initially believed RabbitMQ silently ignored it and that FIX C would cause divergence. WebSearch of RabbitMQ docs/mailing list REFUTED my challenge: RabbitMQ does emit 'NOT_IMPLEMENTED - prefetch_size!=0'. Retracted my objection; FIX C is correct and StrangeQ's silent accept (basic_handlers.go:74) is a genuine protocol-compliance divergence. This resolved in the colleague's favor.
- Challenged fix soundness on batch-release semantics: the cond-gate sketch increments inflight by 1 per acquire but releases can be batched (Release(ackedCount) at 1219, nack batch at 1342, requeue batch at 1610). Verified the 1-acquire-per-delivery / releases-sum-to-deliveries invariant holds today, so a resizable atomic-inflight + cond gate preserves it — provided the down-resize path does NOT forcibly reconcile already-delivered inflight (sketch correctly lets them drain, matching RabbitMQ). Sound but must be tested with -race.
- Challenged teardown-deadlock risk: current Acquire(semCtx,1) returns error on semCancel() (storage_broker.go:179-180, used by UnregisterConsumer at 686-688). A cond-based gate must replicate this via a wake-on-close/Broadcast or it deadlocks consumer teardown; lost-wakeup between the select and cond.Wait is easy to introduce. Colleague already flagged this as high regression risk and proposed shipping count-only FIX A independently with -race before layering global FIX B — agreed.

**Fix concerns:**
- Fix scope claim about the Broker interface needs a correction: the colleague lists 'broker/broker.go (or the interface defining RegisterConsumer)' but the interface actually lives at interfaces/broker.go (RegisterConsumer at line 27, UnregisterConsumer at line 28). Any new UpdateConsumerPrefetch/UpdateChannelPrefetch method must be added there, and any mock/alternate Broker implementations updated.
- The ConsumerState.prefetchSem field comment at storage_broker.go:34 says 'nil for unlimited', but in practice it is NEVER nil (unlimited maps to NewWeighted(2000) at 623). Replacing it with a cond-gate must decide whether unlimited stays a 2000-cap gate or becomes a true no-op; the sketch keeps 2000, which is consistent with current behavior — keep it that way to avoid a behavior change.
- Global (FIX B) shared per-channel gate materially changes accounting: today each consumer holds its own semaphore keyed in activeConsumers by tag; a channel-shared gate must be keyed by channel identity and survive individual consumer register/unregister without releasing the whole gate. The 10 release sites (188,219,267,276,1219,1249,1295,1342,1370,1610) all reference state.prefetchSem per-consumer; routing them to a shared gate risks cross-consumer FIFO/backpressure regressions. Agree with the colleague's staging: land A and C first, treat B as a separate, well-tested change.
- handleBasicQos currently returns a plain Go error (not a channel-close) when the channel is missing (basic_handlers.go:67). FIX C must emit a real channel.close 540 via the existing channel-error path, not a bare returned error, and must do so BEFORE sending qos-ok — verify the channelError helper signature and that no qos-ok is sent afterward.
- The iterate-channel.Consumers loop in the fix sketch must take channel.Mutex under RLock (structures.go:50 is a sync.RWMutex) and must not call into the broker while holding a lock that the broker path could re-acquire, to avoid a lock-ordering deadlock with RegisterConsumer's per-queue mutex.

**Agreed resolution (authoritative):**

Confirmed real bug at MEDIUM severity, protocol-compliance category. All three sub-gaps are verified in source: (1) PrefetchSize is written+logged only (basic_handlers.go:60,74) and never enforced anywhere — grep confirms no read sites outside wire serialization; StrangeQ silently accepts non-zero prefetch_size whereas RabbitMQ rejects it with 'NOT_IMPLEMENTED - prefetch_size!=0' (verified via RabbitMQ docs), so this is a genuine divergence. (2) GlobalPrefetch is written only (basic_handlers.go:75), zero read sites; each consumer always gets an independent semaphore, so global=true channel-shared semantics do not exist (a channel with N consumers permits N*limit unacked). (3) No resize path exists: NewWeighted only at storage_broker.go:623/627 inside RegisterConsumer, consumer snapshots channel.PrefetchCount at consume time (basic_handlers.go:442), and a later basic.qos updates channel fields but never propagates to ConsumerState — so qos-after-consume silently no-ops. Severity stays medium (not high): the canonical qos-before-consume path works correctly, unlimited is bounded at 2000 (not truly unbounded), and only runtime adjustment / consume-then-qos is broken. Fix verdict: needs-revision. FIX A (resizable count gate + UpdateConsumerPrefetch) and FIX C (reject non-zero prefetch_size with channel 540, matching RabbitMQ) are correct and independently shippable; FIX C is low-risk. FIX A requires a cond/atomic gate replacing semaphore.Weighted across ~10 release sites with careful teardown wake-on-close to avoid deadlock, and MUST be validated with -race and the proposed up/down-resize tests. Corrections to the fix: the Broker interface is at interfaces/broker.go (lines 27-28), NOT broker/broker.go; the prefetchSem 'nil for unlimited' comment is stale (unlimited=2000 gate, keep that); handleBasicQos must emit a real channel.close 540 (not a bare error) before qos-ok; and the consumer-iteration loop must respect the channel RWMutex and broker lock ordering. FIX B (global shared per-channel gate) is the largest and riskiest piece (cross-consumer accounting/FIFO regressions) and should be staged separately after A and C land with concurrency tests.

---

## [I21] redelivered flag always false on requeued/nacked/rejected/recovered messages (both basic.deliver and basic.get-ok)

**Rank 12 · P1** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: medium

### Impact
Every re-delivery of a previously-delivered message carries redelivered=false on the wire, violating AMQP 0.9.1 (which requires redelivered=true for any message delivered more than once). Consumers using the flag for poison-message detection, dedup, or idempotency cannot tell a redelivery from a first delivery. Affected paths: basic.nack(requeue=true), basic.reject(requeue=true), basic.recover (RequeueAllForConsumer), consumer-stop/context-cancel requeue in deliverMessage, and basic.get-ok after a requeue. The one path that DOES set the flag correctly is crash recovery via RecoverFromWAL (wal_manager.go:478). Additionally PendingAck.RedeliveryCount is written as 0 and never read anywhere in production, so there is no redelivery counter to drive the flag from.

### Root cause
The requeue ring stores only the raw message tag (uint64) with no redelivery metadata (broker/queue_dispatch.go:177-187). On (re)delivery, deliverMessage re-fetches the message via storage.GetMessage and constructs the Delivery with a hardcoded Redelivered:false literal (storage_broker.go:243) instead of deriving it from whether the tag came from the requeue ring or from the message's own Redelivered field. The stored *protocol.Message.Redelivered is never mutated to true on requeue. The wire serializer faithfully copies delivery.Redelivered (batch_delivery.go:50), so false propagates to the client. The basic.get-ok path has the identical defect: GetMessageForGet returns messages (including from the requeue ring) but sendBasicGet passes a literal false to serializeGetDeliveryInto (basic_handlers.go:609), ignoring message.Redelivered entirely.

### Proof


### Fix

Track redelivery per-tag and stamp the flag onto both delivery paths. Recommended minimal, low-risk approach: mark the stored message as redelivered at the moment of requeue, and read that flag when constructing deliveries.

1) In broker/queue_dispatch.go, change QueueState.Requeue to accept a way to flag redelivery, OR (simpler and lock-free) have each requeue caller flip the stored message's Redelivered flag before calling Requeue. Since GetMessage returns the live *protocol.Message pointer from the AtomicRing (disruptor_storage.go:302 returns ring.ring.LoadByTag), setting message.Redelivered = true on the pointer persists for the next GetMessage of the same tag while it remains in the ring. Add a storage helper MarkRedelivered(queueName, tag) that does ring.ring.LoadByTag(tag) and sets .Redelivered = true (guarded so a nil/miss is a no-op). For spilled/WAL-only messages, MarkRedelivered can be a best-effort no-op since those go through RecoverFromWAL which already sets the flag.

2) Call storage.MarkRedelivered(queueName, deliveryTag) immediately before every queueState.Requeue(...) in: RejectMessage (storage_broker.go:1286), NacknowledgeMessage single (1361) and multiple (1330), RequeueAllForConsumer (1603), RejectGetDelivery (1564), and the consumer-stop/context-cancel branches of deliverMessage (265, 274). Do NOT mark in the normal first-delivery flow.

3) In deliverMessage (storage_broker.go:240-246), set Redelivered: message.Redelivered (and PendingAck.Redelivered: message.Redelivered) instead of the false literal, so a requeued message that was marked carries the flag. Also set the RedeliveryCount if a counter is desired (see optional step 5).

4) In server/basic_handlers.go:609, pass message.Redelivered instead of the literal false to serializeGetDeliveryInto.

5) (Optional, if a real redelivery COUNT is wanted for poison-message limits) Store a redeliveryCount alongside each tag. Since PendingAck.RedeliveryCount is never read, either wire it into a per-queue sync.Map[tag]int incremented in MarkRedelivered, or add a count field to Message. This is a larger change; the boolean flag fix above already restores spec compliance.

Note on the pointer-mutation approach: AtomicRing returns the shared pointer, so setting .Redelivered concurrently with a reader is a benign single-bool data race in theory. To be strictly race-free, either (a) guard with the ring's existing synchronization, or (b) prefer the alternative below.

Alternative (fully race-free, slightly more code): change the requeue ring to store a small struct {tag uint64; redelivered bool} instead of bare uint64. Requeue(tag) pushes {tag,false} for first-time requeues but callers push {tag,true}; tryPopRequeue/Claim return the redelivered bool up to deliverMessage/GetMessageForGet which then set Delivery.Redelivered accordingly. This localizes the flag to the dispatch layer and never mutates shared message state.

**Files to change:**
- `src/amqp-go/broker/storage_broker.go` — In deliverMessage set Redelivered from the message (or from the requeue-ring struct) instead of the false literal (lines 236, 243). Add MarkRedelivered calls before Requeue in RejectMessage (1286), NacknowledgeMessage (1330, 1361), RequeueAllForConsumer (1603), RejectGetDelivery (1564), and the stop/cancel branches (265, 274).
- `src/amqp-go/broker/queue_dispatch.go` — If using the race-free alternative: change requeueTags to []requeuedTag{tag,redelivered}, update Requeue/tryPopRequeue/Claim signatures to carry the redelivered bool.
- `src/amqp-go/storage/disruptor_storage.go` — Add MarkRedelivered(queueName, tag) that LoadByTag and sets .Redelivered = true (best-effort; no-op on miss). Register it on the storage interface.
- `src/amqp-go/interfaces/ (storage interface file)` — Add MarkRedelivered(queueName string, tag uint64) to the storage interface consumed by StorageBroker.
- `src/amqp-go/server/basic_handlers.go` — Line 609: pass message.Redelivered (or the ring-provided redelivered bool) to serializeGetDeliveryInto instead of the literal false.

**Code sketch:**

```go
// storage_broker.go deliverMessage
delivery := &protocol.Delivery{
    Message:     message,
    DeliveryTag: msgID,
    Redelivered: message.Redelivered, // was: false
    Exchange:    message.Exchange,
    RoutingKey:  message.RoutingKey,
    ConsumerTag: state.consumer.Tag,
}

// e.g. RejectMessage requeue branch
if requeue {
    b.storage.MarkRedelivered(state.queueName, deliveryTag)
    b.storage.DeletePendingAck(state.queueName, deliveryTag)
    queueState.Requeue(deliveryTag)
}

// basic_handlers.go basic.get-ok
s.serializeGetDeliveryInto(buf, channelID, deliveryTag, message.Redelivered, message.Exchange, message.RoutingKey, messageCount, message)
```


**Test (TDD):** TDD integration test in broker (or server) package: 1) Publish one message to a queue with a registered consumer; assert the first Delivery has Redelivered==false. 2) basic.nack(requeue=true) the delivery; assert the redelivered Delivery to the (same or next) consumer has Redelivered==true. Repeat for basic.reject(requeue=true) and basic.recover. 3) basic.get variant: publish, basic.get (no_ack=false), reject(requeue=true), basic.get again, assert the second get-ok reports redelivered=true. A focused unit test can call StorageBroker.deliverMessage indirectly by driving the dispatch loop, or assert via the serialized frame bytes (redelivered byte at the known offset). Currently such a test would fail (observes false) and pass after the fix.

**Regression risk:** Low-to-moderate. The boolean-flag change is additive and only affects messages that have actually been requeued. Risk areas: (1) if using the shared-pointer mutation approach, a message re-published or served from a pool could carry a stale Redelivered=true — ensure Redelivered is reset to false at publish time / on pool reset (check Message pooling introduced in recent perf commits). (2) The requeue-ring struct alternative touches Claim/tryPopRequeue hot paths and must preserve FIFO and the lock-free tail-cursor logic. (3) MarkRedelivered on spilled/WAL-only tags is a no-op, so extremely large requeued backlogs that have spilled out of the ring will still report false until WAL recovery — acceptable and documentable, but note it. Formatting must pass gofmt and the existing broker/server test suites should be re-run.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the fix's PRIMARY 'shared-pointer mutation' approach as introducing a real data race: GetMessage returns the live ring pointer (disruptor_storage.go:302 LoadByTag), so a requeue-caller goroutine writing message.Redelivered=true races with deliverMessage reading it. Resolution: the colleague's own 'Alternative' (requeue ring stores {tag,redelivered} struct) is race-free and must be the primary fix, not the fallback. The colleague acknowledged the race but dismissed it as 'benign in theory' — I reject that framing for a lock-free codebase.
- Challenged the colleague's stated regression risk #1 (stale Redelivered=true from a pooled/reused Message). Verified protocol.Message is NOT pooled — it is heap-allocated fresh at basic_handlers.go:296 on every publish with Redelivered defaulting to false; only PendingMessage/ContentHeader/Frame/BasicPublishMethod are pooled (buffer_pool.go). Resolution: this concern is moot and the proposed pool-reset mitigation is unnecessary. This point actually strengthens the fix.
- Probed whether this is cosmetic to justify downgrading below medium. Confirmed it is a genuine AMQP 0.9.1 wire-protocol violation of a documented client-facing field across every requeue path, affecting poison-message detection and idempotency logic. But no data loss/crash. Resolution: medium is correct — not downgraded to low, not upgraded to high.
- Verified existing test basic_get_purge_test.go:326 asserts getOK.Redelivered==false and checked it would not break: it is a first-delivery (publish-then-get) scenario where a fresh message correctly carries false. Resolution: no regression to existing suite.

**Fix concerns:**
- PRIMARY recommended approach (mutating shared message.Redelivered via the GetMessage/LoadByTag live pointer) creates an unsynchronized bool data race between requeue callers and deliverMessage/GetMessageForGet; must use the race-free requeue-ring-struct alternative instead.
- The requeue-ring-struct alternative touches the Claim/tryPopRequeue hot paths (queue_dispatch.go) and must preserve FIFO ordering and the lock-free tail-cursor/requeueCount accounting; needs careful review of Requeue/tryPopRequeue signature changes and the requeueMu-guarded slice.
- Spilled/WAL-only requeued tags (evicted from the ring) will still report Redelivered=false until WAL recovery; acceptable edge case but should be documented in the fix.
- Fix must keep the basic.deliver path (deliverMessage:243) and the basic.get-ok path (basic_handlers.go:609) deriving the flag from the SAME source (the ring-provided bool) to avoid the two paths diverging again.
- RedeliveryCount remains dead (written 0, never read); the boolean fix restores spec compliance but a real redelivery counter for poison-message limits is a separate, larger change and should not be conflated with this fix.

**Agreed resolution (authoritative):**

Real bug, confirmed by direct source reading. The redelivered flag is hardcoded false on every re-delivery path: storage_broker.go:243 (deliverMessage sets Delivery.Redelivered: false literal) and 236 (PendingAck.Redelivered: false / RedeliveryCount: 0), and basic_handlers.go:609 (basic.get-ok passes literal false). The requeue ring (queue_dispatch.go:177-187) stores bare uint64 tags with no redelivery metadata, so there is nowhere to derive the flag from. The wire serializer (batch_delivery.go:50) faithfully copies delivery.Redelivered, so false reaches the client on basic.deliver and basic.get-ok. All requeue call sites confirmed: RejectMessage:1286, NacknowledgeMessage single:1361/multiple:1330, RequeueAllForConsumer:1603, RejectGetDelivery:1564, RequeueAllGetDeliveries:1633, deliverMessage stop/cancel:265/274. The ONE correct path is crash recovery (wal_manager.go:478 sets Redelivered: true); crash_test_tool/main.go:185,207 even documents 'Redelivered flag not yet set during recovery (TODO)'. RedeliveryCount is written 0 and never read in production. This violates AMQP 0.9.1 (redelivered MUST be true on any re-delivery). Severity stays MEDIUM: genuine spec violation affecting poison-message/dedup/retry-limit consumer logic, but no data loss, crash, or durability impact. Fix verdict: NEEDS-REVISION. The colleague's PRIMARY 'recommended minimal' approach — mutating the shared *protocol.Message.Redelivered pointer returned by GetMessage/LoadByTag — introduces a genuine unsynchronized data race (a requeue-caller goroutine writes message.Redelivered=true while deliverMessage reads it concurrently); go test -race would flag it, and this is unacceptable in a lock-free, TDD-disciplined codebase. The colleague's own 'Alternative' (change the requeue ring to store {tag uint64, redelivered bool} and carry the flag up through tryPopRequeue/Claim to deliverMessage and GetMessageForGet) is the CORRECT, race-free design and must be promoted to the primary fix; deliverMessage:243 and basic_handlers.go:609 then read the ring-provided bool instead of message.Redelivered. Note: one of the colleague's stated regression risks is moot — protocol.Message is NOT pooled (freshly heap-allocated at basic_handlers.go:296 with Redelivered defaulting false; only PendingMessage/ContentHeader/Frame/BasicPublishMethod are pooled), so no stale-flag-from-pool concern exists and no pool-reset mitigation is needed. Existing test basic_get_purge_test.go:326 (first-delivery get-ok asserting Redelivered==false) remains green under either fix since a fresh message carries false. The spilled/WAL-only-tag edge case (message evicted from ring before requeue) will still report false until WAL recovery — acceptable and documentable. Module builds clean. Recommended TDD test: publish, deliver, nack(requeue=true), assert re-delivery has Redelivered==true; repeat for reject(requeue=true), basic.recover, and basic.get after reject(requeue=true).

---

## [I11] No receive-side heartbeat/idle timeout; heartbeat sender hardcoded to 5s and ignores negotiated tune value

**Rank 13 · P1** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: medium

### Impact
A dead peer that stops sending data without sending TCP FIN/RST (crashed VM, pulled cable, silently dropped NAT/firewall state) is never detected by the broker. The frame reader blocks forever in io.ReadFull with no read deadline, so none of the connection's goroutines (reader, processor, ack processor, heartbeat sender, consumer delivery loop, all started in processConnectionFrames) ever exit and the *protocol.Connection is never removed from s.Connections. Unacknowledged messages held by that connection's consumers stay locked (not requeued) until the OS TCP keepalive eventually reaps the socket, which on Linux defaults is ~2h+ (7200s idle + probes). Under repeated dead-client churn this accumulates leaked goroutines, memory, and stuck deliveries. Separately, the server violates the AMQP 0.9.1 heartbeat contract: it advertises Heartbeat=60 in connection.tune but sends heartbeats every 5s regardless, and it entirely ignores the client's connection.tune-ok choice, including a client that requested heartbeat=0 (disabled), which still receives 5s heartbeats. The negotiated value and the configured HeartbeatIntervalMS are both dead: the tune-ok payload is never even parsed.

### Root cause
Three coupled gaps. (1) The frame read path (protocol.ReadFrameOptimized, used by both the handshake in processConnectionFrames and the readFrames loop) reads via io.ReadFull with no net.Conn.SetReadDeadline, so a stalled peer blocks the reader goroutine indefinitely; there is no missed-heartbeat detection. (2) sendHeartbeats hardcodes heartbeatInterval := 5 * time.Second with an explicit TODO to use the negotiated tune value. (3) processConnectionMethod's ConnectionTuneOK case just logs and returns nil without deserializing the payload (ConnectionTuneOKMethod has only Serialize, no Deserialize), so the client's negotiated heartbeat value is discarded and never stored on the Connection. The config field interfaces.NetworkConfig.HeartbeatIntervalMS is defined and defaulted (60000) but never consumed by the server.

### Proof

- **`src/amqp-go/server/server.go` : 503-511** — Heartbeat send interval is hardcoded to 5s with an explicit TODO confirming the negotiated tune value is ignored. This matches the claim exactly (lines 508-509).

```go
func (s *Server) sendHeartbeats(...) {
	// Send heartbeats very frequently (every 5 seconds)...
	// TODO: Use negotiated heartbeat value from connection.tune
	heartbeatInterval := 5 * time.Second
```

- **`src/amqp-go/server/connection_handlers.go` : 320-323** — The tune-ok handler neither deserializes the payload nor stores the client's negotiated heartbeat. A client requesting heartbeat=0 (disabled) is silently ignored; the server keeps sending 5s heartbeats and never learns the client's timeout for detecting missed heartbeats.

```go
case protocol.ConnectionTuneOK: // Method ID 31 for connection class
	s.Log.Debug("Received connection.tune-ok", ...)
	return nil
```

- **`src/amqp-go/protocol/frame_optimized.go` : 28-46** — The frame reader uses io.ReadFull with no deadline. Because callers pass conn.Conn (a net.Conn) and never call SetReadDeadline, the reader goroutine blocks forever on a stalled-but-open socket. No missed-heartbeat timeout exists on the receive side.

```go
func ReadFrameOptimized(reader io.Reader) (*Frame, error) {
	...
	_, err := io.ReadFull(reader, header)
	...
	_, err = io.ReadFull(reader, payload)
```

- **`src/amqp-go/server/server.go` : 301-333** — Five goroutines are spawned per connection and teardown is gated on one of them finishing. The reader can only finish on a read error (EOF/RST). If the peer is silently dead, readFrames stays blocked in ReadFrameOptimized, so teardown never triggers and the connection + its goroutines + unacked messages leak. sendHeartbeats detects a broken pipe only when a write fails, which does not happen for a receive-only stall until TCP itself times out.

```go
go s.readFrames(conn, readerDone)
...
go s.sendHeartbeats(conn, heartbeatSenderDone)
...
select {
case <-readerDone: ...
case <-processorDone: ...
}
```

- **`src/amqp-go/server/server.go` : 350-370** — readFrames only exits on a read error. With no SetReadDeadline on conn.Conn, this loop never returns for a silently dead peer, confirming the leak path.

```go
func (s *Server) readFrames(conn *protocol.Connection, done chan struct{}) {
	...
	frame, err := protocol.ReadFrameOptimized(conn.Conn)
	if err != nil { ... conn.Closed.Store(true); return }
```

- **`src/amqp-go/interfaces/config.go` : 45** — A configurable heartbeat interval exists (defaulted to 60000 in config/config.go:25, settable via builder.go:55) but grep shows it is never read anywhere outside config/tests, so operators cannot actually control heartbeat behavior.

```go
HeartbeatIntervalMS int64
```


### Fix

Implement proper heartbeat negotiation and a receive-side idle timeout per AMQP 0.9.1 section 4.2.7. Steps: (1) Add a negotiated-heartbeat field to protocol.Connection, e.g. `HeartbeatSec atomic.Uint32`, and add a `Deserialize([]byte) error` method to protocol.ConnectionTuneOKMethod that parses ChannelMax(uint16), FrameMax(uint32), Heartbeat(uint16). (2) In server.sendConnectionTune, use the configured HeartbeatIntervalMS (converted to whole seconds, min 1) as the server's proposed heartbeat instead of the literal 60, so config becomes live. (3) In processConnectionMethod's ConnectionTuneOK case, deserialize the payload and store the client's chosen heartbeat: `conn.HeartbeatSec.Store(uint32(tuneOk.Heartbeat))`. Per spec the client's value is the final negotiated value (server proposes, client may lower or set 0 to disable). (4) In sendHeartbeats, read the negotiated value: if it is 0, do not start the ticker at all (heartbeats disabled) and just wait on <-done; otherwise set the send interval to negotiatedSec/2 seconds (RabbitMQ convention) with a sane floor. (5) Add receive-side detection: in readFrames, if HeartbeatSec > 0, call `conn.Conn.SetReadDeadline(time.Now().Add(2 * heartbeat * time.Second))` before each ReadFrameOptimized (the spec allows declaring the peer dead after ~2 missed intervals). On a timeout error (os.IsTimeout / net.Error.Timeout()), log and treat it as a dead connection: set conn.Closed and return, which triggers the existing teardown that requeues unacked deliveries. Because handshake frames are read before negotiation completes, apply a conservative fixed handshake deadline (e.g. 30s) during processConnectionFrames handshake reads, then switch to the negotiated deadline once tune-ok is processed. (6) Optionally, on accept, enable TCP keepalive as a backstop for the pre-negotiation window: type-assert conn to *net.TCPConn and call SetKeepAlive(true)/SetKeepAlivePeriod. Note ReadFrameOptimized takes an io.Reader, so SetReadDeadline must be invoked on conn.Conn (the net.Conn) in readFrames, not inside ReadFrameOptimized.

**Files to change:**
- `src/amqp-go/protocol/structures.go` — Add HeartbeatSec atomic.Uint32 field to Connection (records negotiated heartbeat in seconds; 0 = disabled).
- `src/amqp-go/protocol/methods.go` — Add Deserialize([]byte) error to ConnectionTuneOKMethod parsing ChannelMax/FrameMax/Heartbeat.
- `src/amqp-go/server/connection_handlers.go` — In ConnectionTuneOK case (line ~320), deserialize the payload and store conn.HeartbeatSec. In sendConnectionTune, derive the proposed heartbeat from Config.Network.HeartbeatIntervalMS instead of the literal 60.
- `src/amqp-go/server/server.go` — In readFrames, set a read deadline based on the negotiated heartbeat (2x interval) before each read and treat timeouts as a dead-connection close. In sendHeartbeats, replace hardcoded 5s with negotiated/2 and skip sending entirely when heartbeat=0. Apply a fixed handshake read deadline during processConnectionFrames handshake reads. Optionally enable TCP keepalive in handleConnection/acceptLoop.

**Code sketch:**

```go
// protocol/methods.go
func (m *ConnectionTuneOKMethod) Deserialize(data []byte) error {
	if len(data) < 8 { return fmt.Errorf("tune-ok too short") }
	m.ChannelMax = binary.BigEndian.Uint16(data[0:2])
	m.FrameMax = binary.BigEndian.Uint32(data[2:6])
	m.Heartbeat = binary.BigEndian.Uint16(data[6:8])
	return nil
}

// connection_handlers.go, ConnectionTuneOK case:
tuneOk := &protocol.ConnectionTuneOKMethod{}
if err := tuneOk.Deserialize(payload); err != nil { return err }
conn.HeartbeatSec.Store(uint32(tuneOk.Heartbeat))
return nil

// server.go readFrames loop:
hb := conn.HeartbeatSec.Load()
if hb > 0 {
	_ = conn.Conn.SetReadDeadline(time.Now().Add(time.Duration(2*hb) * time.Second))
}
frame, err := protocol.ReadFrameOptimized(conn.Conn)
if err != nil {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		s.Log.Warn("heartbeat missed, closing dead connection", zap.String("connection_id", conn.ID))
	}
	conn.Closed.Store(true)
	return
}

// server.go sendHeartbeats:
hb := conn.HeartbeatSec.Load()
if hb == 0 { <-done; return } // heartbeats disabled by negotiation
interval := time.Duration(hb) * time.Second / 2
if interval < time.Second { interval = time.Second }
ticker := time.NewTicker(interval)
```


**Test (TDD):** Add server-level tests: (1) TestHeartbeatDisabledByClient - complete handshake sending connection.tune-ok with Heartbeat=0, assert conn.HeartbeatSec==0 and that no FrameHeartbeat frames are received from the server within, say, 3s (currently fails: 5s ticker would send). (2) TestNegotiatedHeartbeatHonored - send tune-ok Heartbeat=2, assert the server sends heartbeats at ~1s (2/2) and that conn.HeartbeatSec==2. (3) TestDeadPeerReaped (the key durability test) - open a connection, complete handshake with a small heartbeat (e.g. 2s), register a consumer with an unacked delivery, then stop reading/writing on the client socket WITHOUT closing it (simulate silent death, e.g. by using a net.Pipe or a raw TCP conn that is abandoned). Assert that within ~2*heartbeat seconds the server marks the connection closed, removes it from s.Connections, and requeues the unacked delivery. Before the fix this test hangs until OS keepalive. Also add a protocol unit test TestConnectionTuneOKDeserialize round-tripping Serialize->Deserialize.

**Regression risk:** Moderate. The read deadline must be re-armed before every read; forgetting to clear/re-arm it (or setting it during a legitimately idle-but-alive negotiated period where the client sends heartbeats) could prematurely kill healthy connections, so the deadline must be a generous multiple (2x) of the negotiated interval per spec, and heartbeat=0 must fully disable both sending and the read deadline. The handshake phase reads (start-ok, tune-ok, open) happen before negotiation, so a separate conservative deadline there is needed to avoid killing slow-authenticating clients. Existing tests in server/*_test.go and protocol/rabbitmq_interop_test.go exercise handshake and heartbeat framing and will catch obvious breakage. Reducing the advertised heartbeat via config could surprise clients relying on the old 5s cadence, but that cadence was itself non-compliant. Low risk of data corruption since changes are confined to connection lifecycle and framing timing.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether the leaked deliveries are truly stuck vs. eventually cleaned: verified via handleConnection (server.go:176-217) and broker/storage_broker.go:676-730 that consumer unregister DOES requeue in-flight/buffered deliveries, but ONLY once processConnectionFrames returns, which for a silent-dead peer never happens until OS keepalive. Impact claim holds.
- Challenged whether OS TCP keepalive already backstops this, arguing for a downgrade to low: confirmed net.Listen("tcp") at server.go:104 sets no keepalive and no SetKeepAlive call exists anywhere, so the only backstop is the kernel default (~2h idle). That is a real, long-lived leak under churn, so low is too weak; medium stands.
- Challenged whether the colleague's proof-6 grep claim (HeartbeatIntervalMS never read) was accurate: verified independently — the field is only referenced in config/builder.go, config/config.go, and tests; the server never consumes it. Confirmed.
- Challenged the fix's read-deadline placement: it is proposed for readFrames, but the three handshake reads (server.go:233/267/279) run in processConnectionFrames BEFORE HeartbeatSec is set, so a pre-negotiation dead peer would still leak. The fix acknowledges this and proposes a fixed handshake deadline; flagged that this MUST actually be implemented, not just noted, and that the pre-negotiation window remains a (narrow) gap.
- Challenged the regression risk of a 2x read deadline against the original 'aggressive 5s to survive WriteMutex contention' comment: raised that a slow-but-alive client whose heartbeats are delayed under load could be false-positive killed. Resolved that a generous 2x multiple plus a floor mitigates this, but the concern is legitimately moderate and the deadline multiple must not be tightened.

**Fix concerns:**
- Handshake reads (server.go:233/267/279) occur in processConnectionFrames before negotiation, so the read-deadline arming in readFrames does not cover them; the proposed fixed handshake deadline must be implemented explicitly or the pre-negotiation silent-death window remains a leak.
- Timing false-positives: a healthy client whose own heartbeat transmission is delayed under WriteMutex contention (the exact scenario the original 5s cadence comment was defending against) could be reaped by a too-tight read deadline. The 2x multiple and a sane floor must be preserved; do not tighten.
- Integer duration division interval = time.Duration(hb)*time.Second/2 is correct only with the proposed 1s floor for hb=1; ensure the floor is applied so a client negotiating heartbeat=1 does not degenerate to a 0 or sub-second ticker.
- Must ensure heartbeat=0 fully disables BOTH the send ticker AND the read deadline (leaving the deadline armed with hb=0 would compute a zero/immediate deadline and instantly kill the connection); the sketch handles this but it is the single most dangerous edge to get wrong.
- The read deadline is re-armed per read but never explicitly cleared; confirm the per-read re-arm on every ReadFrameOptimized iteration is sufficient and that no read path bypasses the arming (e.g. verify the deadline covers both the header and payload io.ReadFull calls, which it does since both share conn.Conn's deadline).

**Agreed resolution (authoritative):**

Confirmed real bug at medium severity across all three coupled gaps, every proof point verified against source. (1) protocol/frame_optimized.go:28-46 ReadFrameOptimized uses io.ReadFull with no deadline; grep confirms SetReadDeadline/SetDeadline appear NOWHERE in the non-test codebase and net.Listen("tcp") sets no keepalive. All four read call sites pass conn.Conn with no deadline: handshake reads server.go:233/267/279 and the main loop server.go:356. (2) server/server.go:503-511 sendHeartbeats hardcodes heartbeatInterval := 5*time.Second with the explicit TODO. (3) server/connection_handlers.go:320-323 ConnectionTuneOK case logs and returns nil; protocol/methods.go:333-360 ConnectionTuneOKMethod has only Serialize and no Deserialize (the ParseConnectionTune at methods.go:308-331 is for the server->client tune method, not tune-ok). config field interfaces/config.go:45 HeartbeatIntervalMS is defaulted to 60000 (config.go:25) and settable (builder.go:55) but never read by the server; sendConnectionTune (connection_handlers.go:65-71) hardcodes Heartbeat: 60. The leak path is confirmed: readFrames (server.go:350-380) only exits on a read error and marks conn.Closed; that exit closes FrameQueue, ends processFrames, and returns processConnectionFrames' select, which triggers the cleanup in handleConnection (server.go:176-217) that unregisters consumers (broker/storage_broker.go:676 UnregisterConsumer, which DOES requeue buffered/in-flight deliveries at steps 2/6) and deletes from s.Connections. For a silently-dead peer no read error ever occurs, so none of this runs until OS TCP keepalive reaps the socket (~2h+ on Linux defaults), leaking goroutines/memory and holding unacked deliveries locked. The proposed fix is spec-correct (AMQP 0.9.1 4.2.7): add HeartbeatSec to Connection, add Deserialize to ConnectionTuneOKMethod, store the client's final tune-ok value (client may lower or set 0 to disable), derive the server proposal from HeartbeatIntervalMS, send at negotiated/2, disable both send and read-deadline when 0, and arm SetReadDeadline(2*hb) per read in readFrames treating net.Error.Timeout() as a dead-connection close (which reuses the existing requeue-on-teardown path). Severity stays medium: reliability + protocol-compliance bug with a realistic dead-client-churn trigger, but bounded (keepalive eventually reaps, requeue eventually happens, no data corruption).

---

## [I7] basic.publish does not validate the user-id message property against the authenticated connection username (identity spoofing)

**Rank 14 · P1** · Severity: 🟡 Medium · Category: security · Effort: small · ⚡ Quick win

### Impact
A publisher can set the AMQP `user-id` message property to any arbitrary value and the broker will accept it and faithfully deliver it to consumers unchanged. Consumers that trust `user-id` for provenance/authorization (a documented, common pattern — RabbitMQ advertises "validated user-id" precisely so downstream services can rely on it) can be fooled into attributing a message to another principal. For example, an authenticated low-privilege client "alice" can publish a message stamped `user-id=admin`, and any consumer using `user-id` for audit logging or authorization decisions will treat it as originating from "admin". Impact is bounded to metadata trust (no direct broker compromise), matching the medium severity. RabbitMQ rejects such a mismatched publish with a 406 PRECONDITION_FAILED channel exception; StrangeQ does not.

### Root cause
The publish path performs an authorization check on the exchange (write permission) in handleBasicPublish, but never inspects the `user-id` content-header property. When the complete message is assembled in processCompleteMessage, `pendingMsg.Header.UserID` is copied verbatim into the outgoing Message (basic_handlers.go:311). The connection already knows the authenticated identity (conn.Username, set during handshake in connection_handlers.go:261/291/298/305), but no comparison is ever made between message.UserID and conn.Username. The delivery serializer then propagates the unvalidated UserID to consumers (frame_helpers.go:191-192 sets the flag, :223 writes the value).

### Proof

- **`src/amqp-go/server/basic_handlers.go` : 296-315** — The UserID from the client-supplied content header is copied straight into the message with no validation against the authenticated connection identity.

```go
message := &protocol.Message{
		...
		UserID:          pendingMsg.Header.UserID,
		...
	}
```

- **`src/amqp-go/server/basic_handlers.go` : 107-115** — The only authorization performed at publish time is exchange write permission. There is no user-id property check anywhere in the publish path (handleBasicPublish -> processCompleteMessage).

```go
// Authorization check: basic.publish requires write permission on exchange
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceExchange,
		Resource:     publishMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 40)
	}
```

- **`src/amqp-go/protocol/content.go` : 194-204** — The content-header parser reads the user-id property directly off the wire with no validation, so any value the client sends is trusted.

```go
if header.PropertyFlags&FlagUserID != 0 {
		...
		userID, newOffset, err := decodeShortString(frame.Payload, offset)
		...
		header.UserID = userID
```

- **`src/amqp-go/server/frame_helpers.go` : 191-192, 223** — On delivery the (unvalidated) UserID is re-serialized and sent to consumers, so a spoofed user-id reaches the consumer intact.

```go
if msg.UserID != "" {
		flags |= protocol.FlagUserID
	}
...
		UserID:          message.UserID,
```

- **`src/amqp-go/protocol/structures.go` : 14-30** — The authenticated username is available on the Connection (in scope inside processCompleteMessage) to compare against, but is never used for this purpose.

```go
type Connection struct {
	ID              string
	...
	Username        string                     // Authenticated username
```


### Fix

Add RabbitMQ-compatible validated user-id enforcement in the publish path. In processCompleteMessage (server/basic_handlers.go), immediately after building the `message` struct (line ~315) and before the confirm/transaction/broker logic, add a check: if `message.UserID != "" && message.UserID != conn.Username`, reject the publish with a 406 PRECONDITION_FAILED channel exception scoped to basic.publish (class 60, method 40), and return an error so no further processing occurs. Do NOT reject when UserID is empty (property absent) and do NOT reject when it matches — this mirrors RabbitMQ semantics exactly. Because processCompleteMessage returns before PublishMessage is called, the message is neither routed, buffered into a transaction, nor confirmed. Use the existing sendChannelClose helper (server/authz.go:41) which already marks the channel closed. Prefer amqperrors.PreconditionFailed (406) for the reply code; if that constant is not already defined in the errors package, add it (value 406). Since this changes protocol-visible behavior, gate it behind an opt-in config flag so existing deployments that rely on the permissive behavior are not broken — e.g. Config.Security.ValidateUserID (default false to preserve current behavior, or default true to match RabbitMQ; recommend defaulting false and documenting, then flipping in a later release). When the flag is false, skip the check entirely. Consider special-casing the impersonator tag: RabbitMQ allows users tagged `impersonator` to set an arbitrary user-id; if conn.User is a *interfaces.User with the `impersonator` tag, allow the mismatch. This is optional for a first cut.

**Files to change:**
- `src/amqp-go/server/basic_handlers.go` — In processCompleteMessage, after the `message := &protocol.Message{...}` block (~line 315), insert a validated user-id check gated on the config flag. When message.UserID != "" and != conn.Username, call s.sendChannelClose(conn, channelID, 406, "PRECONDITION_FAILED - user_id property set to '<x>' but authenticated user was '<y>'", 60, 40) and return an error. Optionally allow the mismatch when the authenticated user carries the 'impersonator' tag.
- `src/amqp-go/errors/errors.go` — Add a PreconditionFailed = 406 constant if one does not already exist, alongside AccessRefused, for use as the reply code.
- `src/amqp-go/config/config.go (or wherever Security config lives)` — Add a boolean Security.ValidateUserID field (with sensible default and documentation) to opt into the enforcement.

**Code sketch:**

```go
// in processCompleteMessage, right after building `message`:
if s.Config != nil && s.Config.Security.ValidateUserID &&
    message.UserID != "" && message.UserID != conn.Username {
    // (optional) allow impersonator-tagged users
    if !userHasTag(conn.User, "impersonator") {
        s.sendChannelClose(conn, channelID, amqperrors.PreconditionFailed,
            fmt.Sprintf("PRECONDITION_FAILED - user_id property set to '%s' but authenticated user was '%s'", message.UserID, conn.Username),
            60, 40)
        return fmt.Errorf("user_id mismatch: property=%q connection=%q", message.UserID, conn.Username)
    }
}
```


**Test (TDD):** Add a table-driven test in server/basic_handlers_test.go (TDD): (1) publish with UserID equal to conn.Username -> message is routed/published, no channel.close frame written; (2) publish with UserID != conn.Username and ValidateUserID=true -> processCompleteMessage returns an error, PublishMessage is NOT invoked (use a mock/fake broker recording calls), and a channel.close frame with reply code 406 and class/method 60/40 is written to the connection; (3) publish with empty UserID -> accepted regardless of flag; (4) ValidateUserID=false with a mismatched UserID -> accepted (backward compatibility). Use an in-memory/fake net.Conn to capture written frames and assert the 406 close frame bytes.

**Regression risk:** Low-to-moderate. The core logic change is a small guarded conditional in one function. The main regression risk is behavioral/protocol-visible: any existing client that publishes a mismatched (or arbitrary) user-id will now get its channel closed with 406 when the feature is enabled. This is why the fix must be behind a config flag (recommend default-off initially) so no existing deployment breaks silently. There is no risk to the fast path when the flag is off (single boolean check). Ensure the early return happens BEFORE transaction buffering and confirm-tag acking so a rejected publish does not leak a confirm or a buffered tx operation.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged severity: with AuthenticationEnabled/AuthorizationEnabled both defaulting to false (config.go:44,49) and username hardcoded to 'guest' when auth is off (connection_handlers.go:261), there is no authenticated identity to spoof out-of-the-box. Resolved: held at medium as a defensible ceiling since it is a documented RabbitMQ-parity trust issue when auth is enabled, but flagged that real-world exposure is narrower than the writeup implies (leaning medium-low).
- Challenged the fix's claim to 'add PreconditionFailed = 406 if it does not already exist' — verified it DOES already exist at errors/errors.go:43. Resolved: that filesToChange entry is dead work and must be dropped; the constant is ready to use.
- Challenged the early-return placement for confirm/tx leakage. Resolved: verified PublishMessage is at line 341, confirm-tag assignment at 319-325, tx buffering at 328-334 — inserting the guard after line 315 and returning before them is correct and leaks nothing.
- Raised an unresolved caveat: both callers (processHeaderFrame:198-208 and processBodyFrame:264-274) return the error up the stack after sendChannelClose; the reviewer must confirm the dispatch loop does not additionally kill the whole connection (which would be harsher than RabbitMQ's channel-only 406). Noted this mirrors the existing authzChannelError pattern (authz.go:73-84) so at worst it is consistent, not novel.

**Fix concerns:**
- The 'add PreconditionFailed = 406 constant' step is invalid — the constant already exists at errors/errors.go:43. Drop that file change entirely.
- Verify the frame-dispatch loop does not treat the non-nil error returned by processCompleteMessage as connection-fatal; the intended behavior is a channel-level 406 close (RabbitMQ parity), not a full connection teardown.
- conn.User is typed interface{} (structures.go:20), so the optional impersonator check requires a *interfaces.User type assertion (as authorize() does at authz.go:27), not a direct field access.
- Ensure the check reads conn.Username (not a value derived from the message) and skips entirely when the flag is off, preserving the zero-overhead fast path (single boolean).

**Agreed resolution (authoritative):**

Real bug at medium severity; fix approach is sound but needs revision (remove the redundant errors-constant change and verify dispatcher error handling is channel-scoped) before implementation.

---

# P2

## [I13] exchange.declare accepts arbitrary exchange types; unknown type is stored and silently black-holes all published messages

**Rank 15 · P2** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: small · ⚡ Quick win

### Impact
A client that declares an exchange with a type the broker does not implement (e.g. "x-consistent-hash", "x-delayed-message", a typo like "topics", or an empty "") receives exchange.declare-ok and believes the exchange is usable. Because the routing switch has no default case, every message published to that exchange matches zero queues and is silently discarded (RouteMessage returns nil unless the publish set mandatory=true). Data loss is silent: no channel error, no return, no log. This diverges from AMQP 0.9.1 / RabbitMQ, which reject an unknown exchange type at declare time with a 503 command-invalid channel/connection error ("unknown exchange type 'xyz'"). Secondary defect: the ONE place a type IS checked — re-declaring an existing exchange with a different type (storage_broker.go:291-293) — returns a plain error that propagates to server.go:404-411 and tears down the entire TCP connection instead of sending a channel-level 406 precondition-failed, killing all other channels on that connection.

### Root cause
DeclareExchange stores whatever exchangeType string it is handed as Exchange.Kind with no membership check against the set of implemented types {direct, fanout, topic, headers}, and the server handler handleExchangeDeclare performs no type validation before calling it. The routing dispatch in collectTargetQueues uses a plain switch on exchange.Kind with cases only for the four known types and no default branch, so an unrecognized Kind falls through to zero matches. Combined with RouteMessage treating an empty target-queue list as a no-op success, an unknown type becomes a silent black hole.

### Proof


### Fix

Add exchange-type validation at declare time and return a proper AMQP channel error instead of silently creating a black-hole exchange. Steps: (1) In errors/errors.go (or a shared location) define the canonical set of supported exchange types as a small map/helper, e.g. IsSupportedExchangeType(kind string) bool covering "direct","fanout","topic","headers" (extend later if plugins are added). (2) In server/exchange_handlers.go handleExchangeDeclare, after the reserved-name and authz checks and BEFORE calling s.Broker.DeclareExchange, validate the type — but ONLY for active declares: if !declareMethod.Passive and the type is non-empty and not supported, send a channel.close with 503 command-invalid (classID 40, methodID 10) and return. Passive declares must not validate the type (spec: passive only checks existence), so skip validation when Passive is true. Also reject an empty type on active declare as 503 (RabbitMQ requires a type). (3) Defensively, in broker/storage_broker.go DeclareExchange, add the same guard so the broker never persists an unroutable exchange even if called from a non-server path (recovery_manager, tests): if the type is unknown, return a typed error (errors.NewChannelError with CommandInvalid) rather than storing it. Note recovery_manager.go:128 re-declares previously persisted exchanges on startup; since only valid types could have been persisted after this fix, recovery is unaffected, but keep the broker guard lenient enough not to break recovery of legitimately-typed exchanges. (4) Optionally add a default case to the two routing switches in collectTargetQueues that logs a warning (defense-in-depth) — after (1)-(3) it should be unreachable but protects against any bypass. (5) Separately (can be a follow-up), convert the re-declare type-mismatch path so it surfaces as a 406 precondition-failed channel.close rather than a plain error that drops the connection; the cleanest way is to have handleExchangeDeclare detect the mismatch error kind and call sendChannelClose with 406 instead of returning it up to server.go. Prefer implementing the validation in the server handler (has access to conn/channel and the sendChannelClose helper) as the primary fix, with the broker guard as a safety net.

**Files to change:**
- `src/amqp-go/errors/errors.go` — Add supportedExchangeTypes set and IsSupportedExchangeType helper (single source of truth for implemented exchange types).
- `src/amqp-go/server/exchange_handlers.go` — In handleExchangeDeclare, after reserved-name/authz checks and before Broker.DeclareExchange, reject non-passive declares whose Type is empty or unsupported by sending a channel.close with 503 CommandInvalid (class 40, method 10). Skip validation for passive declares.
- `src/amqp-go/broker/storage_broker.go` — In DeclareExchange, add a defensive guard that returns a typed CommandInvalid error for a new exchange whose type is not in the supported set, so no code path can persist an unroutable exchange. Optionally add a default case to the two switch statements in collectTargetQueues (lines 915-950 and 959-968) that logs a warning.

**Code sketch:**

```go
// errors/errors.go
var supportedExchangeTypes = map[string]struct{}{
    "direct": {}, "fanout": {}, "topic": {}, "headers": {},
}
func IsSupportedExchangeType(kind string) bool { _, ok := supportedExchangeTypes[kind]; return ok }

// server/exchange_handlers.go, inside handleExchangeDeclare after authz, before DeclareExchange:
if !declareMethod.Passive {
    if declareMethod.Type == "" || !amqperrors.IsSupportedExchangeType(declareMethod.Type) {
        s.sendChannelClose(conn, channelID, amqperrors.CommandInvalid,
            fmt.Sprintf("unknown exchange type '%s'", declareMethod.Type), 40, 10)
        return fmt.Errorf("unknown exchange type %q", declareMethod.Type)
    }
}

// NOTE: returning the error still triggers server.go:404 connection teardown.
// To send ONLY a channel.close (not close the connection), return nil after
// sendChannelClose, mirroring how other channel-level rejections should behave,
// OR introduce a sentinel the frame loop treats as channel-level. Confirm the
// desired convention against how authzChannelError callers are handled today
// (authzChannelError currently returns the error, which also tears down the conn
// per server.go:404 — that is a pre-existing wider issue; match the codebase's
// intended pattern and, if channel-only close is desired, return nil here).
```


**Test (TDD):** TDD, two layers. (1) server-level integration test in server/ (alongside existing handler tests): open a connection+channel, send exchange.declare with Type="x-consistent-hash" and Passive=false, assert the broker responds with channel.close carrying ReplyCode 503 and does NOT send declare-ok; also assert declaring with Type="" is rejected, and that a passive declare of a pre-existing valid exchange still succeeds. (2) broker-level unit test in broker/storage_broker_test.go: call b.DeclareExchange("bad","nonexistent",...) and assert it returns a non-nil error and that GetExchange("bad") returns ErrExchangeNotFound (nothing persisted); as a regression guard, declare a valid "topic" exchange, bind a queue, publish a matching message, and assert it is delivered (proves the four known types still route). A pre-fix run must show the bogus-type declare succeeding and the message being dropped; post-fix it must be rejected.

**Regression risk:** Low. The change only adds rejection for exchange types that were previously non-functional anyway (they black-holed messages), so no working workload relied on them. Main risks: (a) rejecting an empty type could break a client that relied on the old lenient behavior of passing "" — but the default nameless exchange is pre-created at startup (Name "" already exists), and clients normally never declare it, so impact is negligible; verify by not validating passive declares. (b) The broker-level guard must not break recovery_manager.go:128 which re-declares persisted exchanges on startup — since only valid types can be persisted after the fix, this is safe, but confirm recovery tests still pass. (c) If validation returns an error up through server.go it will drop the connection rather than just the channel; decide the intended convention (return nil after sendChannelClose for channel-only close) to avoid a heavier-handed teardown than RabbitMQ, and keep it consistent with the existing authzChannelError pattern.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the fix's core convention assumption ('returning the error tears down the connection, heavier-handed than RabbitMQ, so return nil after sendChannelClose'). Verified authz.go:73-84 + server.go:400-411: the codebase's EXISTING channel-error pattern already returns the error and closes the connection. Resolution: for unknown exchange type this connection teardown is actually spec-correct — RabbitMQ closes the connection with 503 command_invalid for unknown types — so the fix must NOT return nil here; it should send channel.close(503) (or better, connection.close 503) and return the error. The fix's suggested 'return nil for channel-only close' would be LESS spec-compliant for this specific case.
- Challenged the claim that the re-declare mismatch (storage_broker.go:291-293) should surface as a 406. Verified RRabbitMQ semantics: re-declaring an existing exchange with a different type is indeed a 406 precondition-failed at the CHANNEL level (distinct from unknown-type which is a 503 connection error). Resolution: the fix conflates the two error classes; unknown-type = 503 (connection close acceptable), type-mismatch-on-redeclare = 406 (channel-only close, return nil). The fix should treat these two paths differently, not identically.
- Challenged the broker-guard regression risk against default exchanges and recovery. Verified storage_broker.go:73-126 creates built-ins via storage.StoreExchange (bypasses DeclareExchange, unaffected) and recovery_manager.go:117-144 tolerates per-exchange errors. Resolution: broker guard is safe as long as it only rejects NEW unknown-type declares; risk downgraded to genuinely low.
- Challenged whether the referenced recovery_manager path exists at the stated location. The JSON cited 'recovery_manager.go:128' without a package; verified the actual file is server/recovery_manager.go:128 (there is no broker/recovery_manager.go). Resolution: minor path imprecision in the JSON; the logic described is accurate.
- Challenged the empty-Type edge case ('is Type="" really black-holed or handled by the default-exchange path?'). Verified collectTargetQueues line 893 keys the default-exchange fast path on exchange.Name=="", NOT Kind, so a named exchange declared with Kind="" falls through the switch to zero matches. Resolution: empty-type black-hole confirmed; rejecting empty Type on active declare is correct.

**Fix concerns:**
- Error-return convention is unresolved in the fix and its default guidance ('return nil after sendChannelClose for channel-only close') is WRONG for the unknown-type case: RabbitMQ closes the CONNECTION with 503 command_invalid for an unknown exchange type. The fix should send a connection.close(503) via sendConnectionClose (server/connection_handlers.go:342) OR keep channel.close(503)+return-the-error (which closes the connection at server.go:409), but must NOT return nil and leave the connection alive.
- The fix conflates two distinct AMQP error classes: unknown-type-at-declare (503 command_invalid, connection-level in RabbitMQ) vs re-declare-with-different-type (406 precondition_failed, channel-level). The 406 re-declare path (storage_broker.go:291-293) and the 503 unknown-type path need different handling and different return conventions; the JSON's step (5) treats them loosely.
- codeSketch uses declareMethod.Type and amqperrors.CommandInvalid but validation must be placed AFTER the existing reserved-name (line 57) and authz (line 64) checks and BEFORE DeclareExchange (line 74) — the sketch says this but implementers should preserve the passive-skip precisely (protocol.ExchangeDeclareMethod has Passive bool at methods.go:813, confirmed).
- Defense-in-depth default cases should be added to BOTH switch statements in collectTargetQueues (the queue-binding switch at 915-950 AND the exchange-to-exchange switch at 959-968); the JSON only explicitly enumerates the first pair of line ranges in one filesToChange entry — ensure both are covered.

**Agreed resolution (authoritative):**

CONFIRMED real bug at MEDIUM severity. I independently verified every load-bearing claim: (1) broker/storage_broker.go:282-327 DeclareExchange stores Kind=exchangeType verbatim with zero membership check against {direct,fanout,topic,headers}; (2) server/exchange_handlers.go:32-97 handleExchangeDeclare does reserved-name + authz checks then calls DeclareExchange with no type validation and sends declare-ok; (3) collectTargetQueues (storage_broker.go:915-950 and 959-968) is a plain switch with NO default case, so an unknown Kind yields zero matches; (4) RouteMessage (lines 828-833) treats an empty target list as a no-op success unless message.Mandatory, so an unknown type silently black-holes every publish. An empty Type="" is also black-holed: the default-exchange fast path (line 893) keys on exchange.Name=="" not Kind, so a NAMED exchange with Kind="" falls through the switch. No exchange-type validation exists anywhere; CommandInvalid=503 is defined in errors/errors.go:59 but unused. FIX APPROACH IS DIRECTIONALLY CORRECT (add IsSupportedExchangeType helper, validate in handleExchangeDeclare for non-passive declares, add broker-level guard as safety net, add routing default cases for defense-in-depth) but NEEDS REVISION on the error-return convention, which the fix author themselves flagged as unresolved. AGREED FINAL POSITION on the convention: I verified authzChannelError (authz.go:73-84) sends channel.close then RETURNS the error, and processFrames (server.go:400-411) sets conn.Closed and tears down the whole TCP connection on ANY non-nil error. So the codebase's existing 'channel error' pattern already closes the connection. For unknown-exchange-type specifically this is actually acceptable and MORE spec-accurate than the fix feared: real RabbitMQ rejects an unknown exchange type with a CONNECTION-level 503 command_invalid ('unknown exchange type xyz'), not a channel-level 406 — so sending channel.close(503) and returning the error (which closes the connection) is close to correct behavior, though ideally the fix should send a connection.close(503) via sendConnectionClose (connection_handlers.go:342) to precisely match RabbitMQ rather than channel.close. Either way, do NOT return nil-after-channel-close for THIS case, since that would leave the connection alive in a way RabbitMQ does not. The SEPARATE re-declare type-mismatch defect (storage_broker.go:291-293 returns a plain fmt.Errorf that propagates to server.go:404 and kills the connection) IS a genuine spec divergence — RabbitMQ returns a channel-level 406 precondition-failed there — and should be fixed as the follow-up the JSON describes (detect mismatch, send channel.close 406, return nil for channel-only close). Regression risk is genuinely LOW: verified default exchanges ("", amq.direct/fanout/topic/headers) are created via storage.StoreExchange directly (storage_broker.go:73-126), bypassing DeclareExchange, so a broker guard cannot break them; recovery_manager.go:128 re-declares only previously-persisted (valid) types and already tolerates per-exchange DeclareExchange errors by collecting them into ValidationErrors rather than aborting; and no existing test declares an unsupported type. Severity stays MEDIUM: silent data loss is serious, but it requires a client to actively declare a non-standard type (a misconfiguration/typo), not a default-path failure, so it is not a broad-impact P0/P1. Required revisions before merge: (a) decide and document the close convention — prefer sendConnectionClose(503) for unknown-type to match RabbitMQ, or at minimum keep channel.close(503)+return-error (connection closes) and do NOT return nil; (b) keep the broker guard lenient (only reject NEW unknown-type exchanges, never the built-ins or recovery of valid types); (c) implement the 406 re-declare-mismatch conversion as described; (d) TDD both layers as specified, with a pre-fix run demonstrating the black-hole.

---

## [I23] DeleteExchange leaves dangling queue/exchange bindings and stale binding caches (silent misrouting on recreate)

**Rank 16 · P2** · Severity: 🟡 Medium · Category: correctness · Effort: small · ⚡ Quick win

### Impact
Deleting an exchange (AMQP exchange.delete, the default ifUnused=false path) never removes the QueueBindings or ExchangeBindings that reference it, and never invalidates the exchangeBindingsCache / exchBindingsFromCache / bindingCache entries for that name. Two concrete consequences: (1) Resource/metadata leak — orphaned binding files accumulate under bindings/ and exchange_bindings/ and are never garbage-collected. (2) Silent misrouting on recreate — because DeclareExchange permits recreating a same-named exchange of a DIFFERENT type after deletion, the leftover bindings are picked up verbatim by collectTargetQueues and matched against the new exchange's routing semantics (e.g. a topic pattern binding stored for the old topic exchange is now evaluated by a recreated fanout/direct exchange). Messages get routed to queues the operator believes are no longer bound, or dropped/duplicated depending on the type change. This violates AMQP expectations that deleting an exchange tears down its bindings.

### Root cause
Binding lifecycle is not tied to exchange lifecycle on delete. broker.DeleteExchange only performs an existence check plus an optional ifUnused binding-count check, then delegates to storage.DeleteExchange, which removes only the exchange file and its exchangeCache entry. Neither layer enumerates and deletes the queue bindings (GetExchangeBindings) or exchange-to-exchange bindings (GetExchangeBindingsFrom) that reference the exchange, nor invalidates the corresponding binding caches. The correct cleanup pattern already exists for queues (DeleteQueue deletes all GetQueueBindings) but was omitted for exchanges. Because binding lookups fall back to a full disk scan (ListBindings) on cache miss, the orphaned files remain matchable even after any cache eviction.

### Proof

- **`src/amqp-go/broker/storage_broker.go` : 330-352** — The broker layer only reads bindings to enforce ifUnused; when ifUnused is false (the AMQP default) it deletes the exchange without ever deleting any queue bindings or exchange-to-exchange bindings, and never invalidates binding caches.

```go
func (b *StorageBroker) DeleteExchange(name string, ifUnused bool) error {
	_, err := b.storage.GetExchange(name)
	...
	if ifUnused {
		bindings, err := b.storage.GetExchangeBindings(name)
		...
		if len(bindings) > 0 { return fmt.Errorf("exchange '%s' in use", name) }
	}
	return b.storage.DeleteExchange(name)
}
```

- **`src/amqp-go/storage/persistent_metadata.go` : 279-294** — The storage layer removes only the exchange file and the exchangeCache entry. It never removes binding files nor invalidates exchangeBindingsCache/bindingCache/exchBindingCache/exchBindingsFromCache, so bindings referencing the exchange survive deletion.

```go
func (pm *PersistentMetadataStore) DeleteExchange(name string) error {
	pm.mutex.Lock(); defer pm.mutex.Unlock()
	path := filepath.Join(pm.baseDir, ExchangesDir, name+FileExtension)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) { ... }
	if pm.cacheEnabled { pm.exchangeCache.Delete(name) }
	return nil
}
```

- **`src/amqp-go/broker/storage_broker.go` : 455-461** — DeleteQueue performs exactly the binding cleanup that DeleteExchange omits, demonstrating the intended pattern and confirming the exchange path is an inconsistent gap rather than by-design.

```go
	// Delete all bindings for this queue
	bindings, err := b.storage.GetQueueBindings(name)
	if err == nil {
		for _, binding := range bindings {
			b.storage.DeleteBinding(binding.QueueName, binding.ExchangeName, binding.RoutingKey)
		}
	}
```

- **`src/amqp-go/broker/storage_broker.go` : 284-296** — Type-mismatch validation only runs when the exchange still exists. After DeleteExchange, GetExchange returns ErrExchangeNotFound, so a same-named exchange of a DIFFERENT kind can be freshly created — and it will then match the leftover bindings.

```go
existing, err := b.storage.GetExchange(name)
...
if existing != nil {
	if existing.Kind != exchangeType { return fmt.Errorf("...type mismatch...") }
	return nil
}
// Create new exchange
```

- **`src/amqp-go/broker/storage_broker.go` : 910-950** — Routing consumes GetExchangeBindings(name) directly. Stale bindings left by a delete are matched against the (possibly recreated, possibly differently-typed) exchange, producing silent misroutes.

```go
bindings, err := b.storage.GetExchangeBindings(exchange.Name)
...
switch exchange.Kind {
case "direct": ... if binding.RoutingKey == routingKey ...
case "fanout": ... (all bindings)
case "topic": ... if b.matchTopicPattern(binding.RoutingKey, routingKey) ...
case "headers": ... if b.matchHeaders(binding.Arguments, message.Headers) ...
```

- **`src/amqp-go/storage/persistent_metadata.go` : 628-656** — On cache miss GetExchangeBindings rebuilds from a disk scan of ListBindings(); since the binding files are never deleted, orphaned bindings reappear even after cache eviction, so the leak is durable, not merely a stale-cache artifact.

```go
func (pm *PersistentMetadataStore) GetExchangeBindings(exchangeName string) (...) {
	if cached, ok := pm.exchangeBindingsCache.Load(exchangeName); ok { return cached... }
	allBindings, err := pm.ListBindings() // full disk scan
	...for _, binding := range allBindings { if binding.ExchangeName == exchangeName { result = append(...) } }
	pm.exchangeBindingsCache.Store(exchangeName, result)
```


### Fix

Make exchange deletion tear down all bindings that reference the exchange, mirroring DeleteQueue. Do the cleanup at the broker layer (broker.DeleteExchange) so it composes existing storage primitives and stays type-agnostic across storage backends.

In broker.DeleteExchange (storage_broker.go:330-352), after confirming the exchange exists and passing the ifUnused check, and BEFORE calling b.storage.DeleteExchange(name):
1. Enumerate queue bindings for the exchange via b.storage.GetExchangeBindings(name) and delete each with b.storage.DeleteBinding(binding.QueueName, binding.ExchangeName, binding.RoutingKey). (DeleteBinding already invalidates queueBindingsCache and exchangeBindingsCache.)
2. Enumerate exchange-to-exchange bindings where this exchange is the SOURCE via b.storage.GetExchangeBindingsFrom(name) and delete each with b.storage.DeleteExchangeBinding(eb.Source, eb.Destination, eb.RoutingKey).
3. Also remove exchange-to-exchange bindings where this exchange is the DESTINATION. There is no GetExchangeBindingsTo helper today; add one to the MetadataStore interface + PersistentMetadataStore + DisruptorStorage (scan loadExchangeBindingsFromDisk filtering on Destination == name), OR, to avoid an interface change, iterate all exchanges and their GetExchangeBindingsFrom results and delete any whose Destination == name. The dedicated helper is cleaner and O(files) instead of O(exchanges*files).
4. After deleting the exchange, defensively invalidate the exchange-side caches for the name: since storage.DeleteExchange does not clear exchangeBindingsCache/exchBindingsFromCache, add those Delete calls inside storage.DeleteExchange (persistent_metadata.go:279-294) so any residual cache entry keyed by the exchange name is dropped: pm.exchangeBindingsCache.Delete(name) and pm.exchBindingsFromCache.Delete(name). This guards the case where bindings were somehow only cached.

Keep the ifUnused semantics intact: only perform destructive binding cleanup when the delete actually proceeds (ifUnused=false, or ifUnused=true with zero bindings — in which case there is nothing to clean but the defensive cache invalidation is still harmless).

Note: GetExchangeBindings currently only counts QUEUE bindings for the ifUnused check; per AMQP, exchange-to-exchange destination bindings also count as 'in use'. That is a separate concern — do not expand ifUnused semantics in this fix to avoid scope creep, but the cleanup in steps 1-3 must run regardless.

**Files to change:**
- `src/amqp-go/broker/storage_broker.go` — In DeleteExchange (around lines 340-351), before 'return b.storage.DeleteExchange(name)', add binding teardown: delete all queue bindings from GetExchangeBindings(name); delete all e2e bindings from GetExchangeBindingsFrom(name); delete all e2e bindings where Destination==name (via new GetExchangeBindingsTo helper or a scan). Errors from cleanup should be logged/tolerated but ideally the primary delete proceeds only after cleanup, matching DeleteQueue's best-effort pattern.
- `src/amqp-go/storage/persistent_metadata.go` — In DeleteExchange (lines 279-294) add, under the cacheEnabled guard, pm.exchangeBindingsCache.Delete(name) and pm.exchBindingsFromCache.Delete(name). Optionally add a GetExchangeBindingsTo(dest) helper that filters loadExchangeBindingsFromDisk by Destination.
- `src/amqp-go/storage/disruptor_storage.go` — If a GetExchangeBindingsTo helper is added, forward it to metadataStore like the other binding methods (around lines 589-599).
- `src/amqp-go/interfaces/storage.go` — If adding GetExchangeBindingsTo, declare it on MetadataStore (near lines 92-95) alongside GetExchangeBindingsFrom. Optional; can be avoided by iterating exchanges in the broker instead.

**Code sketch:**

```go
// broker/storage_broker.go — DeleteExchange, before final delete
// tear down queue bindings that reference this exchange
if qbs, err := b.storage.GetExchangeBindings(name); err == nil {
    for _, bnd := range qbs {
        _ = b.storage.DeleteBinding(bnd.QueueName, bnd.ExchangeName, bnd.RoutingKey)
    }
}
// tear down e2e bindings where this exchange is the source
if ebs, err := b.storage.GetExchangeBindingsFrom(name); err == nil {
    for _, eb := range ebs {
        _ = b.storage.DeleteExchangeBinding(eb.Source, eb.Destination, eb.RoutingKey)
    }
}
// tear down e2e bindings where this exchange is the destination
if ebs, err := b.storage.GetExchangeBindingsTo(name); err == nil { // new helper
    for _, eb := range ebs {
        _ = b.storage.DeleteExchangeBinding(eb.Source, eb.Destination, eb.RoutingKey)
    }
}
return b.storage.DeleteExchange(name)

// storage/persistent_metadata.go — DeleteExchange, inside cacheEnabled block
pm.exchangeBindingsCache.Delete(name)
pm.exchBindingsFromCache.Delete(name)
```


**Test (TDD):** Add TestDeleteExchange_RemovesBindings_NoStaleMatchOnRecreate to broker/exchange_binding_test.go (uses existing createTestBroker/createExchanges/createQueues helpers). Steps: (1) createExchanges(t, b, "ex1", "topic"); createQueues(t, b, "q1"); b.BindQueue("q1","ex1","a.#",nil). (2) Assert GetExchangeBindings("ex1") len==1. (3) b.DeleteExchange("ex1", false). (4) Assert GetExchangeBindings("ex1") len==0 (proves binding files removed, not just cache). (5) b.DeclareExchange("ex1","fanout",false,false,false,nil) — recreate as a DIFFERENT type. (6) call b.PublishMessage("ex1","a.b", msg) (or collectTargetQueues) and assert q1 is NOT targeted (a fanout with no bindings routes nowhere). Before the fix, step 4 returns len==1 and step 6 delivers to q1 via the stale topic binding, so the test fails; after the fix it passes. Add a companion TestDeleteExchange_RemovesExchangeToExchangeBindings: bind ex1->ex2 via BindExchange, delete ex1, assert GetExchangeBindingsFrom("ex1") is empty.

**Regression risk:** Low. The change is additive best-effort cleanup that mirrors the already-shipped DeleteQueue pattern, using existing DeleteBinding/DeleteExchangeBinding primitives (both idempotent w.r.t. os.IsNotExist). Main risks: (1) if a GetExchangeBindingsTo helper is added to the MetadataStore interface, every implementation and test mock of that interface must add the method or the build breaks — mitigated by instead iterating existing helpers in the broker if an interface change is undesirable. (2) Concurrent publish during delete could observe a partially-cleaned binding set, but that race already exists and the net effect is fewer stale matches, not more. (3) DeleteExchange returns nil for not-found before reaching cleanup, so the cleanup path is not invoked for missing exchanges.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether cleanup happens elsewhere in the call chain (channel/AMQP handler). Verified via grep: handleExchangeDelete (server/exchange_handlers.go:100-141) calls Broker.DeleteExchange directly and does no binding cleanup; broker_adapters.go/builder.go just forward. The gap is real, not compensated downstream.
- Challenged the 'silent misrouting on recreate' severity: it requires recreating the same-name exchange as a DIFFERENT type, which typical AMQP clients do not do (they re-declare the same type). Confirmed DeclareExchange (282-296) allows any type after delete since GetExchange returns NotFound. Concluded the misroute is a real but narrow sub-case; the durable leak and same-type resurfacing are the always-present harms, so medium (not high) holds.
- Challenged whether the broker-layer DeleteBinding loop actually clears the exchange-side cache. Verified DeleteBinding (persistent_metadata.go:551-555) invalidates bindingCache, queueBindingsCache, and exchangeBindingsCache — so queue-binding cache invalidation is fully handled by the primitive; the fix's extra defensive Delete is belt-and-suspenders, not load-bearing for that path.
- Challenged the fix's step 3 (destination-side e2e bindings) for feasibility/regression: confirmed GetExchangeBindingsTo does not exist (interfaces/storage.go:96 only has GetExchangeBindingsFrom). Adding it to the MetadataStore interface would break DisruptorStorage and any mocks; the colleague already flagged the broker-iteration alternative, which I endorse to keep regression risk low.

**Fix concerns:**
- Prefer the broker-side ListExchanges + GetExchangeBindingsFrom(Destination==name) iteration over adding a GetExchangeBindingsTo to the MetadataStore interface, to avoid breaking all interface implementations/mocks; if the dedicated helper is added, disruptor_storage.go must forward it and every mock updated or the build breaks.
- Concurrent publish during delete can observe a partially-torn-down binding set (this race already exists); acceptable since net effect is fewer stale matches, but the fix should not be presented as making delete atomic. No new lock is introduced.
- Fix does not (and per the colleague's scope note, should not) expand ifUnused to count e2e destination bindings as 'in use'; that AMQP-conformance gap remains a separate follow-up.
- Ensure the defensive pm.exchBindingsFromCache.Delete(name) is only meaningful for the source-keyed cache; destination-side stale e2e entries are keyed by the OTHER exchange's source name, so they are handled by the DeleteExchangeBinding calls in step 3, not by this Delete — the fix text is correct but reviewers should not assume the cache Delete alone covers destination-side.

**Agreed resolution (authoritative):**

Real bug, medium severity, correctness. Verified against source: broker.DeleteExchange (src/amqp-go/broker/storage_broker.go:330-352) only does an existence check plus an optional ifUnused binding-count check, then calls b.storage.DeleteExchange(name). storage.DeleteExchange (persistent_metadata.go:279-294) removes only the exchange file and the exchangeCache entry. DisruptorStorage.DeleteExchange (disruptor_storage.go:495-499) just forwards to metadataStore.DeleteExchange. The AMQP handler handleExchangeDelete (server/exchange_handlers.go:100-141) calls Broker.DeleteExchange with no extra cleanup. So neither the queue bindings (GetExchangeBindings), nor the e2e bindings where the exchange is source (GetExchangeBindingsFrom), nor those where it is destination are ever removed, and the exchangeBindingsCache/exchBindingsFromCache entries keyed by the name are never invalidated. GetExchangeBindings (persistent_metadata.go:628-656) and GetExchangeBindingsFrom (772-796) rebuild from a full disk scan on cache miss, so orphaned files remain matchable durably. DeleteQueue (storage_broker.go:455-461) performs exactly the binding teardown DeleteExchange omits, confirming this is an inconsistent gap. Consequences: (1) durable resource/metadata leak — orphaned files under bindings/ and exchange_bindings/ never GC'd; (2) dangling bindings resurface when a same-name exchange is recreated (DeclareExchange at 282-327 only type-checks when GetExchange still finds an existing exchange; after delete it returns ErrExchangeNotFound so a same-name exchange of any type can be created), and collectTargetQueues (910-950) matches those stale bindings against the recreated exchange's semantics — e.g. a recreated fanout routes to all leftover bindings ignoring routing key (lines 925-931). The misrouting-on-type-change path is real but narrower (requires recreate with a different type, which typical clients do not do); the leak plus same-type-recreate resurfacing is the always-present correctness harm. Medium is correct — data-plane correctness impact but bounded by the recreate precondition and not a crash/loss on the normal path.

Fix is sound. Do the teardown in broker.DeleteExchange (type-agnostic, composes existing primitives) after the ifUnused gate and before storage.DeleteExchange: (1) delete each GetExchangeBindings(name) via DeleteBinding — verified DeleteBinding (persistent_metadata.go:537-559) invalidates bindingCache, queueBindingsCache and exchangeBindingsCache, so no separate cache handling needed for queue bindings; (2) delete each GetExchangeBindingsFrom(name) via DeleteExchangeBinding — verified DeleteExchangeBinding (751-769) clears exchBindingCache and exchBindingsFromCache[source]; (3) destination-side e2e bindings must also be torn down — confirmed there is NO GetExchangeBindingsTo helper today (interfaces/storage.go:96 only declares GetExchangeBindingsFrom), so prefer the broker-side iteration over ListExchanges + GetExchangeBindingsFrom filtering Destination==name to avoid an interface change that would break every MetadataStore impl/mock (the DisruptorStorage nil-guard delegation pattern at disruptor_storage.go:575-600 would each need updating); the O(exchanges*files) cost is acceptable for a control-plane delete. The defensive additions in storage.DeleteExchange (pm.exchangeBindingsCache.Delete(name) and pm.exchBindingsFromCache.Delete(name) under cacheEnabled) are correct and useful for the zero-binding-but-stale-cache edge and for the destination cache keyed by name; harmless otherwise. Keep ifUnused semantics unchanged. Best-effort/idempotent cleanup (os.IsNotExist tolerated) mirrors DeleteQueue and is low regression risk. The proposed tests (recreate-as-fanout no-stale-match, and e2e source teardown) are correct discriminators; recommend the broker-iteration variant for destination-side rather than a new interface method.

---

## [I27] Client-initiated connection.close is acknowledged but never triggers server-side teardown; connection lingers and server keeps sending frames after close-ok

**Rank 17 · P2** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: small · ⚡ Quick win

> **Severity adjusted by debate:** claimed `low` → agreed `medium`.

### Impact
When a client sends connection.close, the server replies connection.close-ok but never tears down the connection: it does not set conn.Closed, does not close the socket, and does not stop its goroutines. The reader goroutine stays blocked in ReadFrameOptimized (no read deadline exists anywhere), the frame/ack/consumer-delivery goroutines stay alive, and — critically — the heartbeat sender keeps writing FrameHeartbeat frames to the client on a 5s ticker because conn.Closed is still false. This is an AMQP 0.9.1 protocol violation: after sending connection.close-ok the server must not send further frames and should close the socket. Practically: (a) well-behaved clients that expect the server to close after close-ok, or that keep the socket briefly open, leave the server holding all per-connection goroutines and resources (FrameQueue, AckQueue, consumer registrations, WriteMutex) until TCP EOF; (b) the server transmits heartbeat frames onto a connection the client considers closed, which a strict client may treat as a framing error. Under churn of clients that gracefully close, this leaks goroutines/connection state until each underlying TCP socket is actually torn down by the client. Secondary defect: sendConnectionClose hardcodes ClassID/MethodID = 0 and ignores its classMethod argument, so server-initiated close frames never report the offending class/method (and callers actually pass human-readable detail text into that argument, indicating confused parameter semantics).

### Root cause
The ConnectionClose case in processConnectionMethod only sends close-ok and returns nil. Returning nil means processFrame yields (pooled=true, err=nil), so processFrames does not set conn.Closed and does not return, and none of the done channels selected on in handleConnection fire — the code path that calls conn.Conn.Close() and close(conn.Done) is never reached. There is no read deadline / heartbeat-timeout, so readFrames blocks forever on ReadFrameOptimized until the client sends EOF. Separately, sendConnectionClose ignores its classMethod parameter and hardcodes ClassID:0/MethodID:0.

### Proof

- **`src/amqp-go/server/connection_handlers.go` : 328-332** — The comment says 'close the connection' but the handler only sends close-ok and returns nil. It never sets conn.Closed nor signals teardown.

```go
case protocol.ConnectionClose: // Method ID 50 for connection class
		s.Log.Info("Received connection.close", ...)
		// Send connection.close-ok and close the connection
		return s.sendConnectionCloseOK(conn)
```

- **`src/amqp-go/server/frame_helpers.go` : 33-36** — sendConnectionCloseOK writes the frame only; unlike sendConnectionClose it does not call conn.Closed.Store(true). So the client-close path leaves conn.Closed=false.

```go
func (s *Server) sendConnectionCloseOK(conn *protocol.Connection) error {
	return s.sendMethodResponse(conn, 0, 10, 51, &protocol.ConnectionCloseOKMethod{})
}
```

- **`src/amqp-go/server/server.go` : 400-411** — processFrames only sets conn.Closed and returns on a non-nil error. Since the ConnectionClose handler returns nil, the loop continues and no teardown happens.

```go
pooled, err := s.processFrame(conn, frame)
...
if err != nil {
	...
	conn.Closed.Store(true)
	return
}
```

- **`src/amqp-go/server/server.go` : 321-339** — Teardown (socket close + Done signal) only runs after one of the goroutine done channels fires. On a client connection.close none of them fire, so this never executes until the client TCP-disconnects.

```go
select {
case <-readerDone: ...
case <-processorDone: ...
...
}
// Close the connection to unblock reader and processor
conn.Conn.Close()
close(conn.Done)
```

- **`src/amqp-go/server/server.go` : 354-370** — readFrames blocks in ReadFrameOptimized with no read deadline; grep confirms no SetReadDeadline exists anywhere. The connection only ends on client EOF, so it lingers after close-ok.

```go
for {
	frame, err := protocol.ReadFrameOptimized(conn.Conn)
	if err != nil {
		if err.Error() == "EOF" { ... }
		conn.Closed.Store(true)
		return
	}
```

- **`src/amqp-go/server/server.go` : 513-538** — Because conn.Closed stays false after client close-ok, the heartbeat sender keeps writing frames to a connection the client has closed — a protocol violation (server must not send frames after connection.close-ok).

```go
for {
	select {
	case <-ticker.C:
		if conn.Closed.Load() { ... return }
		// Send heartbeat frame
		...WriteFrameToConnection(conn, heartbeatFrame)
```

- **`src/amqp-go/server/connection_handlers.go` : 342-348** — The classMethod argument is never used; ClassID/MethodID are hardcoded to 0. Confirms the secondary claim.

```go
func (s *Server) sendConnectionClose(conn *protocol.Connection, replyCode uint16, replyText, classMethod string) error {
	closeMethod := &protocol.ConnectionCloseMethod{
		ReplyCode: replyCode,
		ReplyText: replyText,
		ClassID:   0, // Set based on classMethod if needed
		MethodID:  0, // Set based on classMethod if needed
	}
```

- **`src/amqp-go/server/connection_handlers.go` : 154-155** — Callers pass reply-text into replyText ("ACCESS_REFUSED") and human-readable detail into the classMethod slot, showing the parameter is both misused and ignored.

```go
s.sendConnectionClose(conn, amqperrors.AccessRefused, "ACCESS_REFUSED",
		"authorization enabled but no authenticated user")
```


### Fix

Make the client-initiated connection.close path perform full teardown, and fix sendConnectionClose to accept/emit the offending class/method.

1) In processConnectionMethod's ConnectionClose case (connection_handlers.go:328-332): after sending close-ok, mark the connection closed and signal teardown so all goroutines exit and the socket is closed. The cleanest approach that fits the existing architecture is to send close-ok, set conn.Closed.Store(true), then return a sentinel error (e.g. errConnectionClosedByClient) — but returning a plain error would log it as 'Error processing frame'. Better: send close-ok, set conn.Closed.Store(true), close(conn.Done) once (guarded), and close the socket so readFrames unblocks (its next ReadFrameOptimized returns EOF/closed and the reader exits, which fires readerDone and triggers the normal teardown in handleConnection). To avoid double-close races with handleConnection's own conn.Conn.Close()/close(conn.Done), use sync.Once for Done and guard the socket close (net.Conn.Close is idempotent for TCP and returns an error on second call which is fine, but wrap to ignore). Simplest robust implementation: return a distinguished error and have processFrames treat it as a clean close (log at Info, set conn.Closed, return) rather than an Error. 

Recommended concrete implementation: (a) define var errClientRequestedClose = errors.New("connection closed by client"); (b) ConnectionClose case sends close-ok then returns errClientRequestedClose; (c) in processFrames, when processFrame returns errClientRequestedClose, log at Info (not Error), set conn.Closed.Store(true), and return — which closes processorDone, fires the select in handleConnection, and runs conn.Conn.Close()+close(conn.Done) normally. This reuses the existing teardown and stops the heartbeat sender (it will see conn.Closed==true). Ensure the heartbeat sender/other write paths stop before or right when teardown runs; setting conn.Closed before returning guarantees the heartbeat sender's next tick returns without writing.

2) Fix sendConnectionClose (connection_handlers.go:342-364): change the signature to take classID/methodID uint16 (or parse classMethod), set ConnectionCloseMethod.ClassID/MethodID accordingly. Since existing callers all pass 0 offending class/method (connection-level refusals), update them to pass 0,0 explicitly, and pass the detail text as ReplyText. Also correct the swapped arguments at call sites: today they pass a symbolic string ("ACCESS_REFUSED") as replyText and the human message as classMethod; standardize on replyText carrying the descriptive message.

**Files to change:**
- `src/amqp-go/server/connection_handlers.go` — In processConnectionMethod ConnectionClose case: after s.sendConnectionCloseOK(conn), set conn.Closed.Store(true) and return a sentinel errClientRequestedClose (define it as a package-level var). Change sendConnectionClose signature from (conn, replyCode, replyText, classMethod string) to (conn, replyCode uint16, replyText string, classID, methodID uint16), populate ClassID/MethodID from the new args, and fix the confused replyText/classMethod call-site arguments.
- `src/amqp-go/server/server.go` — In processFrames, when processFrame returns errClientRequestedClose, log at Info level 'Connection closed by client (connection.close)', set conn.Closed.Store(true) and return (do not log as an error). This triggers the existing teardown in handleConnection which closes the socket and Done channel and stops all goroutines including the heartbeat sender.

**Code sketch:**

```go
// connection_handlers.go
var errClientRequestedClose = errors.New("connection closed by client")

case protocol.ConnectionClose:
	s.Log.Info("Received connection.close", zap.String("connection_id", conn.ID))
	if err := s.sendConnectionCloseOK(conn); err != nil {
		return err
	}
	conn.Closed.Store(true)
	return errClientRequestedClose

// server.go (processFrames)
pooled, err := s.processFrame(conn, frame)
if pooled { protocol.PutFrame(frame) }
if err != nil {
	if errors.Is(err, errClientRequestedClose) {
		s.Log.Info("Connection closed by client (connection.close)", zap.String("connection_id", conn.ID))
	} else {
		s.Log.Error("Error processing frame", ...)
	}
	conn.Closed.Store(true)
	return
}

// connection_handlers.go sendConnectionClose
func (s *Server) sendConnectionClose(conn *protocol.Connection, replyCode uint16, replyText string, classID, methodID uint16) error {
	closeMethod := &protocol.ConnectionCloseMethod{ReplyCode: replyCode, ReplyText: replyText, ClassID: classID, MethodID: methodID}
	...
}
```


**Test (TDD):** Add server/connection_close_test.go (TDD). Test 1 (behavioral, drives the real handler): create a Server, use net.Pipe() as conn.Conn, complete the handshake or invoke processConnectionMethod(conn, protocol.ConnectionClose, payload) directly; assert (a) a connection.close-ok frame (class 10, method 51) is written, (b) conn.Closed.Load() == true, and (c) the returned error is errClientRequestedClose. Test 2 (teardown/heartbeat): start the full goroutine set (or a minimal harness) with a short heartbeat interval, send connection.close from the client side of net.Pipe, then assert within a timeout that the server closes the socket (client read returns EOF) and that NO further heartbeat frames are written after close-ok. Test 3 (classMethod): call sendConnectionClose(conn, 406, "PRECONDITION_FAILED", 60, 40) and decode the written frame, asserting ClassID==60 and MethodID==40 instead of 0/0. Confirm all fail before the fix (Test 1 currently returns nil and conn.Closed stays false; Test 3 currently yields 0/0).

**Regression risk:** Low-to-moderate. Returning a sentinel error from the ConnectionClose path changes control flow: must ensure processFrames distinguishes it from real errors (errors.Is) so it is not logged as an error and so teardown is the clean path. The existing handleConnection teardown already handles socket close + close(conn.Done) + draining AckQueue, so reusing it is safe, but verify there is no double close(conn.Done): setting conn.Closed and returning from processFrames fires processorDone; handleConnection then closes the socket and Done exactly once — do not also close Done inside the handler. Changing sendConnectionClose's signature touches ~7 call sites (all in connection_handlers.go); update them together and re-run go vet/build. The argument-swap fix (replyText vs detail) changes user-visible close reason strings — cosmetic, low risk.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether the connection truly 'lingers forever': a well-behaved client that closes its TCP socket immediately after sending connection.close still triggers EOF in readFrames (server.go:357-369), which sets conn.Closed and fires readerDone, driving normal teardown. So the unbounded-linger and post-close-ok-heartbeat scenarios require a non-standard client that sends connection.close but keeps the socket open. This bounds real-world impact and is why MEDIUM (not higher) is correct — the colleague's severity holds.
- Challenged the heartbeat-after-close-ok race window: since the sender only checks conn.Closed on a 5s tick, could it emit a heartbeat between close-ok and teardown? Verified handleConnection calls conn.Conn.Close() promptly once processorDone fires (server.go:336), so the socket is gone within microseconds and the sender's next tick either sees Closed==true or hits a write error — it will not emit stray heartbeats. Setting conn.Closed before returning from the handler further guarantees this. Concern resolved.
- Challenged the fix for a double-close(conn.Done) race the colleague themselves flagged: grep confirms close(conn.Done) exists exactly once in non-test code (server.go:339). The fix routes through the existing single teardown and does NOT close Done in the handler, so there is no double-close. Concern is fully addressed by the sketch as written.
- Scrutinized the secondary 'confused parameter' claim rather than trusting it: confirmed all 8 sendConnectionClose call sites pass a symbolic code (e.g. "ACCESS_REFUSED") as the replyText param and the human-readable detail as the ignored classMethod param, so the AMQP reply-text on the wire carries only a symbolic name and the descriptive text is discarded. The fix's call-site cleanup (move detail into replyText, pass 0,0 for offending class/method) is correct.

**Fix concerns:**
- The sendConnectionClose signature change is a breaking internal API change touching 8 call sites; all must be updated in the same commit and go vet/build re-run, or compilation breaks. The colleague noted this (~7 sites; actual is 8).
- The argument-swap correction changes user-visible close-reason strings sent to clients (reply-text will now carry the descriptive message instead of the symbolic name). Cosmetic and arguably more correct, but any test or client asserting the old "ACCESS_REFUSED" literal reply-text would need updating.
- The fix addresses the client-close teardown but does NOT add a read/heartbeat timeout (SetReadDeadline). A client that opens a socket and never sends anything (or stops sending after handshake) still leaks a connection indefinitely — the same missing-read-deadline root cause noted in proof item 5. That is a separate, broader hardening gap the fix intentionally leaves out of scope; worth tracking as a follow-up.
- Minor: setting conn.Closed.Store(true) both in the ConnectionClose handler and again in processFrames' errClientRequestedClose branch is redundant (harmless, idempotent) — either site alone suffices.

**Agreed resolution (authoritative):**

Confirmed real bug at MEDIUM. Every load-bearing fact verified in source: (1) connection_handlers.go:328-332 — the ConnectionClose case only does `return s.sendConnectionCloseOK(conn)`; on success that returns nil. (2) frame_helpers.go:33-36 — sendConnectionCloseOK routes through sendMethodResponse and writes the frame ONLY; unlike sendConnectionClose (connection_handlers.go:362, which calls conn.Closed.Store(true)) it never marks the connection closed. So after a client connection.close, conn.Closed stays false. (3) server.go:400-411 — processFrames sets conn.Closed and returns ONLY on a non-nil error; with nil it loops. (4) server.go:321-346 — teardown (conn.Conn.Close() + close(conn.Done)) runs only after one of the goroutine done channels fires, which never happens on a client close. (5) server.go:354-370 — readFrames blocks in ReadFrameOptimized; grep confirms NO SetReadDeadline anywhere in non-test code, so the reader only unblocks on client EOF. (6) server.go:503-548 — the heartbeat sender checks conn.Closed only on its 5s tick and keeps calling WriteFrameToConnection while conn.Closed is false, so it will emit heartbeat frames after close-ok — an AMQP 0.9.1 violation (server must send no frames after connection.close-ok). Net effect: per-connection goroutines (reader/processor/ack/heartbeat/consumer-delivery), FrameQueue/AckQueue, WriteMutex and consumer registrations linger until the client TCP-disconnects. Severity stays MEDIUM (not higher) because the leak is bounded by client behavior — a client that closes its socket right after close-ok triggers EOF and normal teardown promptly; the unbounded-linger case requires a client that sends connection.close but holds the socket open, which is non-standard. Secondary claim also confirmed: sendConnectionClose (connection_handlers.go:342-348) hardcodes ClassID/MethodID=0 and never uses its classMethod arg; all 8 call sites (lines 154,162,185,196,211,250,276,286) pass a symbolic code string like "ACCESS_REFUSED" as replyText and the actual human-readable detail as the ignored classMethod arg — so the descriptive text is dropped from the wire and reply-text carries only a symbolic name. FIX VERDICT: sound. The recommended approach (send close-ok, set conn.Closed.Store(true), return sentinel errClientRequestedClose; processFrames uses errors.Is to log at Info and return, closing processorDone which drives the existing single teardown) correctly reuses handleConnection's teardown. Verified close(conn.Done) exists exactly once (server.go:339), so routing through processorDone introduces NO double-close of Done — the sketch correctly does NOT close Done itself. Ordering is correct: close-ok is written before conn.Closed is set, and handleConnection calls conn.Conn.Close() promptly after processorDone fires, so the heartbeat sender's next tick sees Closed==true (or the socket is already closed) and cannot write further frames. The sendConnectionClose signature change touches 8 call sites (all in connection_handlers.go, all connection-level refusals passing 0/0 offending class/method) — pass 0,0 explicitly and move the descriptive text into replyText. Fix is small, low-to-moderate regression risk, and should ship with the described TDD tests (assert close-ok frame class 10/method 51 written, conn.Closed==true, sentinel returned, no post-close-ok heartbeats, and ClassID/MethodID round-trip).

---

## [I24] Sparse global delivery tags inflate QueueState.Depth() (early backpressure) and force O(gap) tag walks in Claim/GetMessageForGet for mixed/interleaved workloads

**Rank 18 · P2** · Severity: 🟡 Medium · Category: performance · Effort: medium

### Impact
Delivery tags come from a single broker-wide counter (globalDeliveryTag). Each queue's head/tail/minAckCursor therefore live in the GLOBAL tag space, so any queue that receives only a fraction of total traffic (normal for direct/topic routing and mixed multi-queue workloads) sees its tags as a sparse subset of the global sequence. Two concrete consequences: (1) Backpressure trips far too early. Depth() = head - minAckCursor measures the span between the highest and lowest-unacked GLOBAL tag, not the count of messages actually queued. Empirically, 5 real messages at interleaved tags {0,2,4,6,8} report Depth()=9 (measured 80% inflation). With K co-active queues sharing the counter, inflation is ~Kx, so WaitForCapacity/AtHighWaterMark (default WM = ringSize*80% ~ 209K) fires at roughly WM/K real messages. Publishers to lightly-loaded queues get throttled/spilled prematurely, hurting throughput and defeating the intended coordination with the ring-buffer spill threshold. (2) O(gap) work per delivered message. tail is only advanced one step at a time by Claim's CAS. For a queue whose next real message is global tag T while its previous was P, Claim returns every tag P+1..T-1 as a gap; each gap drives a wasted storage.GetMessage (miss) + AckAdvance in deliverMessage plus a full poll-loop iteration. Empirically, delivering 2 real messages at tags {5,12} made Claim return all 13 slots 0..12. Cost per delivered message scales with the number of messages routed to OTHER queues since this queue's last message. GetMessageForGet (basic.get) has the identical walk at storage_broker.go:1457-1475. This is not data loss/mis-ordering (gaps are skipped safely and accounting is preserved), which is why it is medium not high, but it degrades throughput and backpressure fidelity under exactly the fanout/mixed workloads the claim names.

### Root cause
Per-queue dispatch state (QueueState.head/tail/minAckCursor) is expressed in the broker-global tag space produced by b.globalDeliveryTag.Add(1) in PublishMessage (storage_broker.go:843), rather than a per-queue local sequence or a direct message counter. Consequently: Depth() = head - minAckCursor (queue_dispatch.go:249-256) counts global-tag span, not real messages; and Claim (queue_dispatch.go:125-141) / GetMessageForGet (storage_broker.go:1457-1475) walk that span one tag at a time, skipping gap tags via the GetMessage-miss + AckAdvance path in deliverMessage (storage_broker.go:209-222). The two independent minAckCursor writers (AckAdvance at queue_dispatch.go:212-232 vs SetMinAckCursor(GetMinAckCursor()) at storage_broker.go:1217/1240) are both monotonic-forward and do not corrupt each other, but neither addresses the underlying global-tag-space accounting, and AckCursor.head is set to lastTag (ack_cursor.go:29-33) while QueueState.head is lastTag+1, so their drained values differ by one — a maintainability wart, not a separate correctness bug.

### Proof

- **`src/amqp-go/broker/storage_broker.go` : 54, 843, 849-855, 866** — Tags are a single global monotonic counter shared by every queue. Each queue's Publish(msgID) sets its head from a global value, so a queue receiving a fraction of traffic has sparse tags in its head/tail range — the premise of the issue.

```go
globalDeliveryTag atomic.Uint64 // Global counter for unique delivery tags across all queues
...
msgID := b.globalDeliveryTag.Add(1)
...
message.DeliveryTag = msgID / msgCopy.DeliveryTag = msgID
...
queueState.Publish(msgID)
```

- **`src/amqp-go/broker/queue_dispatch.go` : 249-256, 262-268** — Depth is head-minAckCursor, i.e. the global-tag span, not the message count. AtHighWaterMark/WaitForCapacity gate publishers on this inflated value. Verified empirically: 5 msgs at tags {0,2,4,6,8} -> Depth()=9.

```go
func (qs *QueueState) Depth() uint64 { h := qs.head.Load(); c := qs.minAckCursor.Load(); if h <= c { return 0 }; return h - c }
...
func (qs *QueueState) AtHighWaterMark() bool { wm := qs.depthHighWM.Load(); if wm == 0 { return false }; return qs.Depth() >= wm }
```

- **`src/amqp-go/broker/queue_dispatch.go` : 121-141** — Claim advances tail exactly one tag per call and returns each tag — including gap tags that belong to other queues. Verified empirically: reaching 2 real messages at {5,12} returned all 13 slots 0..12.

```go
for { t := qs.tail.Load(); h := qs.head.Load(); if t < h { if qs.tail.CompareAndSwap(t, t+1) { return t, true } ... }
```

- **`src/amqp-go/broker/storage_broker.go` : 202-222** — For every gap tag Claim returns, deliverMessage does a wasted GetMessage (miss) plus AckAdvance and re-loops — O(gap) storage lookups and poll-loop iterations per delivered message.

```go
message, err := b.storage.GetMessage(state.queueName, msgID)
if err != nil { queueState.DeleteInflight(msgID); b.deliveryIndex.Delete(msgID); queueState.AckAdvance(msgID); ... return }
```

- **`src/amqp-go/broker/storage_broker.go` : 1457-1475** — basic.get has the same one-tag-at-a-time walk over sparse gaps with per-gap GetMessage misses.

```go
for { t := queueState.tail.Load(); h := queueState.head.Load(); if t >= h { break }; if !queueState.tail.CompareAndSwap(t, t+1) { continue }; tag = t; msg, err := b.storage.GetMessage(queueName, tag); if err != nil { queueState.AckAdvance(tag); continue } ... }
```

- **`src/amqp-go/broker/storage_broker.go` : 1217, 1240, and queue_dispatch.go:212-232 / 270-280** — Two independent writers to minAckCursor. Both are monotonic-forward (SetMinAckCursor only advances; AckAdvance stores head which only grows or CAS-advances), so they do not corrupt each other. AckCursor.head is lastTag (ack_cursor.go:29-33) vs QueueState.head lastTag+1, so drained values differ by 1 — benign but confusing. This is a maintainability concern, not a distinct correctness bug.

```go
queueState.SetMinAckCursor(b.storage.GetMinAckCursor(state.queueName))  // storage-derived writer
... AckAdvance: if remaining==0 && requeueCount<=0 { qs.minAckCursor.Store(qs.head.Load()) } else if tag==min { CAS(tag,tag+1) }  // second writer
```

- **`src/amqp-go/broker/queue_dispatch_test.go` : 425-461, 463-498** — All existing Depth/dispatch tests publish CONTIGUOUS tags starting at 0 within a single QueueState. None exercises sparse/interleaved global tags, so the inflation and gap-walk are entirely untested — confirms the claim's 'Untested'.

```go
for i := uint64(0); i < 5; i++ { qs.Publish(i) } ... if d := qs.Depth(); d != 5 { ... }
```

- **`src/amqp-go/storage/disruptor_storage.go` : 401-407** — A per-queue real message count already exists (ring.ring.Count()), immune to tag sparseness — the basis for the recommended fix (Depth = waiting + inflight).

```go
func (ds *DisruptorStorage) GetQueueMessageCount(queueName string) (int, error) { ring := ds.getQueueRing(queueName); ... return int(ring.ring.Count()), nil }
```


### Fix

Redefine queue depth so backpressure and depth reflect the true number of outstanding (waiting + in-flight) messages for THAT queue, independent of the global-tag span. Recommended minimal-risk change: keep the tag-based head/tail machinery (it drives FIFO claim ordering) but replace the Depth() metric. Two viable options:

OPTION A (preferred, smallest surface): compute Depth() from already-accurate per-queue counters instead of the global-tag span. Add a broker-level Depth that returns storage.GetQueueMessageCount(queue) + queueState.InflightCount(). Since QueueState.Depth() is a pure QueueState method with no storage handle, either (a) inject the waiting count, or (b) maintain a dedicated waiting counter inside QueueState. Because gap tags are Publish'd only for real messages but AckAdvance is also called for gap tags (deliverMessage GetMessage-miss path), a naive Publish++/AckAdvance-- counter would over-decrement. Instead, decrement the waiting counter only for REAL messages: increment qs.waiting in Publish(); decrement it at the exact points a real stored message is removed (successful ack in AcknowledgeMessage, reject-drop, nack-drop, and basic.get ack) and on Requeue keep it (message still resident). Do NOT decrement on the gap-skip AckAdvance path. Then Depth() = waiting (net waiting) + inflight. This keeps Depth O(1), sparseness-immune, and per-queue.

OPTION B (cleaner long-term, larger): give each QueueState a per-queue local sequence for head/tail so tags are dense per queue, and translate to global DeliveryTag only at store time. This removes BOTH the Depth inflation AND the O(gap) Claim/GetMessageForGet walk, but touches recovery, AckCursor wraparound safety, and storage keying — higher risk.

For the O(gap) walk specifically (only fully removed by Option B), a lower-risk partial mitigation is to have Publish record the actual claimed tags in a compact per-queue structure (e.g. the requeue-style ring already used) so Claim pops real tags instead of scanning; but given the existing ring-buffer storage keys on global tag, the pragmatic fix for this validation is Option A for the depth/backpressure inflation (the higher-impact half) and a follow-up ticket for the walk.

Concretely implement Option A: add `waiting atomic.Int64` to QueueState; `Publish` does `qs.waiting.Add(1)`; add a `RealAckAdvance(tag)` (or a bool param on AckAdvance distinguishing gap-skips) that decrements waiting only for real messages; wire the real-ack call sites (AcknowledgeMessage, RejectMessage drop, NacknowledgeMessage drop, AcknowledgeGetDelivery) to decrement, leave gap-skip AckAdvance (deliverMessage:217, GetMessageForGet:1470) as non-decrementing; change Depth() to `w := qs.waiting.Load(); i := qs.inflight.Load(); if w+i < 0 {return 0}; return uint64(w+i)`. Requeue must NOT touch waiting (message stays resident) but it already moves the message from inflight back to waiting-equivalent — since inflight is decremented in Requeue and the tag stays claimable, add qs.waiting.Add(1) in Requeue to keep the invariant waiting+inflight == resident+in-flight. Verify the invariant with the concurrency test below.

**Files to change:**
- `src/amqp-go/broker/queue_dispatch.go` — Add `waiting atomic.Int64` field; increment in Publish and Requeue; add a real-ack decrement path (new method or bool param) that is NOT taken by the gap-skip AckAdvance; rewrite Depth() to return waiting+inflight clamped at 0. Keep head/tail/minAckCursor for FIFO ordering only (Depth no longer reads them).
- `src/amqp-go/broker/storage_broker.go` — Route the four REAL message-removal sites to the decrementing path: AcknowledgeMessage single (1245) and multiple (1212), RejectMessage drop branch (1290), NacknowledgeMessage single/multiple drop branches (1334/1365), and AcknowledgeGetDelivery. Leave the gap-skip AckAdvance calls (deliverMessage:217, GetMessageForGet:1470) as non-decrementing. Optionally drop the redundant second minAckCursor writer (SetMinAckCursor(GetMinAckCursor())) once Depth no longer depends on minAckCursor.
- `src/amqp-go/broker/queue_dispatch_test.go` — Add sparse-tag depth tests and the invariant concurrency test described below.

**Code sketch:**

```go
// queue_dispatch.go
type QueueState struct { /* ... */ waiting atomic.Int64 }

func (qs *QueueState) Publish(tag uint64) { /* existing head CAS */ qs.waiting.Add(1); qs.NotifyNewMessage() }

// gap-skip path keeps calling AckAdvance (no waiting decrement)
// real removal path calls this:
func (qs *QueueState) AckReal(tag uint64) { qs.waiting.Add(-1); qs.AckAdvance(tag) }

func (qs *QueueState) Requeue(tag uint64) { /* existing */ qs.inflight.Add(-1); qs.waiting.Add(1); /* ring push */ }

func (qs *QueueState) Depth() uint64 {
    d := qs.waiting.Load() + qs.inflight.Load()
    if d < 0 { return 0 }
    return uint64(d)
}
// NOTE: on Claim, move from waiting->inflight (waiting-- happens where ClaimInflight++ happens) OR
// define waiting as 'published-but-not-yet-terminally-removed' and let inflight overlap be handled by
// counting resident==waiting and in-flight separately without double counting. Pick ONE definition and
// assert it in the concurrency test.
```


**Test (TDD):** TDD, three tests in queue_dispatch_test.go: (1) TestDispatch_SparseDepthNotInflated: qs := NewQueueState(0); Publish(0);Publish(2);Publish(4);Publish(6);Publish(8) (5 real msgs at sparse global tags); assert Depth()==5 (currently returns 9, proving the bug). (2) TestDispatch_SparseDepthAfterClaimAndAck: Publish the 5 sparse tags, Claim+ClaimInflight all 5, ack the 5 real tags via the real-ack path, and also Claim the gap tags (1,3,5,7) driving gap-skip AckAdvance; assert Depth()==0 and never exceeds 5 at any point. (3) TestDispatch_DepthInvariantConcurrent: extend TestDispatch_ConcurrentPublishClaimRequeue to assert that at quiescence Depth()==0 and that Depth() never exceeds the number of outstanding (published-minus-terminally-removed) real messages, using an atomic oracle counter maintained by the test. All must fail before the fix (test 1 definitely does — verified Depth()=9) and pass after.

**Regression risk:** Medium. Depth() feeds publisher backpressure (WaitForCapacity/AtHighWaterMark) and the storage-spill coordination, so an off-by-one or missed decrement could either wedge publishers (never-draining depth) or disable backpressure (depth stuck at 0). The counter must be decremented on EXACTLY the terminal-removal paths and incremented on Publish/Requeue, with the gap-skip path deliberately excluded — get this wrong and depth drifts. The chosen 'waiting vs inflight' definition must avoid double counting between Claim (waiting->inflight) and ack (inflight->removed). Mitigate with the concurrency invariant test (test 3) run under -race and with the existing TestDispatch_ConcurrentPublishClaimRequeue_Race. Removing the second minAckCursor writer is optional and should be a separate commit. Recovery (Recover) must also initialize waiting to the recovered message count, not (maxTag-minTag).

### Debate & agreed resolution

Investigator verdict: `confirmed-severity-adjusted` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- placeholder

**Fix concerns:**
- Core formula contradiction: sketch defines Depth()=waiting+inflight but AckReal decrements both waiting and inflight while Claim/ClaimInflight increments only inflight — this double-counts (or under-counts) claimed-but-unacked messages and would cause depth drift. Must pin one convention: decrement waiting at ClaimInflight (waiting to inflight transition) and have real-ack decrement only inflight via existing AckAdvance (do NOT add waiting-- in the ack path). The sketch's own NOTE admits this is unresolved.
- Recover (queue_dispatch.go:234-247) must initialize the new waiting counter to the true recovered message count, since recovered messages bypass Publish (RecoverQueue calls Recover directly). Flagged in the fix text but not implemented in the code sketch; without it, post-recovery Depth re-inflates to maxTag+1-minTag.
- The O(gap) Claim/GetMessageForGet walk (second half of the issue) is explicitly deferred to a follow-up. The fix only remediates the depth/backpressure inflation. This is acceptable for medium but the fix should not be described as fully resolving the candidate.
- Regression risk medium and asymmetric: a missed decrement leaves Depth permanently high and wedges publishers in WaitForCapacity; an over-decrement clamps Depth to 0 and disables backpressure/spill coordination entirely. Must be gated by the proposed invariant concurrency test run under -race, plus the existing TestDispatch_ConcurrentPublishClaimRequeue_Race.
- Removing the redundant second minAckCursor writer (SetMinAckCursor(GetMinAckCursor())) should be a strictly separate commit and is only safe once Depth no longer reads minAckCursor; do not bundle it, as minAckCursor may still be consulted elsewhere (AckCursor wraparound safety in ack_cursor.go).

**Agreed resolution (authoritative):**

Real performance bug, severity medium, confirmed by reading source and by empirical reproduction. Root cause verified: delivery tags come from a single broker-wide counter b.globalDeliveryTag.Add(1) (storage_broker.go:843) and each queue's Publish(msgID) advances head to msgID+1 (queue_dispatch.go:76-90), so per-queue head/tail/minAckCursor live in the sparse global tag space. Two consequences confirmed empirically: (1) Depth() = head - minAckCursor (queue_dispatch.go:249-256) measures global-tag span, not real messages — I ran a scratch test and got Depth()=9 for 5 real messages at sparse tags {0,2,4,6,8}. This feeds AtHighWaterMark/WaitForCapacity (queue_dispatch.go:44-73,262-268) so publishers to lightly-loaded queues in a K-queue mixed workload throttle at ~WM/K real messages (default WM = ringSize*80% ~ 209K via computeDepthHighWM, storage_broker.go:157-167). (2) Claim (queue_dispatch.go:121-141) and the identical walk in GetMessageForGet (storage_broker.go:1457-1475) advance tail one tag at a time and return gap tags; I confirmed Claim returns all 13 slots 0..12 to reach 2 real messages at {5,12}. Each gap drives a wasted storage.GetMessage miss + AckAdvance in deliverMessage (storage_broker.go:202-222). Not data loss or mis-ordering — gaps are skipped safely (inflight ++/-- balances, underflow-guarded) and FIFO is preserved — which is why it is medium not high. The two minAckCursor writers (AckAdvance vs SetMinAckCursor(GetMinAckCursor())) are both monotonic-forward and do not corrupt each other, and the AckCursor.head=lastTag (ack_cursor.go:29-33) vs QueueState.head=lastTag+1 off-by-one is cosmetic — agreed, both benign.

FIX VERDICT: needs-revision. The proposed direction (Option A: replace the global-span Depth with a per-queue outstanding-message count, keeping the tag machinery for FIFO) is sound and low-risk, and Depth() has NO external/metrics consumers (only AtHighWaterMark), so the blast radius is confined to backpressure — good. BUT the code sketch as written contains an unresolved internal contradiction that would cause depth drift: it defines Depth() = waiting + inflight yet has AckReal decrement BOTH waiting and inflight, while Claim/ClaimInflight increments inflight WITHOUT decrementing waiting — so a claimed-but-unacked message is either double-counted (if waiting is not decremented at claim) or under-counted after ack. The sketch's own NOTE admits this convention is unresolved. Correct resolution agreed: decrement waiting exactly at the ClaimInflight (waiting to inflight) transition; leave the real-ack path decrementing only inflight via the existing AckAdvance (NOT waiting, contradicting the sketch's AckReal); keep Requeue's inflight--/waiting++; and the gap-skip AckAdvance (deliverMessage:217, GetMessageForGet:1470) must not touch waiting (correct in the sketch, since gaps never got a Publish waiting++). Additional required, currently only flagged-not-coded: Recover (queue_dispatch.go:234-247) must initialize waiting to the true recovered message count (recovered messages bypass Publish via RecoverQueue), else post-recovery Depth is re-inflated to maxTag+1-minTag. The fix also explicitly scopes out the O(gap) Claim/GetMessageForGet walk (deferred to a follow-up ticket / Option B) — acceptable for a medium perf issue but means the fix only addresses the depth/backpressure half. Regression risk is genuinely medium (a missed decrement wedges publishers forever; an over-decrement disables backpressure) and MUST be gated by the proposed concurrency invariant test under -race. Once the waiting/inflight convention is pinned to one definition and Recover initialization is added, the approach is correct and complete for the depth half.

---

## [I12] Advertised capabilities connection.blocked and consumer_cancel_notify are never emitted on the wire

**Rank 19 · P2** · Severity: 🟡 Medium · Category: protocol-compliance · Effort: large · ⚡ Quick win

> **Severity adjusted by debate:** claimed `high` → agreed `medium`.

### Impact
The server's connection.start advertises capabilities.connection.blocked=true and capabilities.consumer_cancel_notify=true, but neither method is ever sent to the client. (1) consumer_cancel_notify: When a queue is deleted (by another connection, admin, or auto-delete/exclusive teardown) while a consumer is subscribed, the broker silently tears down consumer state server-side (storage_broker.DeleteQueue closes stopCh, deletes from activeConsumers) but never sends the server-initiated basic.cancel (60.30). RabbitMQ-compatible clients (Java client's Consumer.handleCancel, Go amqp091 NotifyCancel, .NET ConsumerCancelled) register handlers based on the advertised capability and expect this notification; instead the consumer object hangs forever believing it is still active, never gets recreated, and message flow silently stops. This is exactly the failure mode the capability exists to prevent. (2) connection.blocked: conn.Blocked back-pressure state is computed and logged (basic_handlers.go:119-129) but connection.blocked/connection.unblocked frames are never emitted, so publishers advertised the capability get no flow-control signal. This half is lower impact because the server intentionally keeps publishing to preserve connection stability, but it is still a false advertisement.

### Root cause
The capability map in sendConnectionStart was populated aspirationally without implementing the corresponding wire behavior. There are no protocol method definitions for connection.blocked (10.60), connection.unblocked (10.61), and no server-initiated path that serializes and writes basic.cancel (60.30). The consumer/queue teardown code (handleBasicCancel, queue_handlers.handleQueueDelete, storage_broker.DeleteQueue) only handles client-driven cancellation and internal state cleanup; it never notifies still-attached consumers when the queue disappears out from under them.

### Proof

- **`src/amqp-go/server/connection_handlers.go` : 21-28** — Both capabilities are advertised true to every connecting client during the handshake.

```go
"capabilities": map[string]interface{}{
  "publisher_confirms":           true,
  "exchange_exchange_bindings":   true,
  "basic.nack":                   true,
  "consumer_cancel_notify":       true,
  "connection.blocked":           true,
  "authentication_failure_close": true,
},
```

- **`src/amqp-go/server/basic_handlers.go` : 119-129** — conn.Blocked is only read and logged; no connection.blocked frame is ever sent to the client. There is no code path that emits connection.blocked/unblocked.

```go
blocked := conn.Blocked.Load()
if blocked {
  s.Log.Warn("High memory usage detected but allowing publish to maintain connection stability", ...)
  // Continue processing instead of returning error
}
```

- **`src/amqp-go/protocol/methods.go` : 10-19** — The connection class method constants stop at 51. There is no ConnectionBlocked (60) or ConnectionUnblocked (61) constant, no struct, and no Serialize/Deserialize for them anywhere in the protocol package.

```go
ConnectionStart=10 ... ConnectionClose=50 ConnectionCloseOK=51
```

- **`src/amqp-go/server/basic_handlers.go` : 481-546** — The only basic.cancel handling is client->server: it consumes an incoming cancel and replies with basic.cancel-ok. There is no server-initiated basic.cancel.

```go
func (s *Server) handleBasicCancel(conn *protocol.Connection, channelID uint16, payload []byte) error { ... err = s.sendBasicCancelOK(conn, channelID, cancelMethod.ConsumerTag) ... }
```

- **`src/amqp-go/server/queue_handlers.go` : 346-371** — queue.delete deletes the queue and replies queue.delete-ok, but never sends basic.cancel to consumers of that queue. Consumers attached from other channels/connections are not notified.

```go
err = s.Broker.DeleteQueue(queueName, deleteMethod.IfUnused, deleteMethod.IfEmpty)
... 
return s.sendQueueDeleteOK(conn, channelID, 0)
```

- **`src/amqp-go/broker/storage_broker.go` : 436-470** — On DeleteQueue the broker stops server-side delivery goroutines and removes consumer records, but performs no client notification. The client-side Consumer remains registered and hangs, which is precisely what consumer_cancel_notify is meant to avoid.

```go
for _, state := range consumers { if state.semCancel != nil { state.semCancel() } state.stopOnce.Do(func() { close(state.stopCh) }); b.activeConsumers.Delete(state.consumer.Tag) } ... for _, consumer := range consumers { b.storage.DeleteConsumer(name, consumer.Tag); b.activeConsumers.Delete(consumer.Tag) }
```

- **`src/amqp-go/server/frame_helpers.go` : 117-146** — The frame_helpers file has sendBasicCancelOK but no sendBasicCancel helper, confirming there is no way to emit a server-initiated basic.cancel.

```go
func (s *Server) sendBasicCancelOK(conn *protocol.Connection, channelID uint16, consumerTag string) error { ... }
```


### Fix

Implement the two advertised behaviors on the wire. Scope the fix to consumer_cancel_notify first (high impact) and connection.blocked second (medium impact), or split into two PRs.

PART A - consumer_cancel_notify (server-initiated basic.cancel 60.30):
1. Add a server->client frame helper sendBasicCancel(conn, channelID, consumerTag) in server/frame_helpers.go that serializes a BasicCancelMethod{ConsumerTag: tag, NoWait: true} and writes it as EncodeMethodFrame(60, 30, ...). (BasicCancelMethod.Serialize already exists in protocol/methods.go:1853, so no new protocol struct is needed - it is currently only used for the incoming direction, but the wire encoding is symmetric.)
2. When a queue is deleted while consumers are attached, notify each affected consumer's owning channel/connection. The cleanest place is at the server layer where the *protocol.Consumer (which carries Channel and thus the Connection) is reachable. Introduce a callback/notification path: have Broker.DeleteQueue (and any auto-delete/exclusive teardown) return the list of consumer tags that were attached to the deleted queue, OR expose a broker-level hook (e.g. an OnConsumerCancelled(consumerTag) callback registered by the server) that fires when the broker force-stops a consumer. In server/queue_handlers.go handleQueueDelete, after DeleteQueue succeeds, for each returned/notified consumerTag look up the owning channel via the server's consumer registry, remove it from channel.Consumers under channel.Mutex, and call sendBasicCancel on that consumer's connection+channel. This must handle the cross-connection case (the deleting connection is not necessarily the consuming one), so the server needs a consumerTag -> (conn, channelID) index. If such an index does not already exist, add one (a sync.Map on Server keyed by consumerTag, populated in handleBasicConsume and cleaned up in handleBasicCancel / channel close / connection close).
3. Ensure idempotency with existing teardown: handleBasicCancel and connection/channel close paths already UnregisterConsumer; the new notification must not double-send. Guard with a sync.Once or a presence check on the consumer index so a client-initiated cancel does not also trigger a server-initiated basic.cancel.

PART B - connection.blocked / connection.unblocked (10.60 / 10.61):
1. Add method constants ConnectionBlocked=60, ConnectionUnblocked=61 in protocol/methods.go, plus a ConnectionBlockedMethod{Reason string} with Serialize (Reason is a short string) and an empty ConnectionUnblocked body.
2. Add sendConnectionBlocked(conn, reason) and sendConnectionUnblocked(conn) helpers that EncodeMethodFrame(10, 60/61, ...). Only send if the client advertised the capability in its connection.start-ok client-properties (parse and store it on the Connection during handleConnectionStartOK); RabbitMQ requires the peer to have advertised connection.blocked before sending these.
3. At the point where conn.Blocked transitions false->true, emit connection.blocked once; on true->false emit connection.unblocked. Do this transition detection with a CompareAndSwap on conn.Blocked or a separate 'blockNotified' atomic so the frame is sent exactly once per transition, from wherever the back-pressure flag is set (search for conn.Blocked.Store callers).

Alternative minimal fix if full implementation is out of scope for this release: stop advertising capabilities you do not honor. Set consumer_cancel_notify and connection.blocked to false (or remove them) in sendConnectionStart so conformant clients do not register handlers that will never fire. This is a one-line-per-capability change and removes the false advertisement immediately, but it degrades interoperability (clients lose the safety net), so it should be a stopgap, not the target state.

**Files to change:**
- `src/amqp-go/server/frame_helpers.go` — Add sendBasicCancel (server-initiated 60.30) and, for Part B, sendConnectionBlocked/sendConnectionUnblocked helpers.
- `src/amqp-go/server/queue_handlers.go` — In handleQueueDelete, after DeleteQueue, notify all consumers of the deleted queue via sendBasicCancel and remove them from their owning channel.
- `src/amqp-go/server/basic_handlers.go` — In handleBasicConsume populate a consumerTag->owner index; in handleBasicCancel remove from it. Guard against double-notify with existing teardown.
- `src/amqp-go/server/server.go` — Add the consumerTag->owner index (sync.Map) on Server; clean it up in connection-close teardown (around line 180-198). Register a broker hook or consume DeleteQueue's returned tags.
- `src/amqp-go/broker/storage_broker.go` — DeleteQueue: return the list of consumer tags it force-stopped (or invoke a registered OnConsumerCancelled hook) so the server can emit basic.cancel. Also cover auto-delete/exclusive teardown paths.
- `src/amqp-go/server/unified_broker.go` — Extend the UnifiedBroker interface (and adapters in broker_adapters.go / builder.go) with the return value or hook needed to surface force-stopped consumer tags.
- `src/amqp-go/protocol/methods.go` — Part B: add ConnectionBlocked=60/ConnectionUnblocked=61 constants and ConnectionBlockedMethod struct with Serialize.
- `src/amqp-go/server/connection_handlers.go` — Part B: parse and store client-properties capabilities from connection.start-ok so blocked/unblocked are only sent to opted-in clients. If choosing the stopgap, flip the two advertised capabilities to false here instead.

**Code sketch:**

```go
// server/frame_helpers.go
func (s *Server) sendBasicCancel(conn *protocol.Connection, channelID uint16, consumerTag string) error {
    m := &protocol.BasicCancelMethod{ConsumerTag: consumerTag, NoWait: true}
    return s.sendMethodResponse(conn, channelID, 60, 30, m)
}

// server/queue_handlers.go (after DeleteQueue succeeds)
for _, tag := range affectedConsumerTags {
    if owner, ok := s.consumerIndex.Load(tag); ok {
        oc := owner.(*consumerOwner) // {conn *protocol.Connection, channelID uint16, chan *protocol.Channel}
        oc.chan.Mutex.Lock()
        if cns, ok := oc.chan.Consumers[tag]; ok { close(cns.Cancel); delete(oc.chan.Consumers, tag) }
        oc.chan.Mutex.Unlock()
        _ = s.sendBasicCancel(oc.conn, oc.channelID, tag) // 60.30 server-initiated
        s.consumerIndex.Delete(tag)
    }
}

// protocol/methods.go
const ( ConnectionBlocked = 60; ConnectionUnblocked = 61 )
type ConnectionBlockedMethod struct{ Reason string }
func (m *ConnectionBlockedMethod) Serialize() ([]byte, error) { return encodeShortString(m.Reason), nil }
```


**Test (TDD):** TDD in server package (mirroring existing integration tests like batch_delivery_test.go). Test 1 (consumer_cancel_notify): start the server, open a connection+channel, basic.consume on queue Q, then on a SECOND connection issue queue.delete Q; assert the first connection receives a basic.cancel (class 60 method 30) frame carrying the matching consumer tag within a timeout. Test 2 (idempotency): client sends basic.cancel for its own consumer; assert exactly one basic.cancel-ok and NO server-initiated basic.cancel is sent. Test 3 (Part B, optional): register a client that advertises connection.blocked, force conn.Blocked false->true, assert a connection.blocked (10.60) frame is received, then transition back and assert connection.unblocked (10.61); a client that did NOT advertise the capability receives neither. If choosing the stopgap, instead assert connection.start's serverProperties.capabilities has consumer_cancel_notify=false and connection.blocked=false.

**Regression risk:** Medium-to-high for the full fix. Sending a server-initiated basic.cancel touches the consumer teardown path which is concurrency-sensitive (channel.Mutex, broker per-queue mutex, activeConsumers) and has recent P0 fixes around double-requeue and connection-close races (see consumer_delivery.go:244 comment). A new consumerTag->owner index must be cleaned up in every teardown path (client cancel, channel.close, connection.close, queue.delete, auto-delete) or it leaks/double-sends. Cross-connection notification means writing to a connection other than the one processing the frame, so it must go through the WriteMutex-protected path (WriteFrameToConnection already locks) and must not deadlock if the target connection is concurrently closing. Extending the UnifiedBroker interface ripples through adapters and mocks. The stopgap (flip capabilities to false) is trivial/low-risk but only removes the false advertisement.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- see agreedResolution and challenges array

**Fix concerns:**
- Part B (connection.blocked/unblocked) as written assumes there is a conn.Blocked.Store transition to hook into, but conn.Blocked is never set anywhere in the tree - the fix must first implement the back-pressure trigger before any blocked/unblocked frame can ever fire. This is unstated extra scope; recommend deferring Part B.
- The consumerTag->owner index must be cleaned up in EVERY teardown path (client basic.cancel, channel.close, connection.close at server.go:181-198, queue.delete, auto-delete/exclusive) or it leaks and can double-send a server-initiated basic.cancel after a client already cancelled. The plan's sync.Once/presence-check guard is necessary, not optional.
- Cross-connection notification writes to a connection other than the one processing queue.delete; must go through WriteFrameToConnection's WriteMutex and must not deadlock against a concurrently-closing target connection, given the recent P0 races around consumer teardown (consumer_delivery.go).
- Extending the UnifiedBroker interface / adapters / builder to surface force-stopped consumer tags ripples through mocks and adapters; the auto-delete and exclusive-queue teardown paths (not just explicit queue.delete) must also be covered or the notification is incomplete.
- Recommend shipping the one-line-per-capability stopgap (set consumer_cancel_notify and connection.blocked to false in connection_handlers.go) first to remove the false advertisement at near-zero risk, then land Part A as a scoped follow-up; do the full implementation only if client interop with these safety nets is actually required.

**Agreed resolution (authoritative):**

Real protocol-compliance bug, confirmed by direct source reading. StrangeQ advertises capabilities.consumer_cancel_notify=true and connection.blocked=true in connection.start (server/connection_handlers.go:21-28) but neither is ever emitted on the wire. Verified: (1) frame_helpers.go has sendBasicCancelOK (line 117) but NO server-initiated sendBasicCancel; grep for any EncodeMethodFrame(60,30) emission or 'consumer_cancel_notify' shows the string appears only in the advertisement. (2) handleBasicCancel (basic_handlers.go:481-546) is client->server only. (3) broker/storage_broker.go DeleteQueue (436-480) force-stops consumers (semCancel, close(stopCh), activeConsumers.Delete, QueueState.Close) with zero client notification; queue_handlers.go handleQueueDelete (311-372) only sends queue.delete-ok. So a consumer on queue Q gets silently orphaned when Q is deleted out from under it (cross-connection, admin, or auto-delete teardown) - exactly the failure consumer_cancel_notify exists to prevent. (4) There are no ConnectionBlocked/ConnectionUnblocked constants, structs, or emit paths (methods.go connection consts stop at 51). AGREED FINAL POSITION: real bug, but severity is MEDIUM not HIGH. It is a graceful-degradation/false-advertisement gap - no data loss or corruption; consumer_cancel_notify only bites on out-of-band deletion of an actively-consumed queue (an uncommon operational event), and RabbitMQ-compatible clients degrade to a hung/stale consumer rather than crashing. The connection.blocked half is even weaker than the colleague stated: I verified conn.Blocked (protocol/structures.go:29) is NEVER .Store()d anywhere in the tree - the blocked branch at basic_handlers.go:119-129 is unreachable dead code - so it can never be true and there is no transition to signal. FIX NEEDS REVISION: Part A (server-initiated basic.cancel) is directionally correct and BasicCancelMethod.Serialize already exists (methods.go:1853) so no new struct is needed, but the plan understates two things - the consumerTag->owner index must be cleaned up in ALL teardown paths (server.go:181-198 conn/channel close, handleBasicCancel, handleBasicConsume registration, auto-delete/exclusive) or it leaks/double-sends, and cross-connection writes must go through WriteFrameToConnection's WriteMutex without deadlocking a concurrently-closing target (recent P0 races per consumer_delivery.go). Part B as written says 'search for conn.Blocked.Store callers' but there are none, so Part B must FIRST implement the back-pressure trigger before it can emit anything - larger scope than described, and given the dead flag it should be deferred. RECOMMENDED PATH: ship the stopgap now (flip both advertised capabilities to false in connection_handlers.go - one line each, low risk) to remove the false advertisement immediately; then implement Part A (consumer_cancel_notify) as a properly-scoped follow-up with the full teardown-path idempotency handling; defer Part B until back-pressure is actually wired.

---

## [I26] queue.declare-ok consumer-count and queue.delete-ok message-count hardcoded to 0

**Rank 20 · P2** · Severity: ⚪ Low · Category: protocol-compliance · Effort: small · ⚡ Quick win

### Impact
Two AMQP 0.9.1 response fields report inaccurate values to clients:

1. queue.delete-ok message-count is always 0, even when the deleted queue held N messages. A client that inspects the deleted-message count (e.g. amqp091-go returns it from Channel.QueueDelete; management/monitoring tooling that logs "purged N on delete") sees 0 regardless of actual contents. The correct count IS computed inside the broker (storage.PurgeQueue returns it) but is silently discarded.

2. queue.declare-ok consumer-count is always 0. For a fresh declare this is coincidentally correct, but for a re-declare (idempotent declare) of an existing queue that has active consumers, the spec/RabbitMQ return the live consumer count; StrangeQ returns 0. Clients that rely on declare to probe how many consumers are attached (a common pattern with passive=true) get wrong data.

No message loss, no crash, no protocol framing violation — the frames are well-formed, only the numeric payload is wrong. Hence low severity. No current test asserts these values, so the gap is silent.

### Root cause
The handlers pass literal constants instead of real values. handleQueueDeclare passes consumerCount=0 to sendQueueDeclareOK because neither protocol.Queue nor the UnifiedBroker interface exposes a per-queue consumer count. handleQueueDelete passes messageCount=0 to sendQueueDeleteOK because Broker.DeleteQueue's signature returns only error; inside StorageBroker.DeleteQueue the purge count from storage.PurgeQueue(name) is thrown away (return value ignored on the b.storage.PurgeQueue(name) call).

### Proof

- **`src/amqp-go/server/queue_handlers.go` : 132-135** — queue.declare-ok is sent with consumerCount hardcoded to the literal 0 (last argument). messageCount is real (queue.MessageCount.Load()), but consumer count is not queried at all.

```go
msgCount := uint32(queue.MessageCount.Load())
return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, 0)
```

- **`src/amqp-go/server/queue_handlers.go` : 370-371** — queue.delete-ok is sent with messageCount hardcoded to the literal 0, even though the queue may have contained messages that were just purged.

```go
// Send queue.delete-ok response with number of deleted messages
return s.sendQueueDeleteOK(conn, channelID, 0)
```

- **`src/amqp-go/broker/storage_broker.go` : 452-453** — The broker computes and then discards the deleted-message count: storage.PurgeQueue returns (int, error) (confirmed at storage_broker.go:1505-1521 and interfaces/storage.go), but here the count return value is dropped. DeleteQueue then returns only error, so the count can never reach the handler.

```go
// Delete all messages in the queue
b.storage.PurgeQueue(name)
```

- **`src/amqp-go/interfaces/broker.go` : 13** — The Broker interface DeleteQueue returns only error, so even if StorageBroker captured the count it cannot be surfaced without a signature change.

```go
DeleteQueue(name string, ifUnused, ifEmpty bool) error
```

- **`src/amqp-go/server/unified_broker.go` : 14** — The server-facing UnifiedBroker mirrors the error-only DeleteQueue signature and exposes no per-queue consumer-count accessor (only GetQueues/GetConsumers globally), so both fixes require interface plumbing.

```go
DeleteQueue(name string, ifUnused, ifEmpty bool) error
```

- **`src/amqp-go/server/frame_helpers.go` : 75-101** — The frame helpers already accept and serialize both fields correctly (QueueDeclareOKMethod.ConsumerCount and QueueDeleteOKMethod.MessageCount); the wire encoding is fine, so only the caller-supplied values are wrong.

```go
func (s *Server) sendQueueDeclareOK(... messageCount, consumerCount uint32) error {
  method := &protocol.QueueDeclareOKMethod{Queue: queueName, MessageCount: messageCount, ConsumerCount: consumerCount}
```


### Fix

Fix the delete-ok count (the higher-value, cleaner half) by threading the purged count out of the broker; optionally fix declare-ok consumer count with a lightweight accessor.

PART A — queue.delete-ok message-count (recommended, self-contained):
1. Change the DeleteQueue signature to return the deleted-message count. In interfaces/broker.go:13, server/unified_broker.go:14, server/builder.go:348 (brokerWrapper), server/broker_adapters.go:30 (StorageBrokerAdapter), and any test doubles (server/ack_worker_test.go:104) change `DeleteQueue(name string, ifUnused, ifEmpty bool) error` to `DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error)`.
2. In broker/storage_broker.go DeleteQueue, capture the purge count: `deleted, _ := b.storage.PurgeQueue(name)` at line 453, and return `deleted, b.storage.DeleteQueue(name)` at line 480. Update the early-return paths (line 409 `return 0, nil`; error returns to `return 0, err` / `return 0, fmt.Errorf(...)`). Note the ifEmpty branch already fetched the count via GetQueueMessageCount; you can reuse it, but simplest is to rely on the PurgeQueue return.
3. In server/queue_handlers.go handleQueueDelete, capture the count: `deleted, err := s.Broker.DeleteQueue(queueName, deleteMethod.IfUnused, deleteMethod.IfEmpty)` and change line 371 to `return s.sendQueueDeleteOK(conn, channelID, uint32(deleted))`.
4. Honor NoWait: queue.delete has a no-wait flag (QueueDeleteMethod.NoWait) — mirror the existing pattern used in handleQueuePurge (only send the ok when !NoWait). Check whether the current handler already gates on NoWait; if not, this is a good moment to add it, but keep it out of scope if you want a minimal diff.

PART B — queue.declare-ok consumer-count (optional, requires a new accessor):
1. Add a method to the broker interface(s), e.g. `GetQueueConsumerCount(name string) int`, implemented on StorageBroker using the existing storage.GetQueueConsumers(name) (already called internally at storage_broker.go:416/464) or the in-memory queueConsumers map (len of the []*ConsumerState slice). Add it to interfaces/broker.go, server/unified_broker.go, adapters, wrapper, and test doubles.
2. In handleQueueDeclare replace the literal 0: `consumerCount := uint32(s.Broker.GetQueueConsumerCount(queue.Name))` then `sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, consumerCount)`.
If Part B is deemed not worth the interface churn for a value that is 0 for the overwhelmingly common fresh-declare case, it is acceptable to ship only Part A and leave a code comment noting declare-ok consumer-count is best-effort 0.

**Files to change:**
- `src/amqp-go/interfaces/broker.go` — Change DeleteQueue to return (int, error); (Part B) add GetQueueConsumerCount(name string) int.
- `src/amqp-go/server/unified_broker.go` — Mirror the new DeleteQueue signature (int, error); (Part B) add GetQueueConsumerCount.
- `src/amqp-go/broker/storage_broker.go` — DeleteQueue: capture PurgeQueue count and return it; update all early/error returns to the two-value form; (Part B) implement GetQueueConsumerCount using GetQueueConsumers/queueConsumers.
- `src/amqp-go/server/broker_adapters.go` — StorageBrokerAdapter.DeleteQueue: propagate (int, error); (Part B) add GetQueueConsumerCount passthrough.
- `src/amqp-go/server/builder.go` — brokerWrapper.DeleteQueue (line 348): propagate (int, error); (Part B) add passthrough.
- `src/amqp-go/server/queue_handlers.go` — handleQueueDelete: capture deleted count, pass to sendQueueDeleteOK; (Part B) query consumer count in handleQueueDeclare and pass instead of literal 0.
- `src/amqp-go/server/ack_worker_test.go` — Update ackTestBroker.DeleteQueue signature to (int, error); add GetQueueConsumerCount stub if Part B.

**Code sketch:**

```go
// broker/storage_broker.go
func (b *StorageBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error) {
    if _, err := b.storage.GetQueue(name); err != nil {
        if errors.Is(err, interfaces.ErrQueueNotFound) { return 0, nil }
        return 0, err
    }
    // ... ifUnused / ifEmpty checks return 0, err ...
    deleted, _ := b.storage.PurgeQueue(name)   // was: b.storage.PurgeQueue(name)
    // ... bindings/consumers cleanup ...
    return deleted, b.storage.DeleteQueue(name)
}

// server/queue_handlers.go
deleted, err := s.Broker.DeleteQueue(queueName, deleteMethod.IfUnused, deleteMethod.IfEmpty)
if err != nil { /* existing error handling */ }
return s.sendQueueDeleteOK(conn, channelID, uint32(deleted))
```


**Test (TDD):** TDD in server (e.g. new cases in server/basic_get_purge_test.go or a queue_delete_test.go): (1) Declare a queue, publish 3 messages, call handleQueueDelete via the server path, and assert the queue.delete-ok frame carries MessageCount==3 (decode the outbound frame or assert on a broker-level DeleteQueue returning (3, nil)). (2) Delete an empty queue -> MessageCount==0. (3) Delete a non-existent queue -> (0, nil). Part B: declare a queue, register 2 consumers, re-declare and assert queue.declare-ok ConsumerCount==2. Add a broker unit test asserting StorageBroker.DeleteQueue returns the purged count.

**Regression risk:** Changing DeleteQueue's signature from error to (int, error) is a breaking interface change touching ~6 call sites plus test doubles; the compiler enforces all are updated, so risk of a missed site is low. Behavioral risk is minimal: delete semantics are unchanged, only an additional return value is surfaced. Main care point is preserving the existing NoWait handling and not altering the ifEmpty/ifUnused early-return error text. Part B adds a new interface method that every broker implementation and mock must implement, slightly larger blast radius; if that churn is undesirable, ship Part A only.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether interfaces.Broker + brokerWrapper edits are 'optional churn' as implied — verified they are REQUIRED: brokerWrapper.DeleteQueue (builder.go:348-350) delegates to unifiedBroker.DeleteQueue, so bumping UnifiedBroker to (int,error) forces a matching change to interfaces.Broker or a count-discard in the wrapper. Colleague's Part A file list is therefore correct and complete.
- Challenged the NoWait handling: colleague listed it as an optional 'check whether' item. Verified handleQueueDelete unconditionally sends delete-ok (queue_handlers.go:371) while handleQueuePurge gates on !NoWait (line 304). This is a genuine independent protocol bug; pushed to fold the two-line NoWait gate into the accepted fix rather than defer it. Colleague's own fix text already anticipated this ('mirror the existing pattern used in handleQueuePurge'), so it resolves cleanly.
- Verified the msgCount half of declare-ok is actually correct (queue.MessageCount.Load()), so the declare-ok defect is narrowly the consumer-count literal only — no overstatement by the colleague.
- Confirmed no other Broker/UnifiedBroker implementations exist beyond StorageBroker + adapters + the one test double, so the compiler-enforced blast radius has no hidden call sites.

**Fix concerns:**
- NoWait must be honored in handleQueueDelete as part of this fix (currently missing) — otherwise the broker keeps sending an unexpected delete-ok frame on no-wait deletes; use `if !deleteMethod.NoWait { ... } return nil` mirroring handleQueuePurge.
- When changing UnifiedBroker.DeleteQueue to (int, error), brokerWrapper.DeleteQueue (builder.go:349) will not compile unless interfaces.Broker.DeleteQueue is also bumped or the wrapper discards the count — do not leave this half-done.
- Preserve exact error text and semantics of the ifUnused/ifEmpty early returns in StorageBroker.DeleteQueue when converting them to the two-value `return 0, err` form; the ifEmpty branch already fetched the count via GetQueueMessageCount but relying on PurgeQueue's return is simpler and avoids a double count.
- Part B (declare-ok consumer count) adds a new interface method to every broker impl and mock; acceptable to defer with a code comment, but if shipped, ensure the count is read from a consistent source (storage.GetQueueConsumers or the queueConsumers map) and note it is best-effort (re-declare only).

**Agreed resolution (authoritative):**

Confirmed real, low-severity protocol-compliance bug on all cited points, verified against the actual source:

1) queue.declare-ok consumer-count is hardcoded 0 — server/queue_handlers.go:135 `return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, 0)` (msgCount IS real via queue.MessageCount.Load(); only consumer count is a literal). For a re-declare of a queue with live consumers this returns wrong data; for the common fresh declare it is coincidentally correct.

2) queue.delete-ok message-count is hardcoded 0 — server/queue_handlers.go:371 `return s.sendQueueDeleteOK(conn, channelID, 0)`, even though the count IS computed and discarded at broker/storage_broker.go:453 `b.storage.PurgeQueue(name)` (storage.PurgeQueue returns (int, error) per interfaces/storage.go:48 and StorageBroker.PurgeQueue at :1505). DeleteQueue's error-only signature (interfaces/broker.go:13, server/unified_broker.go:14) means the count cannot reach the handler without plumbing.

3) Frames are well-formed — frame_helpers.go:75-101 correctly serialize ConsumerCount and MessageCount (protocol structs QueueDeclareOKMethod, QueueDeleteOKMethod at methods.go:1426); only the caller-supplied numeric payload is wrong. No message loss, crash, or framing violation. Low severity is correct.

Fix is SOUND and the filesToChange list is exactly right — I traced the full call chain and it matches: the handler calls s.Broker (static type UnifiedBroker, server.go:32), so Part A must change UnifiedBroker.DeleteQueue (unified_broker.go:14), StorageBrokerAdapter.DeleteQueue (broker_adapters.go:30), StorageBroker.DeleteQueue (storage_broker.go:404, capturing the PurgeQueue count and updating early returns at :409/:411/:418/:421/:429/:432 and final :480), handleQueueDelete (queue_handlers.go:347/371), and the ackTestBroker test double (ack_worker_test.go:104). The interfaces.Broker + brokerWrapper changes the colleague also lists ARE required, not optional churn: brokerWrapper implements interfaces.Broker by delegating to unifiedBroker.DeleteQueue (builder.go:330/348-350), so once UnifiedBroker returns (int, error), builder.go:349 won't compile against an error-only interfaces.Broker — you must either also bump interfaces.Broker.DeleteQueue to (int, error) (colleague's choice, cleaner and compiler-enforced) or have brokerWrapper discard the count. Compiler enforces every site, so a missed call site is essentially impossible; behavioral risk is nil since delete semantics are unchanged. Part B (declare-ok consumer count) is genuinely optional; shipping Part A only with a comment is acceptable.

CHALLENGE that expands scope: I verified handleQueueDelete does NOT honor the NoWait flag (QueueDeleteMethod.NoWait exists at methods.go:1343) — it unconditionally sends delete-ok at line 371, unlike handleQueuePurge which correctly gates on `!purgeMethod.NoWait` (line 304). This is a second, independent protocol-compliance defect (sending an unexpected method frame when the client requested no-wait can desync stricter clients) and is arguably more impactful than the count reporting. The colleague flagged this only as an optional "check whether" and offered to keep it out of scope; I push to make the NoWait gate part of the accepted fix (mirror the purge pattern: `if !deleteMethod.NoWait { return s.sendQueueDeleteOK(conn, channelID, uint32(deleted)) } return nil`), since the handler is already being edited and the fix is a two-line change that closes a real gap. Agreed final position: real bug at low severity; adopt Part A with the NoWait gate folded in; Part B optional.

---

## [I16] nowait not honored in exchange.declare / queue.declare (always sends declare-ok)

**Rank 21 · P2** · Severity: ⚪ Low · Category: protocol-compliance · Effort: trivial · ⚡ Quick win

### Impact
A client that sets the nowait flag on exchange.declare or queue.declare expects NO synchronous reply frame. StrangeQ always sends exchange.declare-ok / queue.declare-ok regardless. A strict client that issued nowait and immediately proceeded (e.g. pipelined further methods without reading a reply) receives an unexpected method frame, which can desynchronize its per-channel reply bookkeeping or, for strict client libraries, be treated as an unexpected/out-of-sequence frame and trigger a channel/connection error. This is inconsistent with the broker's own handling of the analogous nowait flag in exchange.bind, exchange.unbind, queue.purge, basic.cancel, and confirm.select, all of which correctly suppress the -ok when nowait is set. Real-world impact is low because the two dominant clients (RabbitMQ Java/.NET, and Go/amqp091) do not set nowait on declare in normal use and many tolerate an extra reply, but it is a genuine AMQP 0.9.1 conformance gap.

### Root cause
The declare handlers deserialize the NoWait field but never branch on it before writing the response frame. handleExchangeDeclare ends with an unconditional `return s.sendExchangeDeclareOK(...)` and handleQueueDeclare ends with an unconditional `return s.sendQueueDeclareOK(...)`. The nowait-gating pattern used elsewhere (`if method.NoWait { return nil }`) was simply not applied to the two declare paths.

### Proof

- **`src/amqp-go/server/exchange_handlers.go` : 90-97** — exchange.declare-ok is sent unconditionally; there is no `if declareMethod.NoWait` guard before the send, unlike bind/unbind in the same file.

```go
// Record exchange declared metric
	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordExchangeDeclared()
	}

	// Send exchange.declare-ok response
	return s.sendExchangeDeclareOK(conn, channelID)
}
```

- **`src/amqp-go/server/queue_handlers.go` : 131-136** — queue.declare-ok is sent unconditionally; the deserialized declareMethod.NoWait flag is never checked before responding.

```go
	// Send queue.declare-ok response with queue info (spec: must return the
	// actual queue name, including server-generated names)
	msgCount := uint32(queue.MessageCount.Load())
	return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, 0)
}
```

- **`src/amqp-go/protocol/methods.go` : 809-819** — The wire method carries a NoWait bit that is parsed but never consulted by the handler.

```go
type ExchangeDeclareMethod struct {
	Reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]interface{}
}
```

- **`src/amqp-go/protocol/methods.go` : 1061-1070** — queue.declare also carries NoWait, deserialized but ignored in handleQueueDeclare.

```go
type QueueDeclareMethod struct {
	Reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  map[string]interface{}
}
```

- **`src/amqp-go/server/exchange_handlers.go` : 187-190** — Establishes the correct in-codebase pattern for honoring nowait, which declare fails to follow (also mirrored at queue_handlers.go:304 for purge, basic_handlers.go:531 for cancel, confirm_handlers.go:57 for confirm.select).

```go
	if bindMethod.NoWait {
		return nil
	}
	return s.sendExchangeBindOK(conn, channelID)
```


### Fix

Gate the declare-ok responses on the NoWait flag, matching the existing pattern used by bind/unbind/purge/cancel/confirm. In handleExchangeDeclare, before the final `return s.sendExchangeDeclareOK(...)`, add `if declareMethod.NoWait { return nil }`. In handleQueueDeclare, before the final `return s.sendQueueDeclareOK(...)`, add the same guard.

Important spec nuance for queue.declare: when a client sends queue.declare with an empty queue name (server-generated name) AND nowait=true, the client can never learn the generated name, which is a client bug. AMQP 0.9.1 does not strictly forbid it, and RabbitMQ raises a channel error (PRECONDITION_FAILED / 406) in this case. To stay conservative and avoid a behavior regression, the minimal fix is to just suppress the reply when NoWait is set. Optionally (recommended, low risk) also emit a channel error when NoWait is combined with an empty (server-generated) queue name, since silently swallowing the reply leaves the client with no name. If you add that, do it BEFORE the GenerateQueueName() call: `if declareMethod.NoWait && declareMethod.Queue == "" { return s.authzChannelError(conn, channelID, fmt.Errorf("queue.declare with nowait requires an explicit queue name"), 50, 10) }` — but confirm the desired error code/class with maintainers; the core fix is just the suppression. Keep the metric recording (RecordExchangeDeclared / RecordQueueDeclared) and the CurrentQueue tracking BEFORE the nowait check so channel state and metrics stay correct even when no reply is sent.

**Files to change:**
- `src/amqp-go/server/exchange_handlers.go` — In handleExchangeDeclare, insert `if declareMethod.NoWait { return nil }` after the metric-recording block (after line 93) and before the `return s.sendExchangeDeclareOK(...)` at line 96.
- `src/amqp-go/server/queue_handlers.go` — In handleQueueDeclare, insert `if declareMethod.NoWait { return nil }` after the CurrentQueue tracking + metric recording (after line 130) and before the `return s.sendQueueDeclareOK(...)` at line 135. Keep msgCount computation after the guard.

**Code sketch:**

```go
// exchange_handlers.go, end of handleExchangeDeclare
if s.MetricsCollector != nil {
    s.MetricsCollector.RecordExchangeDeclared()
}
if declareMethod.NoWait {
    return nil
}
return s.sendExchangeDeclareOK(conn, channelID)

// queue_handlers.go, end of handleQueueDeclare
// ... after CurrentQueue tracking and metric recording ...
if declareMethod.NoWait {
    return nil
}
msgCount := uint32(queue.MessageCount.Load())
return s.sendQueueDeclareOK(conn, channelID, queue.Name, msgCount, 0)
```


**Test (TDD):** Add table-driven tests in the server package mirroring TestConfirmSelectNoWaitSkipsResponse (publisher_confirms_test.go:98). (1) TestExchangeDeclareNoWaitSkipsResponse: serialize an ExchangeDeclareMethod{Exchange:"ex1", Type:"direct", NoWait:true}, feed it through handleExchangeDeclare with a mock/in-memory connection that captures written frames, and assert zero frames were written to the connection AND the exchange was actually declared in the broker. Add a companion TestExchangeDeclareWaitSendsDeclareOK with NoWait:false asserting exactly one exchange.declare-ok frame is written. (2) TestQueueDeclareNoWaitSkipsResponse / TestQueueDeclareWaitSendsDeclareOK doing the same for queue.declare (with an explicit queue name so the generated-name concern doesn't apply). Assert the queue is created regardless of NoWait. Follow the existing frame-capture harness used in publisher_confirms_test.go and queue_name_handler_test.go so no new test infra is needed.

**Regression risk:** Very low. The change only suppresses an outbound frame in the NoWait=true path, which is currently a no-op-worthy extra frame; the broker-side effects (exchange/queue creation, metrics, CurrentQueue tracking) are unchanged and executed before the guard. Risk surface: (a) any existing test that sends declare with NoWait=true and asserts a declare-ok would now fail — grep shows existing declare tests use NoWait:false (queue_name_handler_test.go:205,234), so none should break; (b) clients that incorrectly rely on receiving a reply after sending nowait would now hang, but that is a client bug and matches spec/RabbitMQ behavior. If the optional empty-name-with-nowait channel error is added, that has slightly higher risk and should be gated behind maintainer review; the core suppression fix carries minimal risk.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether NoWait is actually parsed off the wire or dead: confirmed it IS deserialized at methods.go:923 (ExchangeDeclare, bit 11) and methods.go:1159 (QueueDeclare, bit 11), so the handlers genuinely ignore a live flag — the bug holds.
- Challenged the 'inconsistent with sibling handlers' claim: verified all five cited gates exist (exchange_handlers.go:187, :236; queue_handlers.go:304; basic_handlers.go:531; confirm_handlers.go:57). Claim accurate.
- Challenged severity for possible inflation: agreed LOW is correct — real conformance gap but low real-world impact given dominant client behavior; declined to downgrade to trivial because a strict pipelining client can desync.
- Challenged the optional empty-name+nowait channel error add-on as a regression risk: it would break legitimate fire-and-forget server-named-queue clients; concluded it should be excluded from the minimal fix. Colleague had already gated it behind maintainer review, so this is a clarification not a contradiction.
- Verified state/metric ordering so the guard doesn't skip side effects: confirmed metrics and CurrentQueue tracking already execute before the declare-ok send in both handlers, so inserting the guard at the proposed location is safe.

**Fix concerns:**
- Do NOT include the optional empty-name+nowait channel error in this fix; ship the pure suppression only, per regression-risk reasoning above.
- Ensure the queue.declare guard is inserted AFTER CurrentQueue tracking (queue_handlers.go:120-125) and RecordQueueDeclared (:128-130) so channel state and metrics stay correct when no reply is sent; msgCount computation can move after the guard.
- Tests should assert not just frame suppression but also that the broker actually created the exchange/queue in the NoWait path, to prevent a future refactor from short-circuiting the side effects along with the reply.

**Agreed resolution (authoritative):**

Real bug, confirmed by reading source directly. exchange.declare and queue.declare deserialize NoWait off the wire (protocol/methods.go:923 `m.NoWait = (flags & (1<<11)) != 0` for ExchangeDeclare; methods.go:1159 same bit for QueueDeclare) but both handlers ignore it: handleExchangeDeclare ends with an unconditional `return s.sendExchangeDeclareOK(conn, channelID)` (exchange_handlers.go:96) and handleQueueDeclare with an unconditional `return s.sendQueueDeclareOK(...)` (queue_handlers.go:135). This is a genuine AMQP 0.9.1 conformance gap and is inconsistent with five sibling handlers that correctly gate on NoWait: exchange.bind (exchange_handlers.go:187), exchange.unbind (:236), queue.purge (queue_handlers.go:304), basic.cancel (basic_handlers.go:531), confirm.select (confirm_handlers.go:57). Severity stays LOW: dominant clients (amqp091-go, RabbitMQ Java/.NET) don't set nowait on declare in normal use and tolerate an extra reply; the only failure mode is a strict client that pipelines after a nowait declare and treats the unexpected reply frame as out-of-sequence. Not inflated, not downgradable to trivial — it is a real wire-protocol violation with a plausible (rare) desync. Fix is SOUND: insert `if declareMethod.NoWait { return nil }` immediately before each declare-ok send. Verified the ordering is safe — RecordExchangeDeclared (exchange_handlers.go:91-93) already precedes the send, and for queue.declare both CurrentQueue tracking (queue_handlers.go:120-125) and RecordQueueDeclared (:128-130) precede line 135, so broker state and metrics remain correct when the reply is suppressed. Existing declare tests use NoWait:false so none regress. RECOMMENDATION on the fix's optional add-on: adopt ONLY the pure suppression; do NOT add the optional empty-name+nowait channel error (406/502) in this fix — it is a genuine spec nuance but adding it unprompted risks breaking fire-and-forget clients that legitimately declare a server-named queue with nowait and discard the name, and the colleague already correctly gated it behind maintainer review. Add table-driven tests mirroring TestConfirmSelectNoWaitSkipsResponse (publisher_confirms_test.go:98) using the existing newPipeConnWithFrames harness: assert zero frames written when NoWait=true and exactly one declare-ok when NoWait=false, and that the exchange/queue is created in both cases.

---

## [I14] basic.publish immediate flag deserialized then silently ignored instead of rejected with NOT_IMPLEMENTED (540)

**Rank 22 · P2** · Severity: ⚪ Low · Category: protocol-compliance · Effort: small · ⚡ Quick win

### Impact
A client that publishes with immediate=true expects one of two guaranteed behaviors: (a) if the message cannot be immediately delivered to a ready consumer, it is returned to the publisher via basic.return with reply-code 313 NO_CONSUMERS; or (b) the broker refuses the feature outright. StrangeQ does neither — it decodes the immediate bit, logs it at debug level, and then routes/persists the message exactly as if immediate were false. The publisher believes it received guaranteed immediate-delivery-or-return semantics; instead the message is silently queued and may sit undelivered indefinitely with no basic.return. This is a silent semantic contract violation for the (rare, largely deprecated) set of clients that still set immediate=true. No data loss, no crash, no security exposure — hence low severity. Real-world blast radius is small because immediate was deprecated by AMQP tooling long ago (RabbitMQ removed it in 3.0), but the current behavior is strictly worse than either compliant option: it accepts the flag and lies about honoring it.

### Root cause
The immediate flag is fully wired through the wire codec (BasicPublishMethod.Serialize/Deserialize set and read bit 1) but the handler layer never consumes it. handleBasicPublish only logs publishMethod.Immediate, and completeMessage builds the protocol.Message struct copying Mandatory but omitting Immediate entirely, so downstream routing has no knowledge the flag was ever set. There is no branch anywhere that rejects immediate=true, even though the NotImplemented=540 error constant already exists in the errors package. In short: the field is parsed for completeness but no policy decision was ever attached to it.

### Proof


### Fix

Reject immediate=true at the start of handleBasicPublish before any message tracking begins, matching modern RabbitMQ which raises a connection-level NOT_IMPLEMENTED (540) when a client sets the immediate flag (RabbitMQ removed immediate support in 3.0 and closes the connection with 540). Concretely: immediately after successful Deserialize and the debug log in handleBasicPublish, add a guard: if publishMethod.Immediate is true, return the pooled method to its pool (protocol.PutBasicPublishMethod(publishMethod)) to avoid a leak, then send a connection.close with reply-code amqperrors.NotImplemented (540) and reply-text like "immediate=true is not implemented", and return. Use sendConnectionClose(conn, amqperrors.NotImplemented, "NOT_IMPLEMENTED - immediate=true is not implemented", "basic.publish") which already exists in connection_handlers.go and marks the connection closed. Do NOT create a pending message when rejecting, so no PendingMessages entry is left dangling and the subsequent header/body frames on the (now-closing) connection are harmless. Rationale for connection-close over channel-close: this mirrors real RabbitMQ behavior and the class of clients still sending immediate is negligible, so the harsher connection-level rejection is both spec-faithful and low-blast-radius. If maintainers prefer graceful degradation, an acceptable alternative is a channel-level reject via sendChannelClose(conn, channelID, amqperrors.NotImplemented, ..., 60, 40) returning an error — but connection-close is the RabbitMQ-compatible choice. Add the errors package import alias already used elsewhere (amqperrors) if not present in basic_handlers.go (it imports "github.com/maxpert/amqp-go/errors" as errors already — verify and use errors.NotImplemented).

**Files to change:**
- `src/amqp-go/server/basic_handlers.go` — In handleBasicPublish, after the debug-log block (~line 105) and before the authorize call, add a guard that returns the pooled publishMethod and sends a connection.close with reply-code 540 (errors.NotImplemented) when publishMethod.Immediate is true, then returns. Note basic_handlers.go already imports the errors package (used at line 348 via errors.Is / broker.ErrNoRoute) — confirm the local alias for github.com/maxpert/amqp-go/errors and use its NotImplemented constant.

**Code sketch:**

```go
// in handleBasicPublish, immediately after the debug-log block (~line 105):
if publishMethod.Immediate {
    protocol.PutBasicPublishMethod(publishMethod) // return to pool, avoid leak
    s.Log.Warn("Rejecting basic.publish with immediate=true (not implemented)",
        zap.String("connection_id", conn.ID),
        zap.Uint16("channel_id", channelID),
        zap.String("exchange", publishMethod.Exchange))
    return s.sendConnectionClose(conn, errors.NotImplemented,
        "NOT_IMPLEMENTED - immediate=true is not implemented", "basic.publish")
}
```


**Test (TDD):** Add a test in server (e.g. server/basic_handlers_test.go) named TestBasicPublishImmediateRejected: construct a Server with a test connection/channel, serialize a BasicPublishMethod with Immediate:true via its Serialize(), call s.handleBasicPublish(conn, channelID, payload), and assert (1) the function returns nil (connection.close was sent successfully) or the expected error per chosen strategy, (2) conn.Closed.Load() is true, (3) a connection.close frame with ReplyCode 540 was written to the mock connection's write buffer (decode the frame and assert ClassID/ReplyCode), and (4) conn.PendingMessages has NO entry for channelID (no pending message created). Add a companion negative test TestBasicPublishImmediateFalseAccepted asserting that with Immediate:false a pending message IS created and no connection.close is sent, guarding against over-rejection. Prefer writing these tests first (TDD) so they fail against current code (which creates a pending message and never closes the connection).

**Regression risk:** Low. The change only triggers when immediate=true, a code path currently exercised by essentially no modern clients (immediate was deprecated/removed in RabbitMQ 3.0). Risk items to watch: (1) ensure the pooled publishMethod is returned to the pool exactly once and not double-freed — do not call PutBasicPublishMethod again on a path that already returned; (2) since a connection.close is sent, verify the frame read loop stops publishing subsequent frames on a closed connection (conn.Closed is already checked elsewhere); (3) any existing test or benchmark that sets Immediate:true would now behave differently — grep shows all current tests use Immediate:false (protocol_bench_test.go:138, methods_test.go:484, rabbitmq_interop_test.go:553), so no existing test breaks. If maintainers pick channel-close instead of connection-close, adjust the test's assertion on frame class (channel.close 20.40 vs connection.close 10.50) accordingly.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged whether Immediate is used downstream: verified processCompleteMessage (basic_handlers.go:296-315) copies Mandatory at line 314 but never references Immediate — no rejection branch anywhere. Bug stands.
- Challenged the fix's import claim: verified basic_handlers.go:5 imports stdlib "errors" (line 348 errors.Is), NOT the amqp errors package. The colleague cited line 348 as proof the amqp package is imported — that citation is WRONG. errors.NotImplemented would fail to compile; must alias-import amqperrors and use amqperrors.NotImplemented. Fix downgraded to needs-revision.
- Challenged regression safety of sending connection.close mid-publish while the frame loop keeps running: verified processFrames (server.go:387-416) does NOT check conn.Closed, so trailing header/body frames are still dispatched — but processHeaderFrame/processBodyFrame handle the missing pending message gracefully (return nil), so no corruption or leak. Safe.
- Challenged pool double-free risk: verified PutBasicPublishMethod (buffer_pool.go:201-208) resets and returns; on the reject path no pending message references the method afterward, so single return is correct.
- Challenged the RabbitMQ-compatibility rationale for connection-close over channel-close: web search confirmed RabbitMQ 3.0+ returns not_implemented/540 and closes the connection for immediate=true — the colleague's strategy is exactly right.
- Challenged the proposed test's frame assertion: sendConnectionClose hardcodes ClassID=0/MethodID=0 (connection_handlers.go:346-347) and ignores the classMethod arg, so asserting a basic-class ClassID would fail — test should assert ReplyCode==540 on a connection.close (10.50) frame.

**Fix concerns:**
- Compile error: code sketch uses errors.NotImplemented but basic_handlers.go imports stdlib "errors" (line 5), not the amqp errors package. Must add aliased import amqperrors "github.com/maxpert/amqp-go/errors" and use amqperrors.NotImplemented (or literal 540). The fix's cited 'proof' (line 348) is the stdlib errors.Is, not the amqp package.
- Proposed test asserts on the connection.close frame's ClassID being basic-related, but sendConnectionClose hardcodes ClassID/MethodID to 0 (connection_handlers.go:346-347) and ignores the classMethod string arg. Test should assert only ReplyCode==540 and frame class/method 10.50 (connection.close), not a basic ClassID.
- Minor: passing "basic.publish" as classMethod is cosmetic/ignored — acceptable but worth knowing it does not populate the frame's class/method fields (pre-existing limitation, not introduced by this fix).

**Agreed resolution (authoritative):**

Real bug, confirmed at low severity. Immediate is deserialized (protocol/methods.go:1683), logged at debug (server/basic_handlers.go:104), then never consumed: processCompleteMessage builds protocol.Message copying Mandatory (basic_handlers.go:314) but omitting Immediate, and no rejection branch exists anywhere. StrangeQ accepts immediate=true and silently routes/queues the message as if it were false, violating the client's immediate-delivery-or-return contract with no basic.return. Blast radius is tiny (immediate was removed in RabbitMQ 3.0; near-zero modern clients set it), no data loss/crash/security — low is correct. Fix approach (reject at start of handleBasicPublish with a connection-level NOT_IMPLEMENTED 540) is RabbitMQ-faithful: web search confirms RabbitMQ 3.0+ raises {amqp_error,not_implemented,"immediate=true",'basic.publish'} → 540 connection close. The reject-before-pending-message design is sound: processHeaderFrame (154-161) and processBodyFrame (218-223) both handle 'no pending message' gracefully (log + return nil), and processFrames (server.go:387-416) does not short-circuit on conn.Closed but the trailing header/body frames are harmless. PutBasicPublishMethod is safe (resets and returns; nothing references the method after reject — no double-free). AGREED FIX (with revision): add the guard, but the fix as written will NOT compile — basic_handlers.go:5 imports the STANDARD-LIBRARY "errors" (used at line 348 for errors.Is), not the amqp errors package. errors.NotImplemented does not exist there. The colleague's proof note (that line 348 shows the amqp errors package is imported) is wrong. The fix must add an aliased import amqperrors "github.com/maxpert/amqp-go/errors" (as authz.go:6 already does) and use amqperrors.NotImplemented (or literal 540). Test note: sendConnectionClose hardcodes ClassID/MethodID to 0 (connection_handlers.go:346-347) and ignores the classMethod string, so the test should assert ReplyCode==540 and a connection.close (10.50) frame — it must NOT assert a basic-class ClassID, which would be 0. With the import corrected and the test adjusted, the fix is correct, complete, and regression-safe (all existing tests use Immediate:false).

---

# P3

## [I18] Overlong short-string encoding silently truncates instead of erroring (real, but impact overstated: only server-generated reply-text is affected, not client-supplied names)

**Rank 23 · P3** · Severity: ⚪ Low · Category: protocol-compliance · Effort: small · ⚡ Quick win

### Impact
EncodeShortString and AppendShortString silently drop bytes past 255 (s[:255]) and never signal an error. The claim that this corrupts overlong queue/exchange/consumer-tag names is largely incorrect: AMQP short strings carry a single-byte length prefix, so a client physically cannot transmit a name longer than 255 bytes, and decodeShortString (methods.go:771-787) reads at most 255 bytes. Client-supplied identity strings therefore arrive already <=255 and are never truncated on re-encode. The only realistic truncation vector is SERVER-GENERATED text that is not bounded before encoding: ChannelClose/ConnectionClose/BasicReturn ReplyText (methods.go:586; server/authz.go:44, server/connection_handlers.go:345, server/frame_helpers.go:251), which can exceed 255 bytes for a long authorization/error message. That text is human-readable diagnostics, so truncation is cosmetic (a clipped error string), not identity corruption. A secondary defect: s[:255] can split a multi-byte UTF-8 rune, emitting an invalid-UTF-8 short string that a strict client could reject. Net effect is a minor robustness/observability gap, not a data-integrity or routing bug.

### Root cause
EncodeShortString/AppendShortString treat a too-long input as a caller error but handle it by silently clamping to 255 bytes rather than returning an error or letting callers bound the string. Because the encode API cannot fail, server code that builds unbounded diagnostic strings (ReplyText) has no signal that its text was clipped, and the byte-index slice s[:255] is not rune-aware.

### Proof

- **`src/amqp-go/protocol/methods.go` : 622-632** — Confirms silent truncation with no error return. Comment says 'characters' but len(s) and s[:255] operate on BYTES, so a multi-byte UTF-8 rune can be split, producing invalid UTF-8.

```go
func EncodeShortString(s string) []byte {
	if len(s) > 255 {
		// Truncate if longer than 255 characters
		s = s[:255]
	}
	result := make([]byte, 1+len(s))
	result[0] = byte(len(s))
	copy(result[1:], []byte(s))
	return result
}
```

- **`src/amqp-go/protocol/methods.go` : 637-644** — The hot-path variant has the identical silent-truncate behavior and no error channel.

```go
func AppendShortString(buf []byte, s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	buf = append(buf, byte(len(s)))
	buf = append(buf, s...)
	return buf
}
```

- **`src/amqp-go/protocol/methods.go` : 771-787** — Decode reads a single length byte (max 255). Proves a client CANNOT deliver a >255-byte name over the wire, so client-supplied queue/exchange/consumer-tag names are never truncated on re-encode. This refutes the claim's stated impact.

```go
strLen := int(data[offset])
	offset++
	if offset+strLen > len(data) {
		return "", offset, fmt.Errorf("short string extends beyond data")
	}
	str := string(data[offset : offset+strLen])
```

- **`src/amqp-go/protocol/methods.go` : 586** — ChannelClose.ReplyText is encoded through the truncating helper. ReplyText is the one realistic overlong-input path since it is server-built diagnostic text, not client-bounded.

```go
replyTextBytes := encodeShortString(m.ReplyText)
```

- **`src/amqp-go/server/authz.go` : 41-47** — replyText originates from server-side error messages (e.g. authorization failures embedding a resource name) with no length bound before encoding, so it can exceed 255 bytes and be clipped.

```go
func (s *Server) sendChannelClose(conn *protocol.Connection, channelID uint16, replyCode uint16, replyText string, classID, methodID uint16) {
	closeMethod := &protocol.ChannelCloseMethod{
		ReplyCode: replyCode,
		ReplyText: replyText,
```

- **`src/amqp-go/protocol/structures.go` : 226-233** — Server-generated queue names are always 'amq.gen.<hex>' and far under 255 bytes, confirming server-generated identity names are never truncated.

```go
func GenerateQueueName() string {
...
	return "amq.gen." + fmt.Sprintf("%x", b)
```

- **`src/amqp-go/protocol/compliance_test.go` : 164-177** — An existing test explicitly asserts truncation-to-255 as the intended contract, showing the behavior is deliberate. Any fix must update this test (and methods_test.go:247-254) to reflect the new contract.

```go
longStr := string(make([]byte, 300))
	encoded := EncodeShortString(longStr)
	// Should be truncated to 255
	if encoded[0] != 255 {
		t.Errorf("Expected max length 255, got %d", encoded[0])
	}
```


### Fix

Make short-string encoding fail loudly for the one path that can actually produce overlong input (server-generated ReplyText), and make truncation UTF-8-safe as defense-in-depth. Concretely: (1) Keep EncodeShortString/AppendShortString non-erroring for the hot path but change the clamp to be rune-aware so we never emit invalid UTF-8: instead of s[:255], find the largest prefix <=255 bytes that ends on a rune boundary. (2) Add a validated helper for names that SHOULD be bounded by the caller. Because the wire already caps client input at 255, the realistic win is on encode of server-built strings: bound ReplyText at the call sites. Add a helper in server (e.g. truncateReplyText(s string) string) that clips reply text to <=255 bytes on a rune boundary and appends an ellipsis marker, and call it in sendChannelClose (authz.go:41), sendConnectionClose (connection_handlers.go:342), and sendBasicReturn (frame_helpers.go:248) before assigning ReplyText. This keeps AMQP framing valid while preserving readable diagnostics. (3) Optionally add a new EncodeShortStringChecked(s string) ([]byte, error) that returns an error when len(s)>255, and use it only in non-hot code paths where the caller wants to detect programmer error; leave the existing non-erroring functions for the serialize hot paths. Update the two existing tests (compliance_test.go MaxLengthShortString and methods_test.go) to assert the new rune-boundary behavior (encoded[0] <= 255 AND utf8.Valid(encoded[1:]) is true) rather than exactly 255 for a 300-null-byte input (null bytes are single-byte runes so 255 still holds there; add a separate multibyte case).

**Files to change:**
- `src/amqp-go/protocol/methods.go` — Add a shared rune-aware clampShortString helper and route both EncodeShortString and AppendShortString through it so truncation never splits a UTF-8 rune. Optionally add EncodeShortStringChecked returning an error for non-hot callers. Add import unicode/utf8.
- `src/amqp-go/server/authz.go` — In sendChannelClose, bound replyText (truncateReplyText) before building ChannelCloseMethod so overlong server error text is clipped intentionally with a marker rather than silently.
- `src/amqp-go/server/connection_handlers.go` — In sendConnectionClose, bound replyText before building ConnectionCloseMethod.
- `src/amqp-go/server/frame_helpers.go` — In sendBasicReturn, bound replyText before building BasicReturnMethod.
- `src/amqp-go/protocol/compliance_test.go` — Update MaxLengthShortString and add a multibyte-rune case asserting encoded[0] <= 255 and utf8.Valid(encoded[1:]) — proving no rune is split.
- `src/amqp-go/protocol/methods_test.go` — Update the length assertion (lines ~247-254) to allow the rune-boundary clamp (encoded length may be <255 for multibyte input) instead of requiring exactly 255.

**Code sketch:**

```go
// protocol/methods.go — rune-safe clamp shared by both encoders
func clampShortString(s string) string {
	if len(s) <= 255 {
		return s
	}
	// largest prefix <=255 bytes ending on a rune boundary
	end := 255
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end]
}

func EncodeShortString(s string) []byte {
	s = clampShortString(s)
	result := make([]byte, 1+len(s))
	result[0] = byte(len(s))
	copy(result[1:], s)
	return result
}

func AppendShortString(buf []byte, s string) []byte {
	s = clampShortString(s)
	return append(append(buf, byte(len(s))), s...)
}

// server side — bound diagnostic text before encoding
func truncateReplyText(s string) string {
	if len(s) <= 255 { return s }
	return s[:252] + "..." // then rune-clamp via clampShortString in encoder
}
```


**Test (TDD):** Add TestEncodeShortString_RuneSafeTruncation in protocol/methods_test.go: build a string of 200 three-byte runes (e.g. strings.Repeat("é", 200) = 400 bytes), call EncodeShortString and AppendShortString, and assert: (a) total encoded length is 1+encoded[0], (b) encoded[0] <= 255, (c) utf8.Valid(encoded[1:]) is true (no split rune) — this fails on the current s[:255] which slices at byte 255 mid-rune and produces invalid UTF-8. Add TestTruncateReplyText in the server package: pass a 500-byte error string, assert result is <=255 bytes, ends with the ellipsis marker, and is valid UTF-8. This is TDD: both tests fail against current code (invalid UTF-8 / no bounding at call sites) and pass after the fix.

**Regression risk:** Low. Hot-path encoders keep the same non-erroring signature, so no call sites break. The clamp only changes behavior for inputs >255 bytes containing multibyte runes at the boundary (extremely rare on this wire since inbound is already capped at 255). Two existing tests hard-code 'expect exactly 255' and MUST be updated or they will fail — this is expected and part of the change. Bounding ReplyText at call sites changes only diagnostic text content for pathologically long error messages; no framing or routing behavior changes. Adding unicode/utf8 import is trivial.

### Debate & agreed resolution

Investigator verdict: `confirmed-severity-adjusted` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the impact scope: verified decodeShortString (methods.go:776) reads exactly one length byte, so inbound names are hard-capped at 255 and never truncate on re-encode. The colleague's downgrade of the original claim is CORRECT.
- Challenged 'only server-generated text is affected': found authz.go:74-76 passes err.Error() to sendChannelClose, and errors.go:16-20/154 wrap client-supplied resource names in a fixed prefix plus extra strings (NewExchangeTypeMismatch embeds name + expected + actual types). So a legitimate <=255-byte client name plus wrapper can exceed 255. The overlong vector is broader than 'server-generated diagnostics' -- but impact is still only clipped diagnostic text, so severity is unchanged (low).
- Challenged the UTF-8 split claim empirically: ran a Go program. s[:255] on 2-byte runes (e) IS invalid UTF-8 (255 is odd -> mid-rune split); s[:255] on genuine 3-byte runes is VALID (255 % 3 == 0 lands on a boundary). So the split defect is real but conditional on rune-width alignment.
- Refuted the proposed TDD test as written: the colleague's test says 'strings.Repeat("e", 200) = 400 bytes' and calls it '200 three-byte runes' -- but e is a 2-byte rune, and a test built on real 3-byte runes would NOT fail against current code (s[:255] stays valid at a 3-byte boundary), contradicting their 'fails against current code' TDD claim. The test must use a 2-byte rune (or any width where 255 lands mid-rune) to be genuinely red-first. Intent is right, specifics are wrong.
- Verified the clampShortString code sketch is correct: output is always <=255 bytes (loop caps end at 255, decrements on continuation bytes), so the byte(len(s)) cast never overflows; confirmed valid UTF-8 output for 2-, 3-, and 4-byte rune inputs.

**Fix concerns:**
- Proposed test is factually incorrect: 'e' is a 2-byte UTF-8 rune, not the '3-byte rune' the fix labels it, and 200 of them is 400 bytes only because they are 2 bytes each. A test using genuine 3-byte runes would pass against current code (s[:255] is valid when 255 is divisible by 3), so it would NOT be red-first. The test MUST use a 2-byte rune (or any count where byte 255 falls mid-rune) to actually demonstrate the current bug. Recommend strings.Repeat("é", 200) and asserting utf8.Valid(encoded[1:]) is false on current code, true after.
- truncateReplyText sketch does s[:252] + "..." by byte index, which itself can split a multi-byte rune at byte 252 before the encoder's clampShortString runs. Since '...' (3 ASCII bytes) is appended, total is 255, so clampShortString returns it unchanged and does NOT re-clamp the mid-rune split at 252. The truncateReplyText helper must itself be rune-aware (or clamp first, then append marker within the 255 budget). The fix's own comment '// then rune-clamp via clampShortString in encoder' is wrong -- clampShortString only acts when len>255, and 255 is not >255.
- The fix should confirm 255-divisible-by-rune-width cases still round-trip: for 3-byte runes clampShortString returns s[:255] unchanged (valid), which is correct, but the new compliance_test.go assertion should not accidentally require length < 255 for those inputs.
- Optional EncodeShortStringChecked adds API surface with no current caller that needs it (all hot paths keep the non-erroring signature); low value, can be dropped to keep the change minimal.

**Agreed resolution (authoritative):**

Confirmed real but low severity. Two distinct defects in protocol/methods.go: (1) EncodeShortString (lines 622-632) and AppendShortString (lines 637-644) silently clamp inputs >255 bytes via s[:255] with no error return; (2) that byte-index slice can split a multi-byte UTF-8 rune, emitting an invalid short string. The colleague's core defense is correct and I verified it: decodeShortString (methods.go:776) reads a single length byte, so client-supplied names arrive physically <=255 bytes and are never truncated on re-encode. This refutes the original claim of queue/exchange/consumer-tag identity corruption. The one realistic overlong-input vector is diagnostic ReplyText, and I found it is broader than the colleague stated: authz.go:74-76 feeds err.Error() into sendChannelClose, and errors.go wrappers (e.g. NewExchangeTypeMismatch at errors.go:154) embed client-supplied names PLUS a fixed prefix and additional strings, so a valid <=255-byte client name inside the wrapper can push the full ReplyText past 255. So overlong text arises from client-names-embedded-in-error-wrappers, not only purely server-generated text. Impact is still cosmetic (a clipped diagnostic string), not routing/data-integrity corruption -> LOW severity confirmed. The fix APPROACH is sound: the clampShortString sketch is correct (I empirically verified it produces valid UTF-8 and always <=255 bytes for 2-, 3-, and 4-byte runes, so byte(len(s)) never overflows), and bounding ReplyText at the three call sites (authz.go, connection_handlers.go:342, frame_helpers.go:248) is right. Both existing tests (compliance_test.go MaxLengthShortString lines 164-177 and methods_test.go lines 247-254) do hard-code exact-255 expectations and must be updated. However the fix NEEDS REVISION on its test design: the proposed TDD test is factually wrong and would not be red-first as claimed.

---

## [I30] LifecycleManager.GetStats/GetConnections report placeholder data: zeroed message/byte counters, hardcoded Username="guest", and synthetic ConnectedAt/LastActivity timestamps

**Rank 24 · P3** · Severity: ⚪ Low · Category: observability · Effort: small · ⚡ Quick win

### Impact
Any consumer of the public interfaces.Server API (GetStats/GetConnections, declared at interfaces/server.go:217-221) receives misleading monitoring data: MessagesPublished/MessagesDelivered/BytesReceived/BytesSent are always 0, every connection reports Username="guest" (even when a different SASL user authenticated), and ConnectedAt/LastActivity are always "now" (so uptime-per-connection and idle detection are impossible). The Username field is the most concrete defect because the correct value already exists on the Connection struct and is being silently discarded. Practical blast radius is limited today because no in-repo HTTP/admin endpoint or other non-test caller surfaces this data (the only callers are the interface definition itself); the risk is to external library users who wire these methods into a dashboard and get wrong values. This confirms the claimed low severity.

### Root cause
The methods were written as stubs with TODO markers before the supporting data plumbing existed. Three distinct gaps: (1) message/byte totals are only recorded into write-only Prometheus counters via the MetricsCollector interface (server/metrics.go:24-25) which exposes no read-back getters, so GetStats has no in-process running total to read and falls back to literal 0; (2) GetConnections hardcodes "guest" instead of reading the already-populated Connection.Username field; (3) the Connection struct has no ConnectedAt/LastActivity fields, so those timestamps are fabricated with time.Now().

### Proof

- **`src/amqp-go/server/lifecycle.go` : 356-368** — The four message/byte statistics are literal zeros with an acknowledging TODO, never populated from any real counter.

```go
// TODO: Add message counters when available
		MessagesPublished: 0,
		MessagesDelivered: 0,
		BytesReceived:     0,
		BytesSent:         0,
```

- **`src/amqp-go/server/lifecycle.go` : 391-399** — Username is hardcoded to "guest" and both timestamps are set to time.Now() on every call, ignoring per-connection reality. Note conn.Vhost is read correctly on the very next line, showing the same pattern should be used for conn.Username.

```go
Username:      "guest", // TODO: Add username field to Connection struct
			VirtualHost:   conn.Vhost,
			Channels:      channelCount,
			ConnectedAt:   time.Now(), // TODO: Add ConnectedAt field to Connection struct
			LastActivity:  time.Now(), // TODO: Add LastActivity field to Connection struct
```

- **`src/amqp-go/protocol/structures.go` : 14-30** — The TODO comment 'Add username field to Connection struct' is stale: the Username field already exists and is populated during handshake, yet GetConnections ignores it. There are, however, no ConnectedAt/LastActivity fields, so those timestamps genuinely require a struct change.

```go
type Connection struct {
	ID              string
	Conn            net.Conn
	Channels        sync.Map
	Vhost           string
	Username        string                     // Authenticated username
```

- **`src/amqp-go/server/metrics.go` : 4-29** — The metrics interface is write-only (Record*/Update* verbs, no getters), confirming GetStats has no existing in-process counter to read back, so populating those four fields requires adding new atomic counters, not merely wiring an existing value.

```go
type MetricsCollector interface {
	...
	RecordMessagePublished(size int)
	RecordMessageDelivered(size int)
```

- **`src/amqp-go/interfaces/server.go` : 217-256** — These methods are part of the exported Server interface, so the placeholder data is contractually exposed to external library consumers; but no in-repo caller consumes it, bounding severity to low.

```go
GetStats() *ServerStats
	// GetConnections returns active connections
	GetConnections() []ConnectionInfo
```


### Fix

Fix in three independent, incremental steps ordered by cost/benefit. STEP 1 (trivial, real correctness fix): In GetConnections replace the hardcoded Username: "guest" with Username: conn.Username, mirroring the adjacent conn.Vhost read. This immediately surfaces the correct authenticated user. STEP 2 (small): Add ConnectedAt and LastActivity fields to protocol.Connection. Set ConnectedAt in NewConnection (time.Now()). For LastActivity, store it as an atomic (e.g. LastActivity atomic.Int64 holding UnixNano) and update it on each inbound frame in the processFrames read loop; expose a helper GetLastActivity() time.Time. Then read both in GetConnections instead of time.Now(). Using an atomic avoids adding lock contention on the per-frame hot path. STEP 3 (medium): Populate the message/byte counters in GetStats. The cleanest approach that does not depend on Prometheus internals is to add four atomic counters (messagesPublished, messagesDelivered, bytesReceived, bytesSent atomic.Int64) to the Server struct, increment them alongside the existing MetricsCollector.RecordMessagePublished/RecordMessageDelivered calls (basic_handlers.go:368, 628, 729; batch_delivery.go:84) and at the frame-read/write boundaries for BytesReceived/BytesSent, then read them in GetStats via Load(). Alternatively expose read-back getters on the concrete Prometheus collector, but the atomic approach is simpler, lock-free, and backend-independent. If a full implementation is out of scope, at minimum do STEP 1 now (it is a genuine data-correctness bug, not just a missing feature).

**Files to change:**
- `src/amqp-go/server/lifecycle.go` — In GetConnections, change Username: "guest" to Username: conn.Username and drop the stale TODO; replace ConnectedAt/LastActivity time.Now() with conn.ConnectedAt and conn.GetLastActivity() once fields exist. In GetStats, replace the four literal 0s with lm.server.messagesPublished.Load(), etc.
- `src/amqp-go/protocol/structures.go` — Add ConnectedAt time.Time and LastActivity atomic.Int64 (UnixNano) to the Connection struct; initialize ConnectedAt and LastActivity in NewConnection; add GetLastActivity() time.Time and TouchActivity() helpers.
- `src/amqp-go/server/server.go` — Add four atomic.Int64 counters (messagesPublished, messagesDelivered, bytesReceived, bytesSent) to the Server struct.
- `src/amqp-go/server/basic_handlers.go` — Increment s.messagesPublished/s.bytesReceived (publish path ~L368) and s.messagesDelivered/s.bytesSent (delivery paths ~L628, L729) next to the existing MetricsCollector.Record* calls.
- `src/amqp-go/server/batch_delivery.go` — Increment s.messagesDelivered/s.bytesSent alongside the RecordMessageDelivered call (~L84).
- `src/amqp-go/protocol frame read loop (processFrames)` — Call conn.TouchActivity() on each inbound frame read to keep LastActivity current.

**Code sketch:**

```go
// lifecycle.go GetConnections
connInfo := interfaces.ConnectionInfo{
    ID:            id,
    RemoteAddress: remoteAddr,
    Username:      conn.Username,        // was "guest"
    VirtualHost:   conn.Vhost,
    Channels:      channelCount,
    ConnectedAt:   conn.ConnectedAt,     // new field
    LastActivity:  conn.GetLastActivity(),
}

// structures.go
type Connection struct {
    // ...
    ConnectedAt  time.Time
    LastActivity atomic.Int64 // UnixNano
}
func (c *Connection) TouchActivity()             { c.LastActivity.Store(time.Now().UnixNano()) }
func (c *Connection) GetLastActivity() time.Time { return time.Unix(0, c.LastActivity.Load()) }

// lifecycle.go GetStats
MessagesPublished: lm.server.messagesPublished.Load(),
MessagesDelivered: lm.server.messagesDelivered.Load(),
BytesReceived:     lm.server.bytesReceived.Load(),
BytesSent:         lm.server.bytesSent.Load(),
```


**Test (TDD):** Add TestGetConnections_ReportsRealUsername in server/lifecycle_test.go: construct a Server, insert a *protocol.Connection into s.Connections with Username="alice" and Vhost="/prod", call lm.GetConnections(), assert result[0].Username == "alice" (currently fails, returns "guest") and VirtualHost == "/prod". For STEP 2, add TestGetConnections_UsesConnectionTimestamps: set conn.ConnectedAt to a fixed past time, assert GetConnections returns that exact time (not time.Now()); call TouchActivity, assert LastActivity advances. For STEP 3, add TestGetStats_CountsMessages: with a real (non-NoOp) counter path, publish N messages of known size through the handler, then assert GetStats().MessagesPublished == N and BytesReceived == totalBytes (currently fails, returns 0). Each test should first be written to fail against current code (TDD red), then pass after the fix.

**Regression risk:** STEP 1 (Username) is near-zero risk: it reads an existing field the same way conn.Vhost is already read. STEP 2/3 touch the per-frame hot path; using atomics (not mutexes) keeps overhead to a single atomic add per frame and avoids contention, but increments must be placed carefully to avoid double-counting (e.g. redelivery vs first delivery). The atomic LastActivity write on every frame is the only hot-path addition and is lock-free. No protocol behavior changes; only observability output changes, so functional AMQP conformance is unaffected.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether Username was actually available or genuinely absent: verified connection_handlers.go:291 sets conn.Username = user.Username on successful SASL auth (and 'guest'/'authenticated' in other branches), so the hardcoded 'guest' in GetConnections discards a real authenticated value — the TODO comment claiming 'Add username field to Connection struct' is stale. Resolved in colleague's favor: this elevates it from a stub to a genuine correctness bug.
- Challenged whether ConnectedAt could be derived from existing state rather than needing a struct change: verified lifecycle.go:50 startTime is server-wide only, no per-connection timestamp exists anywhere (grep found only the interface fields and the time.Now() stubs). Resolved in colleague's favor: per-connection ConnectedAt/LastActivity genuinely require new Connection fields.
- Challenged severity by hunting for any real caller: grep confirmed GetStats()/GetConnections() have NO callers outside interfaces/server.go declarations — no HTTP/admin endpoint surfaces them. Resolved: LOW severity is correct, not undersold.
- Challenged STEP 3 for double-counting: both RecordMessageDelivered sites (basic_handlers.go:628 basic.get, :729 push delivery) fire unconditionally including redeliveries. The fix body says 'increment alongside the existing Record* calls' which would count redeliveries in MessagesDelivered, yet the colleague's own regressionRisk note warns to avoid redelivery double-counting — a self-contradictory caveat. Minor and STEP-3-only; matching Prometheus counter semantics is defensible but should be clarified before implementing STEP 3.

**Fix concerns:**
- STEP 3 counter placement is internally inconsistent with its own regressionRisk warning: incrementing alongside RecordMessageDelivered at basic_handlers.go:628/729 counts redeliveries (both sites fire regardless of the redelivered flag), yet the note cautions against 'redelivery vs first delivery' double-counting. Clarify intended semantics (likely match the existing Prometheus counter = count every wire delivery) before implementing. Minor; STEP 3 is optional.
- The proof's suggested increment at 'frame-read/write boundaries for BytesReceived/BytesSent' is under-specified — needs to be pinned to the reader goroutine (bytes read) and conn.Conn.Write sites (bytes sent, e.g. basic_handlers.go:617/717) to avoid missing paths like heartbeats; not load-bearing for the LOW-severity core fix (STEP 1).

**Agreed resolution (authoritative):**

Confirmed real bug at LOW severity. All three defects verified against source: (1) lifecycle.go:394 hardcodes Username:"guest" while conn.Username is genuinely populated during SASL handshake (connection_handlers.go:291 sets it to the authenticated user's name) and conn.Vhost is read correctly one line below at :395 — this is a genuine data-correctness bug (a real value is silently discarded), not merely a missing feature. (2) lifecycle.go:397-398 fabricate ConnectedAt/LastActivity with time.Now(); the Connection struct (structures.go:14-30) has no such fields and only a server-wide startTime exists (lifecycle.go:50), so these genuinely require new struct fields. (3) lifecycle.go:364-367 return literal 0 for MessagesPublished/Delivered/BytesReceived/BytesSent; the MetricsCollector interface (metrics.go:4-79) is write-only with no getters, so new counters are required. Severity correctly bounded to LOW: GetStats/GetConnections are exported on interfaces.Server (server.go:217-221) but have ZERO in-repo callers other than the interface declaration itself, so the only blast radius is external library users wiring these into a dashboard. The proposed three-step fix is sound: STEP 1 (Username: conn.Username, near-zero risk, mirrors the adjacent Vhost read) is the highest-value change and should be done regardless; STEP 2 (ConnectedAt time.Time + LastActivity atomic.Int64 with TouchActivity() in processFrames at server.go:384) is correct and lock-free; STEP 3 counters use accurate call-site references (basic_handlers.go:368/628/729, batch_delivery.go:84). All line numbers in the fix verified accurate.

---

## [I35] generateID/GenerateQueueName crypto/rand fallback uses time-seeded global math/rand and can produce colliding IDs

**Rank 25 · P3** · Severity: ⚪ Low · Category: correctness · Effort: trivial · ⚡ Quick win

### Impact
If crypto/rand.Read ever returns an error, generateID() falls back to seeding the GLOBAL math/rand source with time.Now().UnixNano() and returning its first Int63(). Two connections whose fallback executes within the same nanosecond get the same seed, hence the same first Int63() value, hence identical "conn-<n>" IDs. Connection.ID is the key in server.Connections (server.go:163). A collision means: (a) the second registration overwrites the first, so the map tracks only one *Connection; (b) when either connection tears down, delete(s.Connections, connection.ID) at server.go:211 removes the shared key, dropping the still-live connection from tracking. Consequences are limited to the in-memory tracking map: graceful shutdown (lifecycle.go:268) will not force-close the untracked live connection, and GetConnections()/connection-count metrics under-report. Message routing and per-connection state are unaffected because each *Connection object is independent. GenerateQueueName() has the identical fallback pattern, which for generated (amq.gen.*) queue names is more concerning since queue names key routing/state. NOTE: this fallback branch is effectively unreachable on Linux/macOS with modern Go — crypto/rand.Read reads the kernel CSPRNG and, since Go 1.24, is documented to never return an error (it panics internally on OS RNG failure). The module builds with go1.26. So this is a real latent defect but practically dead code, consistent with the claimed low severity.

### Root cause
The fallback re-seeds the process-global math/rand source from a nanosecond clock and immediately reads one value. Time-based seeding is not collision-resistant at sub-nanosecond call rates, and mrand.Seed on the global source is discouraged since Go 1.20 (the global source is auto-seeded). There is no monotonic/uniqueness component (e.g., a counter) mixed in, so identical seeds yield identical outputs. The design assumed crypto/rand failure is impossible but still wrote a fallback that does not preserve the uniqueness invariant IDs are relied upon for.

### Proof

- **`src/amqp-go/protocol/structures.go` : 207-219** — Fallback seeds the global math/rand from a nanosecond clock and returns its first value; two invocations within the same nanosecond produce identical IDs. Excerpt: `_, err := rand.Read(b); if err != nil { mrand.Seed(time.Now().UnixNano()); fallbackID := mrand.Int63(); return fmt.Sprintf("conn-%d", fallbackID) }`
- **`src/amqp-go/protocol/structures.go` : 226-234** — Same defective fallback pattern for auto-generated queue names, where a collision aliases queue routing/state. Excerpt: `if err != nil { mrand.Seed(time.Now().UnixNano()); return fmt.Sprintf("amq.gen.%d", mrand.Int63()) }`
- **`src/amqp-go/server/server.go` : 163-211** — Connection.ID is the map key: `s.Connections[connection.ID] = connection` at 163 and `delete(s.Connections, connection.ID)` at 211. Colliding IDs cause the second connection to overwrite the first; teardown of either then deletes the shared key, dropping the surviving live connection from the tracking map.
- **`src/amqp-go/server/lifecycle.go` : 268-272** — Graceful shutdown only iterates tracked connections (`for _, conn := range lm.server.Connections { conn.Conn.Close() }`), so an untracked (collided-away) live connection would not be force-closed — the concrete downstream harm.

### Fix

Make the fallback collision-resistant and drop the misuse of the global math/rand seed. Preferred, minimal-behavior-change: replace the time-seeded global-rand fallback with one that cannot collide even if reached, by combining a process-lifetime monotonic atomic counter with the nanosecond timestamp. Add a package-level `var idFallbackCounter atomic.Uint64` in structures.go. Factor the fallback into a small helper `func fallbackID(prefix string) string` that does `n := idFallbackCounter.Add(1); return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)`. In generateID() on crypto/rand error return `fallbackID("conn")`; in GenerateQueueName() return `fallbackID("amq.gen")`. The counter guarantees uniqueness within the process regardless of clock resolution. Remove the `mrand.Seed(...)`/`mrand.Int63()` calls and delete the now-unused `mrand "math/rand"` import (verify with grep that no other use remains); `time` stays (still used elsewhere). Alternative fail-fast option: since crypto/rand.Read failure indicates a fundamentally broken system and is documented not to occur on supported platforms, panic instead of degrading uniqueness. Recommend the counter approach to preserve availability. Run gofmt and `go build ./...` / `go vet ./...`.

**Files to change:**
- `src/amqp-go/protocol/structures.go` — Add `var idFallbackCounter atomic.Uint64` and an unexported `fallbackID(prefix string) string` helper. Rewrite the fallback branches in generateID() (~lines 210-216) and GenerateQueueName() (~lines 229-232) to call fallbackID instead of mrand.Seed/mrand.Int63. Remove the now-unused `mrand "math/rand"` import (line 6) after confirming no other usage.

**Code sketch:**

```go
var idFallbackCounter atomic.Uint64

func fallbackID(prefix string) string {
	n := idFallbackCounter.Add(1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)
}

func generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fallbackID("conn")
	}
	return fmt.Sprintf("%x", b)
}

func GenerateQueueName() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fallbackID("amq.gen")
	}
	return "amq.gen." + fmt.Sprintf("%x", b)
}
```


**Test (TDD):** Add a TDD unit test in src/amqp-go/protocol proving the fallback is collision-free. TestFallbackIDUnique calls fallbackID("conn") from many goroutines for a large count (e.g. 100k), inserts each result into a map guarded by a mutex (or a sync.Map), and asserts zero duplicates. Against the old mrand.Seed(time.Now().UnixNano())+Int63() logic this fails (multiple calls land in the same nanosecond and collide); with the counter-based helper it passes. Also add TestGenerateIDUnique asserting N happy-path generateID() results are all distinct as a guard against regressions in the primary path.

**Regression risk:** Very low. The normal crypto/rand path and resulting ID format (32-hex / amq.gen.<32hex>) are unchanged; callers treat IDs as opaque strings and nothing parses the fallback format. Only the practically-unreachable fallback branch changes. Main mechanical risk is leaving the math/rand import unused, which the compiler/go vet flags immediately.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged severity: the colleague called the branch 'practically unreachable' but I verified via go doc crypto/rand.Read on the 1.26.1 toolchain that Read NEVER returns an error and crashes the process irrecoverably on OS RNG failure, so the branch is provably dead code. This strengthens the case for LOW (arguably borderline-informational), and I confirmed low is the correct ceiling — resolved in favor of low.
- Challenged a factual error in the colleague's proof/fix rationale: they claim 'time stays (still used elsewhere)'. grep shows time. appears ONLY at structures.go:213 and 230 (both fallback lines) — it is NOT used elsewhere. The fix's fallbackID helper reintroduces time.Now().UnixNano(), so the import remains valid, but the stated justification is wrong. Minor, does not affect fix correctness.
- Challenged the fix's regression-safety claim by inspecting callers: found methods.go:1786 does generateID()[:8]. The OLD fallback conn-<Int63> could be as short as 6 chars (e.g. 'conn-5') and would PANIC on [:8]. The colleague's regression analysis did NOT identify this. Their fix happens to fix it (conn-<19-digit-nanos>-<counter> is always >=8 chars), so the outcome is safe, but the analysis got there by luck rather than by identifying the constraint.
- Challenged the queue-name fallback format change: proposed amq.gen-<n>-<n> replaces the dot after 'gen' with a hyphen versus the current amq.gen.<int>. Verified this is harmless (HasPrefix('amq.') still true at authz.go:91; all chars pass the ^[a-zA-Z0-9-_.:]*$ regex in queue_name_test.go), but recommended keeping the '.' after amq.gen for convention consistency (fmt.Sprintf('amq.gen.%d.%d', ...)). Non-blocking nit.
- Challenged whether the module version claim held: colleague said 'builds with go1.26'. go.mod actually declares go 1.25.1; only the installed toolchain is 1.26.1. Immaterial to the crypto/rand behavior (identical across both), but the stated version was inaccurate.

**Fix concerns:**
- Colleague's proof asserts 'time stays (still used elsewhere)' which is factually wrong — time is used ONLY in the two fallback lines; it stays valid solely because the new fallbackID helper reuses time.Now().UnixNano(). Cosmetic rationale error, not a correctness issue.
- Regression analysis omitted the generateID()[:8] slice at methods.go:1786; the fix incidentally hardens an existing panic hazard (old 'conn-<smallInt>' could be <8 chars) but this was not called out.
- Minor consistency nit: proposed amq.gen-<nanos>-<counter> drops the conventional trailing dot of the amq.gen. prefix; harmless (authz HasPrefix('amq.') and valid-char regex both still pass) but cleaner to keep the dot.
- Because the branch is provably dead code (crypto/rand.Read cannot return an error), the fix is defensive-only; the equally-valid fail-fast/panic alternative would also be acceptable. Recommend keeping the counter approach to preserve availability, as the colleague proposed.

**Agreed resolution (authoritative):**

Confirmed real but low-severity latent defect. The math/rand-seeded fallback in generateID (structures.go:207-219) and GenerateQueueName (structures.go:226-234) is collision-prone and misuses the deprecated global mrand.Seed, but sits in an error branch that crypto/rand.Read can never trigger (Go doc: Read never returns an error and crashes irrecoverably on RNG failure), making it dead code — hence LOW. The proposed counter-based fallbackID fix is sound, complete, and regression-safe: IDs are opaque to all callers, the authz reserved-name HasPrefix('amq.') check (authz.go:91) and queue-name valid-char regex still hold, and it additionally hardens the generateID()[:8] slice at methods.go:1786 that the old fallback could have panicked on. Fix accepted with two minor nits (correct the 'time used elsewhere' rationale; optionally keep the amq.gen. trailing dot for convention). The TDD test is valid.

---

## [I2] globalDeliveryTag reconstructed from WAL-only recovery under-advances past segment-held tags, causing tag reuse/collision after restart

**Rank 26 · P3** · Severity: ⚪ Low · Category: durability · Effort: medium

> **Severity adjusted by debate:** claimed `high` → agreed `low`.

**Depends on / sequence after:** I1

### Impact
After a restart where the log-compaction pipeline has checkpointed unacked persistent messages from the WAL into segments (WAL files deleted), the global delivery-tag counter is rebuilt only from tags still present in the WAL. Any unacked message that lives only in a segment carries a delivery tag higher than any surviving WAL tag but is invisible to recovery. globalDeliveryTag is therefore set below those segment tags. New publishes then hand out delivery tags that collide with the segment-resident tags. Because delivery tags are the storage/ack key everywhere (segment ackBitmap keyed by offset==deliveryTag, deliveryIndex/getDeliveryQueues keyed by tag, GetMessage segment fallback keyed by tag), the collision corrupts ack routing and message identity: (a) GetMessage(queue, T) can fall through the ring/WAL to segments (disruptor_storage.go:312-316) and return the STALE segment message for a freshly published tag T; (b) acking the new message calls segments.Acknowledge(queue, T) and marks the OLD segment message acked (or vice versa), so a genuinely unacked persistent message is silently dropped or a new message is served twice. This is a data-integrity/durability defect that only manifests once checkpointing has run (5-minute default interval or WAL roll), i.e. on any non-trivial-uptime broker, and it compounds the separate segment-recovery gap it references (I1).

### Root cause
Recovery derives the global delivery-tag high-water mark solely from WAL contents. recoverPersistentMessages iterates GetRecoverableMessages() (server/recovery_manager.go:236-248) to compute maxDeliveryTag, then calls AdvanceDeliveryTag(maxDeliveryTag) (line 286). GetRecoverableMessages (storage/disruptor_storage.go:871-890) delegates only to wal.RecoverFromWAL(), which scans only the shared WAL directory (storage/wal_manager.go:319-349). But performCheckpoint deletes the WAL file after copying unacked messages to segments (storage/wal_manager.go:1301-1319, os.Remove(info.path) at 1302), and segment offsets equal the original delivery tags (StoreMessage passes message.DeliveryTag as the WAL offset at disruptor_storage.go:261; CheckpointBatch/writeMessageBatch preserve rm.Offset as the segment offset at segment_manager.go:295-334). So checkpointed tags are absent from the WAL scan, and there is no persisted delivery-tag counter (nothing in storage/persistent_metadata.go tracks it; grep for delivery tag / counter in that file returns nothing) and no code path feeds segment tags into AdvanceDeliveryTag (the only production caller is recovery_manager.go:286). AdvanceDeliveryTag's CAS-forward-only logic (broker/storage_broker.go:1394-1404) is correct in isolation but is fed an under-computed value.

### Proof

- **`src/amqp-go/storage/disruptor_storage.go` : 871-889** — Recovery's only source of recoverable messages is the WAL. Segments are never consulted, so tags that were checkpointed out of the WAL into segments are not returned here.

```go
func (ds *DisruptorStorage) GetRecoverableMessages() (map[string][]*protocol.Message, error) {
	if ds.wal == nil { return make(map[string][]*protocol.Message), nil }
	recoveredMessages, err := ds.wal.RecoverFromWAL()
	...
}
```

- **`src/amqp-go/storage/wal_manager.go` : 318-349** — RecoverFromWAL scans only .wal files in the shared directory. It cannot observe tags that now live exclusively in segment files.

```go
sharedDir := filepath.Join(wm.dataDir, "shared")
files, err := os.ReadDir(sharedDir)
...
for _, file := range files { if filepath.Ext(file.Name()) != WALFileExtension { continue } ... }
```

- **`src/amqp-go/storage/wal_manager.go` : 1288-1319** — performCheckpoint writes unacked messages to segments then DELETES the WAL file. Those messages (and their delivery tags) are now only in segments and invisible to RecoverFromWAL on the next restart.

```go
for queueName, queueMsgs := range byQueue { if err := segMgr.CheckpointBatch(queueName, queueMsgs); err != nil { ... } }
...
// Checkpoint succeeded — delete WAL file and clean up
_ = os.Remove(info.path)
qw.closeOldFileHandle(fileNum)
```

- **`src/amqp-go/server/recovery_manager.go` : 235-289** — maxDeliveryTag is computed only over WAL-recovered messages, then used to advance the global counter. Segment-held higher tags are excluded, so the counter under-advances.

```go
var maxDeliveryTag uint64
for queueName, messages := range recoverableMessages {
	for _, message := range messages {
		if message.DeliveryTag > maxDeliveryTag { maxDeliveryTag = message.DeliveryTag }
	...
if maxDeliveryTag > 0 { r.broker.AdvanceDeliveryTag(maxDeliveryTag) ... }
```

- **`src/amqp-go/broker/storage_broker.go` : 1394-1404** — CAS-forward-only is correct but only ever advances to the WAL-derived value; it never sees segment tags, so globalDeliveryTag can start below live segment tags.

```go
func (b *StorageBroker) AdvanceDeliveryTag(tag uint64) {
	for { current := b.globalDeliveryTag.Load(); if tag <= current { return }; if b.globalDeliveryTag.CompareAndSwap(current, tag) { return } } }
```

- **`src/amqp-go/broker/storage_broker.go` : 843-855** — New publishes draw tags by incrementing the under-advanced counter, so they reuse tags that still exist as unacked messages in segments.

```go
msgID := b.globalDeliveryTag.Add(1)
...
message.DeliveryTag = msgID
storeMsg = message
```

- **`src/amqp-go/storage/disruptor_storage.go` : 296-318** — GetMessage is tag-keyed with a segment fallback. Once a tag is reused, a lookup for the new tag can return the stale segment message, and the segment ackBitmap (keyed by the same tag) gets cross-acked, corrupting message identity/ack routing.

```go
if msg, ok := ring.ring.LoadByTag(deliveryTag); ok { return msg, nil }
if ds.wal != nil { if msg, err := ds.wal.Read(queueName, deliveryTag); err == nil { return msg, nil } }
if ds.segments != nil { if msg, err := ds.segments.Read(queueName, deliveryTag); err == nil { return msg, nil } }
```

- **`src/amqp-go/storage/segment_manager.go` : 295-334** — Segment offset == rm.Offset == original delivery tag, confirming segments are keyed by delivery tag and thus collide directly with reused tags.

```go
func (sm *SegmentManager) CheckpointBatch(queueName string, messages []*RecoveryMessage) error { ... }
...
position := qs.currentSegment.fileSize.Load() + int64(len(buf))
msgBytes := serializeSegmentMessage(rm.Message, rm.Offset)
...
updates = append(updates, indexUpdate{offset: rm.Offset, position: position})
```


### Fix

Reconstruct the delivery-tag high-water mark from ALL durable stores at recovery, not just the WAL, AND/OR persist the counter so it survives restarts independent of message lifecycle. Recommended combined fix: (1) Add a Storage method that returns the maximum durable delivery tag observed across WAL + segments, and use it in recovery to seed AdvanceDeliveryTag. (2) Belt-and-suspenders: persist the counter to the metadata store on a periodic/at-shutdown basis and load it at startup, taking max(persisted, WAL-derived, segment-derived). 

Concrete minimal fix (option 1, lowest risk): Add a method to the Storage interface, e.g. MaxDurableDeliveryTag() (uint64, error). Implement it on DisruptorStorage to take the max over (a) wal.RecoverFromWAL() offsets, (b) every segment's highest offset across all queueSegments (both acked and unacked — the counter must clear acked tags too, since an acked segment message still consumed a tag). For segments, iterate sm.queueSegments and each QueueSegments' sealed+current segments to find the max offset written; expose a SegmentManager.MaxOffset() helper that tracks the highest offset ever written. In recoverPersistentMessages (recovery_manager.go:284-289), replace the WAL-only maxDeliveryTag with max(maxDeliveryTag, storage.MaxDurableDeliveryTag()). Keep AdvanceDeliveryTag as-is (CAS-forward-only is fine). 

Stronger fix (option 2, add on top): give DisruptorStorage/broker a persisted counter. On each globalDeliveryTag.Add, no per-publish sync needed; instead persist the current value to persistent_metadata.go on WAL roll / checkpoint / graceful shutdown, and on startup seed with max(persisted, MaxDurableDeliveryTag()). This also protects the case where ALL messages were acked and both WAL+segments were compacted away (max durable tag then reads 0 and the counter would reset to 0, re-colliding with tags that may still be referenced by not-yet-flushed offset checkpoints/consumers). Recommend implementing at least option 1 now and option 2 to close the fully-drained edge case.

**Files to change:**
- `src/amqp-go/interfaces/storage.go (or wherever the Storage interface is declared)` — Add MaxDurableDeliveryTag() (uint64, error) to the Storage interface.
- `src/amqp-go/storage/disruptor_storage.go` — Implement MaxDurableDeliveryTag(): compute max over wal-recovered offsets and segments.MaxOffset(); return the maximum. Reuse GetRecoverableMessages logic for the WAL side or read offsets directly.
- `src/amqp-go/storage/segment_manager.go` — Add SegmentManager.MaxOffset() that ranges over queueSegments and each QueueSegments' segment set, returning the highest offset ever written (must include acked entries — track a per-QueueSegments maxOffset atomic updated in writeMessageBatch, since the ackBitmap only tells you which are acked, not the max).
- `src/amqp-go/server/recovery_manager.go` — In recoverPersistentMessages, after computing WAL-derived maxDeliveryTag, call storage.MaxDurableDeliveryTag() and pass the maximum of the two into AdvanceDeliveryTag (lines 284-289). Log both values.
- `src/amqp-go/storage/persistent_metadata.go (option 2)` — Optionally add persistence of the global delivery-tag high-water mark (write on checkpoint/roll/shutdown, load at startup) and expose it to the broker so the seed is max(persisted, durable-scan).

**Code sketch:**

```go
// segment_manager.go
func (sm *SegmentManager) MaxOffset() uint64 {
    var mx uint64
    sm.queueSegments.Range(func(_, v any) bool {
        qs := v.(*QueueSegments)
        if o := qs.maxOffset.Load(); o > mx { mx = o }
        return true
    })
    return mx
}
// writeMessageBatch: after computing each update.offset
//   for { cur := qs.maxOffset.Load(); if update.offset <= cur { break }; if qs.maxOffset.CompareAndSwap(cur, update.offset) { break } }

// disruptor_storage.go
func (ds *DisruptorStorage) MaxDurableDeliveryTag() (uint64, error) {
    var mx uint64
    if ds.wal != nil {
        rms, err := ds.wal.RecoverFromWAL()
        if err != nil { return 0, err }
        for _, rm := range rms { if rm.Offset > mx { mx = rm.Offset } }
    }
    if ds.segments != nil { if o := ds.segments.MaxOffset(); o > mx { mx = o } }
    return mx, nil
}

// recovery_manager.go (recoverPersistentMessages tail)
durableMax, err := r.storage.MaxDurableDeliveryTag()
if err == nil && durableMax > maxDeliveryTag { maxDeliveryTag = durableMax }
if maxDeliveryTag > 0 { r.broker.AdvanceDeliveryTag(maxDeliveryTag) }
```


**Test (TDD):** TDD in storage/log_compaction_integration_test.go (or a new recovery_tag_test.go): (1) Create a DisruptorStorage with a small SegmentSize. (2) StoreMessage persistent messages with delivery tags 1..N (DeliveryMode=2). (3) Force ds.wal.sharedWAL.rollFile() + performCheckpoint() so the WAL files are deleted and messages land in segments (mirror log_compaction_integration_test.go:139-143). (4) Assert ds.MaxDurableDeliveryTag() == N (fails today because the WAL-only path returns < N, proving the bug). (5) End-to-end variant: call GetRecoverableMessages() and assert the max tag returned is < N (documents the gap), then assert MaxDurableDeliveryTag() == N. (6) Broker-level regression: seed a StorageBroker via recovery with a segment-only tag T, then Publish and assert the new tag > T (no collision) and that GetMessage for T still resolves the original segment message, not the new publish.

**Regression risk:** Low-to-moderate. Adding a Storage interface method touches all Storage implementations and mocks (compile-time breakage is the main risk — grep for implementors of interfaces.Storage and update them, including test doubles). MaxOffset must count acked segment entries too, otherwise a queue whose highest-tag message was acked would under-report; track a monotonically-increasing maxOffset atomic in writeMessageBatch to avoid depending on the ackBitmap. Reading segments at startup is O(segment count), negligible vs existing recovery cost. The forward-only AdvanceDeliveryTag semantics are unchanged, so no risk of the counter going backwards. Option 2 (persisted counter) adds a small write on checkpoint/shutdown; guard it so a stale persisted value never lowers the seed (always take the max).

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the core impact claim by checking whether segments are reloaded at startup. Found NewSegmentManagerWithConfig (segment_manager.go:134-144) returns an empty manager and getOrCreateQueueSegments (395-441) always creates a fresh empty QueueSegments; grep confirmed no os.ReadDir/scan of .seg files at startup. This makes the colleague's stale-read/cross-ack corruption (proof items a/b) unreachable in current code -> downgraded severity high->low.
- Challenged whether a reused global tag even routes to the same per-queue segment. GetMessage/DeleteMessage are keyed by (queueName, deliveryTag); a reused tag can land on any queue. Combined with segments-not-reloaded, confirmed no live collision target exists post-restart.
- Challenged the fix's option-1 feasibility: its MaxOffset() ranges the in-memory sm.queueSegments map which is EMPTY at recovery (segments not loaded), so the fix is a no-op standalone and requires I1. Verified the proposed test reuses the same in-process ds, masking this -> flagged as false confidence.
- Challenged the codeSketch's qs.maxOffset atomic: no such field exists; per-SegmentFile.maxOffset is a plain uint64 under mutex (segment_manager.go:111,357-358). Also verified compaction (682-749) does not recompute maxOffset, so the high-water mark survives compaction (helps the fix) but the sketch is not drop-in.
- Confirmed the counter under-advance is real (not refutable): recovery_manager.go:286 is the sole production AdvanceDeliveryTag caller, fed WAL-only maxDeliveryTag, and no persisted counter exists in persistent_metadata.go. So agreeIsRealBug=true, but the harm is latent, not active.

**Fix concerns:**
- Option 1 as sketched (in-memory sm.queueSegments scan) is a no-op on a real restart because segments are not loaded from disk until I1 is implemented; it must scan .seg files on disk or use option 2.
- The proposed TDD test reuses the same in-process DisruptorStorage after forcing checkpoint, so the in-memory segment maps are still populated; it does not model a real restart and would pass without proving the standalone fix works -> false confidence. Rewrite to construct a fresh storage instance.
- codeSketch references a nonexistent qs.maxOffset atomic; actual code has per-SegmentFile.maxOffset (uint64 under segment mutex). MaxOffset() must range sealed+current segments and account for compaction not recomputing maxOffset.
- Adding MaxDurableDeliveryTag() to the Storage interface breaks all implementors and test doubles (compile-time); the fix must enumerate and update them.
- Because the active corruption is unreachable without I1, sequencing matters: this fix is prerequisite hardening to land WITH the I1 segment-reload fix; landing it alone yields little value beyond option-2's persisted counter.

**Agreed resolution (authoritative):**

Real but the impact/severity is overstated; downgraded from high to low. VERIFIED-TRUE mechanism: globalDeliveryTag is rebuilt at recovery solely from WAL contents. recovery_manager.go:230-289 computes maxDeliveryTag only over GetRecoverableMessages(), which delegates only to wal.RecoverFromWAL() (disruptor_storage.go:871-889 -> wal_manager.go:318-349 scans only shared/*.wal). performCheckpoint deletes the WAL file after copying unacked messages to segments (wal_manager.go:1302), and segments preserve DeliveryTag as the offset (segment_manager.go:332,354-358). AdvanceDeliveryTag (storage_broker.go:1394-1404) is the only production consumer and is fed the under-computed value; persistent_metadata.go has no counter persistence (grep-confirmed). So the counter DOES under-advance after checkpoint+restart. REFUTED impact: the colleague's data-integrity harms (a) GetMessage(Q,T) returning a STALE segment message for a reused tag, and (b) acking a reused tag cross-acking the OLD segment message, are UNREACHABLE in the current code. NewSegmentManagerWithConfig (segment_manager.go:134-144) returns an empty manager and does NOT scan existing .seg files; getOrCreateQueueSegments (395-441) always builds a FRESH QueueSegments with a new empty segment file (openNextSegmentLocked names files by time.Now().UnixNano(), 596), empty in-memory index, empty sealedSegments, and empty ackBitmap. readMessage (495-539) only consults in-memory currentIdx/sealedSegments. There is NO code anywhere that reloads segment files at startup. Therefore after restart, ds.segments.Read/Acknowledge operate only on segments written in the CURRENT process; a reused tag cannot resolve to or cross-ack a pre-restart segment message. The described corruption requires segment messages to be live/readable post-restart, which is exactly the capability that is missing (this is why the colleague set dependsOn=[I1]). The checkpointed segment-only messages are simply LOST at restart (subsumed by I1), so the tag-collision-corrupts-their-identity harm is moot. In isolation, I2's only real effect is cross-restart delivery-tag reuse, which has no current correctness consequence (post-restart tags are still monotonic within the session, AMQP delivery tags are per-channel and reset on reconnect anyway, and WAL-recovered tags are already covered by AdvanceDeliveryTag). Hence severity is LOW as a standalone latent/hygiene bug; it would rise to HIGH only conditionally, once I1 (segment reload at startup) exists. FIX ASSESSMENT: option 1 as sketched (MaxDurableDeliveryTag ranging in-memory sm.queueSegments -> ds.segments.MaxOffset()) is a NO-OP on a real restart because that map is empty until something reads segment files from disk (i.e., it needs I1). To work standalone it must scan segment FILES on disk, or use option 2 (persist the counter to metadata on checkpoint/roll/shutdown, seed with max(persisted, durable-scan)) which is the genuinely robust fix and correctly handles the fully-drained edge case. Also: the sketch references a nonexistent qs.maxOffset atomic (code has per-SegmentFile.maxOffset uint64 under segment mutex; note compaction at 682-749 rewrites segments without recomputing maxOffset, which happens to preserve the high-water mark). The proposed TDD test is MISLEADING: it reuses the same in-process ds after forcing checkpoint, so queueSegments is still populated and the test passes without modeling an actual restart (new process, empty in-memory map) — false confidence. AGREED: keep the fix as prerequisite hardening bundled with I1, but implement it as option 2 (persisted counter) or a genuine on-disk segment scan, and rewrite the test to spin up a fresh storage instance (empty in-memory maps) to model a true restart.

---

## [I31] Dead code bundle: message_cache/message_index (with latent makeCacheKey collision), storage/codec, inmem_transaction_store, and segment_manager checkpointLoop no-op goroutine

**Rank 27 · P3** · Severity: ⚪ Low · Category: tech-debt · Effort: small · ⚡ Quick win

### Impact
Roughly 1,700+ lines of unreachable code (protocol/message_cache.go 336L, protocol/message_index.go 303L, storage/codec.go 826L, storage/inmem_transaction_store.go 125L) ship in the binary and confuse readers/maintainers. All symbols have ZERO external references and no self-tests, so they cannot be relied upon and are not exercised by CI. The makeCacheKey implementation contains a genuine correctness defect (rune truncation + code-point-not-decimal encoding) that would cause silent cache-key collisions if this code were ever wired up — a trap for a future engineer who assumes the LRU cache/index subsystem works. Separately, segment_manager.go checkpointLoop is a per-queue goroutine that does nothing except set an unread field lastCheckpoint on every CheckpointInterval tick, wasting one goroutine + one timer per queue (N goroutines/timers for N queues) with no functional benefit. Net effect is maintainability drag, larger binary, and misleading surface area, not a runtime correctness or availability bug in the live path.

### Root cause
Incremental development left several speculative/abandoned subsystems in the tree. The LRU message cache + message index (protocol/message_cache.go, message_index.go) and the binary codec (storage/codec.go) were superseded by the DisruptorStorage/WAL/segment persistence path and never removed. InMemTransactionStore was replaced by DisruptorStorage implementing the TransactionStore interface directly. checkpointLoop in segment_manager.go was left as an explicit placeholder ("Phase 6C will implement actual checkpointing from WAL // For now, this is a placeholder") but is still spawned as a real goroutine per queue. makeCacheKey was written incorrectly (string(rune(deliveryTag)) instead of strconv.FormatUint) but the bug is masked because the code is dead.

### Proof

- **`src/amqp-go/protocol/message_index.go` : 120-125** — Real defect: string(rune(deliveryTag)) converts the uint64 tag to a single Unicode code point (rune is int32), NOT the decimal text 'deliveryTag' promised by the comment. deliveryTag values > 0x10FFFF (and any > 2^31 after truncation) map to U+FFFD (replacement char), so distinct tags collide to the same key. It is a genuine correctness bug, but latent because the function is only called from NewMessageIndex, which itself is dead.

```go
func makeCacheKey(queueName string, deliveryTag uint64) string {
	// Pre-allocate buffer to avoid allocations
	// Format: "queueName:deliveryTag"
	return queueName + ":" + string(rune(deliveryTag))
}
```

- **`src/amqp-go/protocol/message_index.go` : 66-81** — Only in-package caller of makeCacheKey. Grep across the whole module shows MessageIndex/NewMessageIndex/MessageIndexManager/MessageState have 0 external referencing files and no *_test.go, confirming the file is unreachable.

```go
func NewMessageIndex(deliveryTag uint64, message *Message, queueName string) *MessageIndex { ... CacheKey: makeCacheKey(queueName, deliveryTag) ... }
```

- **`src/amqp-go/protocol/message_cache.go` : 27-51** — grep -rln for MessageCache/NewMessageCache across --include=*.go returns only protocol/message_cache.go itself (0 external files, no test file). The entire 336-line LRU cache is dead.

```go
type MessageCache struct { ... }
func NewMessageCache(maxBytes int64) *MessageCache { ... }
```

- **`src/amqp-go/storage/codec.go` : 11-62** — MessageCodec/QueueMetadataCodec/NewMessageCodec/NewQueueMetadataCodec have 0 references outside codec.go; the real persistence path (disruptor_storage.go, wal_manager.go) does not call Serialize/Deserialize. NOTE: the claim that the header 'mislabels format CBOR' is FALSE — grep -in cbor storage/codec.go returns nothing; the header documents a custom binary 'Version 1' format. Dead-code portion of the claim is confirmed; the CBOR-mislabel portion is rejected.

```go
// MessageCodec provides binary serialization/deserialization for AMQP messages
// Wire format ... Version 1 (current)
type MessageCodec struct{}
func NewMessageCodec() *MessageCodec { ... }
```

- **`src/amqp-go/storage/inmem_transaction_store.go` : 12-22** — grep shows InMemTransactionStore/NewInMemTransactionStore have 0 external references. The TransactionStore interface (interfaces/storage.go:105) is actually implemented by DisruptorStorage (storage/disruptor_storage.go:634 func (ds *DisruptorStorage) BeginTransaction), which is the store the broker uses. InMemTransactionStore is a redundant dead implementation.

```go
type InMemTransactionStore struct { transactions map[string]*interfaces.Transaction; mutex sync.RWMutex }
func NewInMemTransactionStore() *InMemTransactionStore { ... }
```

- **`src/amqp-go/storage/segment_manager.go` : 619-637** — The tick branch only writes qs.lastCheckpoint (an unread field) then loops; there is no checkpointing work. This is a confirmed no-op loop body, matching the inline 'placeholder' comment.

```go
func (qs *QueueSegments) checkpointLoop() {
	defer qs.wg.Done()
	ticker := time.NewTicker(qs.cfg.CheckpointInterval)
	...
	case <-ticker.C:
		// Phase 6C will implement actual checkpointing from WAL
		// For now, this is a placeholder
		qs.lastCheckpoint = time.Now()
	case <-qs.stopChan:
		return
```

- **`src/amqp-go/storage/segment_manager.go` : 427-431** — checkpointLoop is spawned as one of three per-queue goroutines when a queue's segments are created, so every queue permanently runs this no-op timer loop. Its only side effect (qs.lastCheckpoint) is never read anywhere (segment_manager.go:91 declaration + :631 write only). Confirmed per-queue no-op goroutine + timer waste.

```go
segments.wg.Add(3)
	go segments.checkpointLoop()
	go segments.compactionLoop()
	go segments.batchAckLoop()
```


### Fix

Remove the dead code and neutralize the no-op goroutine, in reviewable steps.

1) Delete unused files entirely (they have zero references and no tests):
   - protocol/message_cache.go
   - protocol/message_index.go
   - storage/codec.go
   - storage/inmem_transaction_store.go
   Before deleting, run a fresh grep for every exported symbol each file defines to confirm no reference has appeared (MessageCache, CacheEntry, CacheStats, MessageIndex, MessageIndexManager, MessageState/StateReady/StateUnacked/StatePaged, makeCacheKey, MessageCodec, NewMessageCodec, QueueMetadataCodec, QueueMetadata, NewQueueMetadataCodec, InMemTransactionStore, NewInMemTransactionStore). If any single symbol IS referenced (e.g. QueueMetadata used by a wal recovery path), keep only that symbol and delete the rest; the current grep shows all are 0-external.

2) If the team prefers to preserve message_index.go as a future building block instead of deleting it, do NOT leave the makeCacheKey bug in place — replace the body with a correct decimal encoding so the latent collision cannot bite a future caller:
   return queueName + ":" + strconv.FormatUint(deliveryTag, 10)
   (add import \"strconv\"). Deletion is preferred over preservation given zero usage and the project rule against dead/bogus code.

3) segment_manager.go checkpointLoop: since it is a documented placeholder that only writes an unread field, either (a) stop spawning it until real checkpointing lands — change segments.wg.Add(3) to wg.Add(2) and drop the `go segments.checkpointLoop()` line and delete the function and the now-unused qs.lastCheckpoint field; or (b) keep the function but guard the spawn behind a config flag. Option (a) is cleanest and removes one goroutine + one timer per queue. Verify qs.lastCheckpoint has no other reader (grep confirms none) before removing the field; leave lastCheckpointOffset alone if it is used elsewhere (grep it).

4) Run gofmt, go vet ./..., and the full test suite to confirm nothing depended on these symbols. Add a lint gate (deadcode / golangci-lint 'unused') so future dead exports in internal packages are flagged.

**Files to change:**
- `src/amqp-go/protocol/message_cache.go` — Delete the file (336 lines, zero references, no tests).
- `src/amqp-go/protocol/message_index.go` — Delete the file (303 lines, zero references, no tests). If instead kept by team decision, fix makeCacheKey to use strconv.FormatUint(deliveryTag, 10) and add the strconv import.
- `src/amqp-go/storage/codec.go` — Delete the file (826 lines). MessageCodec/QueueMetadataCodec unused by the live persistence path. (Do NOT act on the claimed 'CBOR mislabel' — the header says binary 'Version 1'; no CBOR text exists.)
- `src/amqp-go/storage/inmem_transaction_store.go` — Delete the file (125 lines). TransactionStore is implemented by DisruptorStorage; this in-memory variant is unused.
- `src/amqp-go/storage/segment_manager.go` — Change wg.Add(3) to wg.Add(2) at ~line 427, remove the `go segments.checkpointLoop()` line, delete func checkpointLoop (619-637), and remove the now-orphaned lastCheckpoint field (line 91) after confirming it has no reader.
- `src/amqp-go/.golangci.yml` — Optionally enable the 'unused'/'deadcode' linters (or add a `go vet`/deadcode step in CI) to prevent regressions of unused exported/internal symbols.

**Code sketch:**

```go
// segment_manager.go — spawn site
-	segments.wg.Add(3)
-	go segments.checkpointLoop()
+	segments.wg.Add(2)
	go segments.compactionLoop()
	go segments.batchAckLoop()

// delete func (qs *QueueSegments) checkpointLoop() { ... }
// delete field: lastCheckpoint time.Time

// IF message_index.go is kept instead of deleted:
-	return queueName + ":" + string(rune(deliveryTag))
+	return queueName + ":" + strconv.FormatUint(deliveryTag, 10)
```


**Test (TDD):** Primary verification is compile + full suite: `go build ./...`, `go vet ./...`, and `go test ./...` from src/amqp-go must all pass after deletion, proving nothing referenced the removed code. For the checkpointLoop change, add TestQueueSegments_NoCheckpointGoroutine (or extend an existing segment_manager_test): capture runtime.NumGoroutine() before creating a QueueSegments, create+Close one, then assert goroutines return to baseline and that per-queue goroutine count == 2 (compaction + batchAck) rather than 3. If message_index.go is retained instead of deleted, add TestMakeCacheKey_NoCollision asserting makeCacheKey(\"q\",1) != makeCacheKey(\"q\",2) and makeCacheKey(\"q\", 1<<40) == \"q:1099511627776\" (fails on the current rune-truncation impl, passes with strconv.FormatUint).

**Regression risk:** Low. Deleting truly unreferenced files cannot break compilation (verified 0 external refs for every symbol; no self-tests). The only non-mechanical judgment is confirming qs.lastCheckpoint has no reader before removing the field (grep confirms it is write-only) and that QueueMetadata isn't consumed by a WAL recovery path (grep shows 0 external files). Risk is a hidden reflection/string-based lookup, which is not used in this codebase. Removing the checkpointLoop goroutine changes runtime goroutine/timer counts (some timing-sensitive tests could assert goroutine counts) — re-run the suite to catch that. No behavioral change to any live message/persistence path.

### Debate & agreed resolution

Investigator verdict: `confirmed-severity-adjusted` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether QueueMetadata is consumed by a WAL recovery/deserialization path (fix flags this as the one non-mechanical risk): verified all QueueMetadata references are confined to storage/codec.go itself; the live persistence path (disruptor_storage.go, wal_manager.go, segment_manager.go) does its own inline serialization and never calls MessageCodec/QueueMetadataCodec Serialize/Deserialize. Safe to delete.
- Challenged whether the fix could accidentally remove a REAL checkpoint loop: there are two checkpointLoop methods — QueueSegments.checkpointLoop (segment_manager.go:620, no-op placeholder) and QueueWAL.checkpointLoop (wal_manager.go:1228, which calls performCheckpoint() and does real WAL->segment work). Different receiver types; the fix correctly targets only the segment_manager no-op. Resolved.
- Challenged the field-removal step for name collisions: lastCheckpoint appears as time.Time in segment_manager.go:91 AND as uint64 in wal_manager.go:89, and lastCheckpointOffset (:92) is a distinct field. Verified the fix must remove only segment_manager.go:91; the fix text already calls out leaving lastCheckpointOffset alone and grep-confirming readers, which is adequate.
- Challenged regression risk from goroutine-count assertions: located NumGoroutine-style test files, but they are in server/ (ack_worker_test.go, m1_fanin_test.go), not the segment package — unlikely to be affected by dropping one QueueSegments goroutine. The fix's recommendation to add TestQueueSegments_NoCheckpointGoroutine and re-run the full suite is the correct safeguard.
- Challenged whether InMemTransactionStore is reachable via the TransactionStore interface: interfaces/storage.go:105 defines TransactionStore (embedded at :20 in a composite interface) and it is implemented by DisruptorStorage (BeginTransaction at disruptor_storage.go:634); InMemTransactionStore is never assigned to that interface anywhere. Confirmed redundant/dead.

**Fix concerns:**
- Line-count in the claim (1,700+) is slightly overstated: actual wc -l gives 336+303+826+125 = 1,590 lines. Immaterial to the fix but worth noting the number is approximate.
- The strconv.FormatUint fallback (keep message_index.go and patch makeCacheKey) should NOT be taken — it preserves ~639 lines of dead cache/index code and contradicts the project's no-dead-code rule; deletion is the correct branch. The fix already states deletion is preferred, so this is just an emphasis, not a defect.
- When removing the lastCheckpoint field, take care to touch ONLY segment_manager.go:91 (time.Time) and leave both lastCheckpointOffset (:92) and the unrelated wal_manager.go:89 lastCheckpoint uint64 intact. The fix mentions grep-confirming readers but does not explicitly warn about the same-name field in wal_manager.go.
- Enabling a golangci-lint 'unused' gate (fix step 4 / .golangci.yml) will NOT catch these particular cases automatically, since staticcheck's unused analyzer does not flag exported package-level symbols by default — the exported dead symbols (MessageCache, MessageCodec, etc.) would still pass unless the deadcode tool or a whole-program analysis is used. The lint gate is a nice-to-have but should not be relied on to have caught this.

**Agreed resolution (authoritative):**

Confirmed as tech-debt (low severity), not a runtime correctness/availability bug in any live path. Independently verified: (1) All four files are dead — grep -rln across the module for every exported symbol (MessageCache, CacheEntry, CacheStats, MessageIndex, MessageIndexManager, MessageState/StateReady/StateUnacked/StatePaged, makeCacheKey, MessageCodec, NewMessageCodec, QueueMetadataCodec, QueueMetadata, InMemTransactionStore, NewInMemTransactionStore) returns ONLY the defining file; no *_test.go, no build tags, no reflection, no interface dispatch; `go build ./...` passes. (2) makeCacheKey (protocol/message_index.go:121-124) genuinely uses string(rune(deliveryTag)) — a single Unicode code point, not the decimal text the comment promises — so distinct large tags collide; but its sole caller NewMessageIndex is dead, so the defect is latent. (3) The CBOR-mislabel sub-claim is correctly REJECTED (grep -in cbor storage/codec.go returns nothing; header documents a custom binary 'Version 1' format). (4) segment_manager.go QueueSegments.checkpointLoop (:620-637) is a confirmed no-op: tick branch only writes qs.lastCheckpoint (declared :91, written :631, zero readers), spawned per-queue via wg.Add(3) at :428-429, wasting one goroutine + one timer per queue. FIX AGREED AS SOUND: delete the four unreferenced files; for segment_manager change wg.Add(3)->Add(2), drop `go segments.checkpointLoop()`, delete the func and the write-only lastCheckpoint field. Prefer deletion of message_index.go over the strconv.FormatUint patch given zero usage and the project's no-dead-code rule. Then run gofmt, go vet ./..., go test ./... to prove nothing referenced the removed symbols.

---

## [I32] Dead interface contracts (interfaces.Server, interfaces.Broker) and wrap-then-unwrap brokerWrapper dance in server/builder.go

**Rank 28 · P3** · Severity: ⚪ Low · Category: tech-debt · Effort: small · ⚡ Quick win

### Impact
Confusing, misleading dead code and a latent API trap in the public builder. (1) interfaces.Server is never referenced anywhere and cannot be satisfied by *server.Server (signature mismatch), so any consumer trying to program against it as a contract is misled. (2) WithBroker(interfaces.Broker) is exported and looks usable, but any caller passing a real interfaces.Broker implementation gets a hard runtime failure at Build() ("unsupported broker type") — a trap, not a feature. (3) WithUnifiedBroker wraps into *brokerWrapper purely so Build() can immediately unwrap it, and brokerWrapper carries ~5 methods that only return "not supported" errors and are never invoked. (4) The Build() error message directs users to WithDefaultBroker, a method that does not exist. Net effect: extra surface area to maintain/understand, misleading public API, no runtime impact for the actually-used WithUnifiedBroker / default-broker paths.

### Root cause
interfaces.Broker and interfaces.Server were defined early as aspirational contracts, but the implementation converged on the server-package UnifiedBroker interface (server/unified_broker.go) and concrete *server.Server + LifecycleManager instead. The old interfaces were never removed. WithBroker/interfaces.Broker was retained "for backward compatibility," but no adapter from interfaces.Broker to UnifiedBroker was ever written, so Build() rejects everything except the internally-produced *brokerWrapper. The wrapper therefore exists only to smuggle a UnifiedBroker through a field typed as interfaces.Broker and be unwrapped, and its unsupported-operation methods (GetMessage/AckMessage/NackMessage/GetQueueInfo/GetExchangeInfo/GetBrokerStats) are never called.

### Proof

- **`src/amqp-go/interfaces/server.go` : 204-222** — interfaces.Server declares Start(ctx)/Stop(ctx)/Health/GetStats/GetConnections. A repo-wide grep for 'interfaces.Server\b' returns zero matches (exit code 1) — including tests — so the interface is never used as a type. There is no 'var _ interfaces.Server' assertion either.
- **`src/amqp-go/server/server.go` : 66,612** — func (s *Server) Start() error and func (s *Server) Stop() error take NO context argument, whereas interfaces.Server requires Start(ctx context.Context)/Stop(ctx context.Context). Therefore *server.Server cannot satisfy interfaces.Server; the interface's other methods (Health/GetStats/GetConnections) live on LifecycleManager (server/lifecycle.go:295,328,372), not Server. The interface matches nothing in the codebase.
- **`src/amqp-go/server/builder.go` : 113-118** — WithUnifiedBroker stores the UnifiedBroker as &brokerWrapper{...} in a field typed interfaces.Broker, with the comment 'We'll handle the type assertion in Build()'.
- **`src/amqp-go/server/builder.go` : 223-231** — Build() type-asserts b.broker.(*brokerWrapper) and immediately unwraps wrapper.unifiedBroker. The only other branch (a genuine interfaces.Broker) returns fmt.Errorf('unsupported broker type - use WithUnifiedBroker or WithDefaultBroker'). WithDefaultBroker does not exist anywhere (grep finds it only inside this error string), so the message is also stale.
- **`src/amqp-go/server/builder.go` : 107-110** — WithBroker(broker interfaces.Broker) is exported but is never called anywhere (grep '.WithBroker(' in *_test.go and repo-wide returns no external callers). Any real interfaces.Broker passed here is guaranteed to hit the 'unsupported broker type' error at line 230 — a public API trap.
- **`src/amqp-go/server/builder.go` : 368-416** — brokerWrapper.GetMessage/AckMessage/NackMessage/GetQueueInfo/GetExchangeInfo/GetBrokerStats all just return 'not supported' errors. Because the wrapper is always immediately unwrapped at Build() (lines 225-226) and never used as an interfaces.Broker at runtime, these methods are never invoked — they exist only to satisfy the interfaces.Broker method set.
- **`src/amqp-go/storage/disruptor_storage.go` : 931** — var _ interfaces.Storage = (*DisruptorStorage)(nil) — the only such compile-time assertion in the repo. This confirms interfaces.Storage is a real, enforced contract, in contrast to interfaces.Broker/interfaces.Server which have no assertions and no implementers.

### Fix

Delete the dead contracts and collapse the wrap/unwrap indirection so the builder speaks UnifiedBroker directly. Concretely: (1) Change ServerBuilder.broker field from interfaces.Broker to server.UnifiedBroker. (2) Rewrite WithUnifiedBroker to store the UnifiedBroker directly (b.broker = unifiedBroker) with no wrapper. (3) In Build(), replace the type-assert/unwrap block (lines 223-237) with a plain nil check: if b.broker != nil use it directly as the UnifiedBroker, else construct the default storage broker as today. (4) Delete brokerWrapper struct and all its methods (builder.go:330-416). (5) Delete the exported WithBroker method (builder.go:106-110) — it is unused and only ever fails; if source/binary backward-compat matters, instead keep the signature but have it return an error via the builder or panic with a clear 'use WithUnifiedBroker' message, but preferred is deletion since there are no callers. (6) Delete interfaces.Broker (interfaces/broker.go:6-35) if QueueInfo/ExchangeInfo/BrokerStats structs are unused elsewhere; grep first — GetQueueInfo/GetExchangeInfo/GetBrokerStats reference those structs, and they may be referenced elsewhere, so keep any struct that has real users and only remove the Broker interface itself. (7) Delete interfaces.Server (interfaces/server.go:204-222); keep HealthStatus/ServerStats/ConnectionInfo structs because LifecycleManager returns them (lifecycle.go:295/328/372). (8) Fix or remove the stale 'WithDefaultBroker' reference in any remaining error text. Run gofmt and go build ./... and go vet ./... after each removal to catch stragglers.

**Files to change:**
- `src/amqp-go/server/builder.go` — Change broker field type to UnifiedBroker; simplify WithUnifiedBroker to store directly; simplify the Build() broker-selection block to a nil check; delete brokerWrapper (struct + all methods, ~lines 330-416); delete unused WithBroker method (~lines 106-110).
- `src/amqp-go/interfaces/broker.go` — Delete the Broker interface (lines 6-35). Keep QueueInfo/ExchangeInfo/BrokerStats structs only if still referenced after builder cleanup; otherwise remove them too. Verify with grep before deleting each struct.
- `src/amqp-go/interfaces/server.go` — Delete the Server interface (lines 204-222). Keep HealthStatus, ServerStats, ConnectionInfo structs (used by LifecycleManager). Remove the now-unused context import if nothing else in the file uses it.

**Code sketch:**

```go
// builder.go
type ServerBuilder struct {
    // ...
    broker UnifiedBroker // was interfaces.Broker
}

func (b *ServerBuilder) WithUnifiedBroker(ub UnifiedBroker) *ServerBuilder {
    b.broker = ub
    return b
}

// in Build():
var unifiedBroker UnifiedBroker
if b.broker != nil {
    unifiedBroker = b.broker
} else {
    storageBroker := broker.NewStorageBroker(storageImpl, b.config.GetEngine())
    unifiedBroker = NewStorageBrokerAdapter(storageBroker)
}
// delete brokerWrapper entirely; delete WithBroker entirely
```


**Test (TDD):** Because this is a pure dead-code/refactor cleanup with no behavior change on the live path, the proof is compile + existing suite green plus a small guard test. Add server/builder_test.go cases: (a) TestBuild_WithUnifiedBroker_UsesProvidedBroker — construct a fake UnifiedBroker, call NewServerBuilder().WithUnifiedBroker(fake).Build(), assert no error and that srv.Broker == fake (identity), proving the wrapper removal preserves the direct-use path. (b) TestBuild_DefaultBroker — call Build() with no broker set, assert a non-nil StorageBrokerAdapter broker. Run `go build ./... && go vet ./... && go test ./...` from src/amqp-go to prove nothing referenced the deleted interfaces/methods. A grep-based CI check (grep -rn 'interfaces.Broker\|interfaces.Server\b\|brokerWrapper\|WithBroker(' returning nothing) documents completeness.

**Regression risk:** Low. The live paths (WithUnifiedBroker and the default storage broker) are unchanged behaviorally — only the intermediate wrapper is removed, and srv.Broker ends up being the same UnifiedBroker instance. Risks: (1) deleting WithBroker is a source-breaking change for any external module importing this package — but it is unusable today (always errors), so removing it only turns a runtime failure into a compile error, which is arguably better; if strict API stability is required, keep the method as a clearly-documented no-op that returns/records an error instead. (2) Deleting the QueueInfo/ExchangeInfo/BrokerStats or HealthStatus/ServerStats/ConnectionInfo structs could break LifecycleManager or metrics code — mitigated by grepping each struct before deletion and keeping the ones with real users (HealthStatus/ServerStats/ConnectionInfo are used by LifecycleManager and must stay). (3) go vet/build across the module fully covers accidental references since these are compile-time symbols.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged the colleague's proof wording that grep 'interfaces.Server\b' returns zero matches: a naive grep for 'interfaces.Server' actually returns 5 hits (interfaces.ServerConfig in config/config.go and interfaces.ServerStats in lifecycle.go). Verified the word-boundary form grep -E 'interfaces\.Server\b' does return exit 1 / zero matches, so the interface type itself is genuinely unreferenced. The proof's conclusion holds; only the raw grep needs the \b anchor to be accurate.
- Challenged whether deleting the QueueInfo/ExchangeInfo/BrokerStats structs is safe: grepped repo-wide and confirmed they are referenced ONLY inside interfaces/broker.go (the Broker interface) and builder.go's brokerWrapper — both of which the fix deletes — so they become dead and are safe to remove. Fix already says 'grep first', which is correct.
- Challenged whether removing the Server interface leaves a dangling import: confirmed context is imported in interfaces/server.go and used ONLY by Start(ctx)/Stop(ctx) at lines 206/209, so the import must be dropped or the build fails. The fix explicitly flags this, so it is complete.
- Challenged the identity-preservation claim behind the proposed test (srv.Broker == fake): verified Build() assigns unifiedBroker to server.Broker at builder.go:263 and the default path uses NewStorageBrokerAdapter (broker_adapters.go:14) which returns UnifiedBroker, so removing the wrapper preserves the exact instance. Claim holds; test is valid.
- Attempted to upgrade severity above low: rejected. Both live construction paths (WithUnifiedBroker and default storage broker) are behaviorally unchanged, no AMQP correctness or reliability path touches the dead symbols, and the only 'risk' (removing unused public WithBroker) is a source-break for a method that can never succeed today. LOW is correct.

**Fix concerns:**
- The fix is a source-breaking API change for any external importer of package server (removes exported WithBroker, and changes ServerBuilder.broker field type — though that field is unexported, so only WithBroker matters externally). Since WithBroker always fails at Build() today, this only turns a runtime failure into a compile error, which is acceptable; but if strict API stability is a stated guarantee, retain WithBroker as a documented method that returns/records an error rather than deleting it.
- When deleting QueueInfo/ExchangeInfo/BrokerStats, run go build ./... AND go vet ./... across the whole module afterward — these are exported symbols in the public interfaces package and could in principle be referenced by out-of-tree code or examples; in-tree they are dead, but the removal from a public package is technically an API change too. If in doubt, keep the three structs (they are tiny, zero-cost) and only remove the Broker interface + brokerWrapper.
- Ensure the stale 'WithUnifiedBroker or WithDefaultBroker' error text is deleted entirely (not just edited) since the whole assert/unwrap branch it lives in is being replaced by a nil check.

**Agreed resolution (authoritative):**

Confirmed real bug, severity LOW (tech-debt / misleading dead API, no runtime impact on live paths). All seven proof points independently verified against source. (1) interfaces.Server (interfaces/server.go:204-222) requires Start(ctx)/Stop(ctx) but *server.Server has Start() error/Stop() error (server/server.go:66,612) — cannot satisfy the interface; grep -E "interfaces\.Server\b" returns zero matches (exit 1); the interface's Health/GetStats/GetConnections live on LifecycleManager (lifecycle.go:295,328,372). Genuinely dead. (2) WithBroker(interfaces.Broker) (builder.go:107) has zero callers and any real Broker hits "unsupported broker type" at builder.go:230 — a public API trap. (3) WithUnifiedBroker wraps into &brokerWrapper{} (builder.go:116) only for Build() to immediately unwrap it (builder.go:225-226). (4) WithDefaultBroker referenced in the error string (builder.go:230) does not exist anywhere else — stale message. (5) brokerWrapper's GetMessage/AckMessage/NackMessage/GetQueueInfo/GetExchangeInfo/GetBrokerStats (builder.go:368-416) return "not supported" and are never invoked. (6) The only compile-time contract assertion is var _ interfaces.Storage on disruptor_storage.go:931, confirming Broker/Server have no enforced implementers. The proposed fix is sound and regression-safe: collapse the field to server.UnifiedBroker, store directly in WithUnifiedBroker, replace Build()'s assert/unwrap with a nil check, delete brokerWrapper + WithBroker + the Broker/Server interfaces. Identity preserved: Build() assigns unifiedBroker to server.Broker (builder.go:263) and the default path already uses NewStorageBrokerAdapter directly (broker_adapters.go:14 returns UnifiedBroker), so srv.Broker == the provided instance. Verified additional cleanups the fix must include (all already flagged in it): QueueInfo/ExchangeInfo/BrokerStats are referenced ONLY by the Broker interface + brokerWrapper (both deleted) so they become fully dead and can be removed; HealthStatus/ServerStats/ConnectionInfo ARE used by LifecycleManager and must stay; the context import in interfaces/server.go is used ONLY by the Server interface (lines 206,209) and must be removed or the build breaks. Baseline go build ./... passes (exit 0). Severity stays LOW — no correctness/reliability/runtime path is affected; deleting WithBroker only converts a guaranteed runtime error into a compile error for hypothetical (nonexistent) external callers, which is strictly better.

---

## [I33] ~18 dead config fields (validated/defaulted but never read) + errors package ~91% unused + 6 duplicate error-code constants

**Rank 29 · P3** · Severity: ⚪ Low · Category: tech-debt · Effort: medium

### Impact
Operators can set these config knobs (via YAML/env/builder) and Validate() will happily accept them, but the broker silently ignores every one — creating a false contract. Example concrete gaps: Network.ConnectionTimeoutMS/HeartbeatIntervalMS/TCPKeepAlive*/ReadBufferSize/WriteBufferSize are never applied to any net.Conn or listener; Server.MemoryLimitPercent/MemoryLimitBytes advertise RabbitMQ-style memory back-pressure that does not exist; Storage.RetentionMS/MessageTTL and Engine.ExpiredMessageCheckIntervalMS promise TTL/retention enforcement that is not wired (WAL RetentionPeriod is hardcoded to 0=disabled, ack cleanup uses a hardcoded 1h cutoff); Engine.OffsetCleanupBatchSize/OffsetCleanupIntervalMS/AvailableChannelBuffer are validated but read by nothing. The errors package ships a full typed error hierarchy (79 exported symbols) of which only ~7 are used outside the package, so ~91% is dead code that still must be compiled, tested, and maintained. Six error-code constants are exact duplicates (NotFound==NotFound2==404, etc.), and IsNotFound/IsPreconditionFailed/IsAccessRefused each redundantly OR two identical values. No runtime crash or data loss — this is maintainability/false-affordance debt only.

### Root cause
Config surface and the errors package were built out speculatively ahead of the features that would consume them (memory limits, TTL/retention, TCP tuning, a typed error taxonomy for every AMQP class). Those features were never implemented (or were implemented with hardcoded values / a different, smaller constant set), but the config fields, validation rules, builder setters, and error constructors were left in place. The duplicate *2 constants stem from copy-pasting the AMQP 0.9.1 reply-code table into two labeled blocks ('Connection errors' and 'Channel errors') even though the codes overlap across classes.

### Proof

- **`src/amqp-go/interfaces/config.go` : 42-53, 67-75, 130-143, 159, 257-275** — Defines the 18 fields in question (Network.ConnectionTimeoutMS/HeartbeatIntervalMS/TCPKeepAlive*/ReadBufferSize/WriteBufferSize; Storage.MessageTTL/CacheMB/MaxFiles/RetentionMS; Server.ChannelTimeoutMS/MessageTimeoutMS/MemoryLimitPercent/MemoryLimitBytes; Engine.AvailableChannelBuffer/ExpiredMessageCheckIntervalMS/OffsetCleanupBatchSize/OffsetCleanupIntervalMS). Note line 255 even self-documents ExpiredMessageCheckIntervalMS as 'Not yet implemented — reserved for future TTL enforcement feature.'
- **`src/amqp-go/config/config.go` : 24-29, 34-36, 69-73, 100-103, 153-155, 197-198, 249-262** — These fields are given defaults (DefaultConfig) and, for ConnectionTimeoutMS/AvailableChannelBuffer/ExpiredMessageCheckIntervalMS/OffsetCleanupBatchSize/OffsetCleanupIntervalMS, are fail-fast validated in Validate(). A repo-wide grep for '.<Field>' outside config/, interfaces/, and _test.go files returned zero consumer reads for all 18 fields, while sibling fields (RingBufferSize, WALBatchSize, SegmentSize, ConsumerMaxBatchSize, MaxFrameSize, CheckpointIntervalMS, WALCleanupCheckIntervalMS, Address) ARE read by broker/storage/server — proving these 18 are uniquely dead.
- **`src/amqp-go/config/builder.go` : 47-71, 88-101, 197-203** — Builder setters (WithConnectionTimeout, WithHeartbeat, WithTCPKeepAlive, WithBufferSizes, WithCacheSize, WithMaxFiles, WithRetention, WithTimeouts) write these fields but nothing downstream reads them — the setters are the only write sites outside DefaultConfig.
- **`src/amqp-go/errors/errors.go` : 36-66, 408-433** — Duplicate constants: ConnectionForced=320 & ConnectionForced2=320; InvalidPath=402 & InvalidPath2=402; AccessRefused=403 & AccessRefused2=403; NotFound=404 & NotFound2=404; ResourceLocked=405 & ResourceLocked2=405; PreconditionFailed=406 & PreconditionFailed2=406. IsNotFound checks 'amqpErr.Code == NotFound || amqpErr.Code == NotFound2' (line 412) — an OR of two equal values; same pattern for IsPreconditionFailed (421) and IsAccessRefused (430).
- **`src/amqp-go/errors/errors.go` : 1-443 (whole package)** — The package exports 79 symbols (24 consts, 11 types, 38 package funcs, 6 methods). A repo-wide grep for amqperrors.* outside the package found only 7 distinct symbols used externally: AccessRefused, PreconditionFailed, InvalidPath, NewAuthError, NewAuthorizationFailed, IsAccessRefused (in a test), and AuthError (in a doc comment). All other constructors (NewChannelNotFound, NewQueueNotFound, NewExchangeNotFound, NewMessageTooLarge, NewProtocolError, NewStorageUnavailable, NewConfigValidationError, etc.) and helpers (IsConnectionError, IsChannelError, IsNotFound, IsPreconditionFailed, GetErrorCode) appear only in errors.go and errors_test.go — ~91% dead.
- **`src/amqp-go/storage/wal_manager.go` : 37, 49, 812-814** — WAL retention uses its own RetentionPeriod field that defaults to 0 (disabled) and is never populated from Storage.RetentionMS, confirming the config's RetentionMS/MessageTTL retention knobs are inert. Similarly recovery_manager.go:304 uses a hardcoded 1h expired-ack cutoff instead of any Engine cleanup-interval config.

### Fix

Split into two independent, low-risk cleanups. (A) CONFIG — for each of the 18 fields, choose per-field between DELETE and WIRE-UP; do not leave any field validated-but-unread. Recommended: DELETE the ones with no near-term consumer (MessageTTL, CacheMB, MaxFiles, RetentionMS, MemoryLimitPercent, MemoryLimitBytes, ChannelTimeoutMS, MessageTimeoutMS, AvailableChannelBuffer, ExpiredMessageCheckIntervalMS, OffsetCleanupBatchSize, OffsetCleanupIntervalMS) and their DefaultConfig entries, Validate() branches, and ConfigBuilder setters; WIRE-UP the cheap network ones that clearly should have effect (ConnectionTimeoutMS -> SetReadDeadline on the AMQP handshake; TCPKeepAlive/TCPKeepAliveIntervalMS -> tcpConn.SetKeepAlive/SetKeepAlivePeriod at accept; ReadBufferSize/WriteBufferSize -> bufio.NewReaderSize/NewWriterSize on the connection; HeartbeatIntervalMS -> the Tune negotiation heartbeat value) IF the accompanying Tune/handshake code exists — otherwise delete them too. Whatever you delete from interfaces/config.go must also be removed from DefaultConfig, Validate, builder.go, and any assertions in config_test.go, and any documented example YAML. (B) ERRORS — collapse the 6 duplicate *2 constants: delete ConnectionForced2/InvalidPath2/AccessRefused2/NotFound2/ResourceLocked2/PreconditionFailed2 and simplify IsNotFound/IsPreconditionFailed/IsAccessRefused to a single-value comparison (e.g. `return amqpErr.Code == NotFound`). Then prune unused error constructors/types: keep AMQPError, AuthError, NewAuthError, NewAuthorizationFailed, IsAccessRefused, and the constants actually referenced by server/*.go (AccessRefused, PreconditionFailed, InvalidPath) plus whatever the tests you keep exercise; delete the exchange/queue/consumer/message/protocol/storage/config/channel/connection typed-error families that are referenced only by errors_test.go, and delete their now-orphaned tests. Run `go build ./... && go vet ./... && go test ./config/... ./errors/... ./server/... ./auth/...` after each package's changes. Because unused unexported/exported symbols in Go do not break compilation, the compiler will not catch a missed reference — rely on grep + full build/test. Do config and errors in separate commits so regressions are bisectable.

**Files to change:**
- `src/amqp-go/interfaces/config.go` — Remove struct fields for whichever of the 18 are deleted (Network.ConnectionTimeoutMS/HeartbeatIntervalMS/TCPKeepAlive/TCPKeepAliveIntervalMS/ReadBufferSize/WriteBufferSize; Storage.MessageTTL/CacheMB/MaxFiles/RetentionMS; Server.ChannelTimeoutMS/MessageTimeoutMS/MemoryLimitPercent/MemoryLimitBytes; Engine.AvailableChannelBuffer/ExpiredMessageCheckIntervalMS/OffsetCleanupBatchSize/OffsetCleanupIntervalMS). Keep any you decide to wire up.
- `src/amqp-go/config/config.go` — Delete the corresponding DefaultConfig initializers and the matching branches in Validate() (lines ~153-155, ~197-198, ~249-262 and the Storage/Server defaults) for every deleted field.
- `src/amqp-go/config/builder.go` — Delete builder setters that only touch deleted fields: WithConnectionTimeout, WithHeartbeat, WithTCPKeepAlive, WithBufferSizes, WithCacheSize, WithMaxFiles, WithRetention; trim WithTimeouts to only set fields you keep (CleanupIntervalMS is used elsewhere—verify before removing).
- `src/amqp-go/config/config_test.go` — Remove/adjust assertions referencing deleted fields (e.g. ConnectionTimeoutMS at lines 289/310, AvailableChannelBuffer at 27-52) so the suite still compiles and passes.
- `src/amqp-go/errors/errors.go` — Delete the 6 *2 duplicate constants; simplify IsNotFound/IsPreconditionFailed/IsAccessRefused to single-value comparisons; delete unused typed-error families and their constructors that are referenced only by tests (Connection/Channel/Exchange/Queue/Consumer/Message/Protocol/Storage/Config errors as applicable), keeping AuthError + NewAuthError + NewAuthorizationFailed + the constants used by server/auth.
- `src/amqp-go/errors/errors_test.go` — Delete tests that exercise removed symbols (e.g. the NotFound2/PreconditionFailed2/AccessRefused2 cases at lines ~346/358/370, and constructor tests for removed families) so the package still builds and tests pass.
- `src/amqp-go/server/*, src/amqp-go/auth/file_auth.go` — No changes expected — these use only kept symbols (AccessRefused, PreconditionFailed, InvalidPath, NewAuthError, NewAuthorizationFailed). Verify with grep after the errors edit that no removed symbol is referenced.

**Code sketch:**

```go
// errors.go — collapse duplicate constants and simplify helpers
const (
    ConnectionForced   = 320
    InvalidPath        = 402
    AccessRefused      = 403
    NotFound           = 404
    ResourceLocked     = 405
    PreconditionFailed = 406
    ContentTooLarge    = 311
    NoRoute            = 312
    NoConsumers        = 313
    FrameError         = 501
    SyntaxError        = 502
    CommandInvalid     = 503
    ChannelErrorCode   = 504
    UnexpectedFrame    = 505
    ResourceError      = 506
    NotAllowed         = 530
    NotImplemented     = 540
    InternalError      = 541
)

func IsNotFound(err error) bool {
    var amqpErr *AMQPError
    return errors.As(err, &amqpErr) && amqpErr.Code == NotFound
}
// (same single-value form for IsPreconditionFailed / IsAccessRefused)
```


**Test (TDD):** TDD, two parts. (1) errors: add TestDuplicateConstantsRemoved asserting the taxonomy is minimal and helpers still classify correctly — e.g. `require.True(t, IsNotFound(&AMQPError{Code: NotFound}))`, `require.True(t, IsAccessRefused(&AMQPError{Code: AccessRefused}))`, `require.False(t, IsNotFound(&AMQPError{Code: PreconditionFailed}))`; and a compile-time guard test that the package still builds with the *2 constants gone. (2) config: add TestConfigNoDeadFields — after `cfg := DefaultConfig()` marshal to YAML and assert the removed keys are absent, and assert Validate() no longer references them (a config that previously failed only because ConnectionTimeoutMS<=0 now validates). Also keep a codebase-guard test/CI grep (scripts) asserting every EngineConfig/NetworkConfig field validated in Validate() has at least one read outside config/interfaces/_test — this prevents regressions where a new validated-but-unread field is reintroduced. All existing suites (`go test ./...`) must stay green.

**Regression risk:** Low. Deleting truly-dead config fields cannot change runtime behavior because nothing reads them; the only observable change is that Load()/Build() will now REJECT a config file/env that still sets a removed key only if you also add strict-unknown-key handling — koanf's Unmarshal silently ignores unknown keys by default, so old YAML with removed keys will simply be ignored (no error). The main risk is a missed reference: Go does not fail to compile on an unused exported symbol, so a symbol you delete could still be referenced somewhere the initial grep missed — mitigate with `go build ./... && go vet ./... && go test ./...` across ALL packages, not just config/errors. Collapsing the *2 constants is behavior-preserving because they hold identical numeric values and the OR-simplification compares the same value. If you choose to WIRE UP the network fields instead of deleting, that carries real behavioral risk (new deadlines/keepalives on live connections) and should be a separate, tested change, not part of this cleanup.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged whether any of the 18 fields are read via an indirect path (e.g. HeartbeatIntervalMS feeding Tune negotiation): resolved against the colleague-favorable direction — heartbeat is hardcoded to 5s at server/server.go:509 with a literal 'TODO: Use negotiated heartbeat value from connection.tune', and Tune advertises a hardcoded Heartbeat:60 at connection_handlers.go:70, so the config field is genuinely inert.
- Challenged the MemoryLimit* deadness by following the server.go:220 comment referencing server/memory_monitor.go: the file does not exist anywhere in the repo (find returned nothing), and there is no memory monitor / flow-control consumer of the fields — confirming the feature was never built despite the comment claiming it was 'moved' there.
- Challenged the fix's claim that removing fields is behavior-preserving for config loading: verified Load() uses plain k.Unmarshal with no DecoderConfig/ErrorUnused, so koanf ignores unknown keys — old YAML with removed keys will not error. Claim holds.
- Challenged the fix's completeness on documentation: found docs/memory-management.md (whole file), SECURITY.md:92, and deployment/systemd/README.md:54 shipping the dead keys as documented features — none listed in filesToChange. This is a real gap that downgrades the fix to needs-revision.
- Challenged the errors external-usage list (colleague implied ~7): enumerated every amqperrors.* reference in non-test files and confirmed exactly AccessRefused, PreconditionFailed, InvalidPath, NewAuthError, NewAuthorizationFailed, AuthError plus IsAccessRefused (test only) — matches the claim; all other constructors have 0 external references.

**Fix concerns:**
- filesToChange omits documentation that constitutes the strongest part of the false-affordance: docs/memory-management.md asserts a fully implemented RabbitMQ-style memory-management/flow-control feature with --memory-limit-percent/--memory-limit-bytes flags that do not exist; must be deleted or rewritten as 'not implemented' when MemoryLimit* fields are removed. SECURITY.md:92 (connection_timeout) and deployment/systemd/README.md:54 (heartbeat_interval) similarly need cleanup.
- The stale comment at server/server.go:220 ('memoryMonitor implementation moved to server/memory_monitor.go for Phase 2') points to a file that does not exist and should be removed as part of the MemoryLimit* cleanup to avoid future confusion.
- Because Go does not error on unused exported symbols, the only safety net is grep + full build/test; the fix correctly calls for `go build/vet/test ./...` across ALL packages, but should explicitly run the full module (root at src/amqp-go) not just the four named packages, since errors is imported by auth and multiple server files (confirm_handlers, connection_handlers, authz, tx_handlers) — a missed reference there would not surface from ./errors/... alone.
- Minor: the fix says IsAccessRefused is kept because a test uses it (auth/permission_test.go:755/779), but IsNotFound/IsPreconditionFailed/GetErrorCode/IsConnectionError/IsChannelError have zero external refs and can be deleted with their tests — the fix should be explicit that IsAccessRefused stays only due to that test, or the test itself could be dropped if the helper is pruned.

**Agreed resolution (authoritative):**

Confirmed real, low-severity maintainability/false-affordance debt (not a runtime bug). Verified independently: (1) All 18 config fields have zero consumer reads outside config/interfaces/_test (grep empty for every field), while the control fields the colleague named — RingBufferSize, WALBatchSize, SegmentSize, ConsumerMaxBatchSize, MaxFrameSize, CheckpointIntervalMS, WALCleanupCheckIntervalMS — ARE genuinely read (broker/storage/server), proving these 18 are uniquely dead. Validate() fail-fast branches for dead fields confirmed at config/config.go:153-155 (ConnectionTimeoutMS), 197-198 (AvailableChannelBuffer), 249-251 (ExpiredMessageCheckIntervalMS), 257-259 (OffsetCleanupBatchSize), 261-263 (OffsetCleanupIntervalMS). interfaces/config.go:255 self-documents ExpiredMessageCheckIntervalMS as 'Not yet implemented'. (2) errors package = 79 exported symbols (24 consts + 38 funcs + 11 types + 6 methods); only 7 used externally (AccessRefused, PreconditionFailed, InvalidPath, NewAuthError, NewAuthorizationFailed, AuthError, and IsAccessRefused in a test) — every New*/Is*/GetErrorCode helper family has 0 external non-errors references. (3) Six *2 constants (ConnectionForced2/InvalidPath2/AccessRefused2/NotFound2/ResourceLocked2/PreconditionFailed2) are exact duplicates, referenced only inside errors.go + errors_test.go:346/358/370; IsNotFound/IsPreconditionFailed/IsAccessRefused OR two identical values (errors.go:412/421/430). (4) Inertness of TTL/retention/heartbeat confirmed: WAL RetentionPeriod defaults 0 and is set only in tests (never from Storage.RetentionMS); server/recovery_manager.go:304 hardcodes 1*time.Hour ack cutoff; heartbeat is hardcoded 5s at server/server.go:509 with a literal 'TODO: Use negotiated heartbeat value'; Tune advertises hardcoded Heartbeat:60 at connection_handlers.go:70. Regression-safety of the delete path holds: Load() uses plain k.Unmarshal with no ErrorUnused decoder, so koanf silently ignores removed keys in old YAML; baseline go build ./... is clean. FIX NEEDS REVISION on completeness: the filesToChange list omits shipped documentation that makes the false contract worse and must also be corrected — docs/memory-management.md claims 'StrangeQ implements RabbitMQ-style memory management with publisher flow control' and documents --memory-limit-percent/--memory-limit-bytes CLI flags/config keys that do not exist (the referenced server/memory_monitor.go file was never created — see the stale 'moved to ... for Phase 2' comment at server.go:220); SECURITY.md:92 and deployment/systemd/README.md:54 reference connection_timeout/heartbeat_interval. Deleting the fields without also removing/correcting these docs leaves the false affordance in place. With that doc cleanup added (and the CI grep-guard test the colleague already proposed), the approach is otherwise sound and low-risk. Agreed to keep the network-field WIRE-UP option out of this cleanup as a separate tested change.

---

## [I36] requeueTags ring pins consumed backing-array prefix while non-empty (transient waste + realloc churn), not unbounded

**Rank 30 · P3** · Severity: ⚪ Low · Category: performance · Effort: medium

### Impact
During sustained partial-requeue churn (the ring never drains fully to zero), the consumed head of the requeueTags backing array stays pinned. Two concrete costs: (1) Transient wasted memory proportional to the largest burst since the last reallocation. After a mass-requeue event (channel close / consumer cancel of many unacked messages) followed by slow re-consumption with a trickle keeping the ring non-empty, the entire wide backing array stays live even though len is tiny. Empirically, draining a 1,000,000-tag burst down to len=1 leaves cap=135617 (~1 MB of uint64) pinned until the next append reallocation or a full drain. (2) Allocation/copy churn: because append repeatedly reallocates once the advancing head exhausts remaining cap, a steady live-set of ~100 tags stabilizes around cap ~220 and reallocates+copies roughly every ~120 pop/push cycles, generating GC garbage on the delivery hot path. The claim wording 'retains indefinitely' overstates it: memory IS reclaimed by GC when append reallocates or when the ring hits zero (nil at line 202), so it is bounded transient waste, not an unbounded leak — hence severity stays low.

### Root cause
tryPopRequeue advances the slice header via requeueTags = requeueTags[1:] instead of using an index cursor into a fixed-capacity ring. The advancing header keeps a reference into the middle of the backing array, so the already-consumed prefix plus unused tail capacity cannot be garbage-collected until either (a) the ring fully drains and is set to nil (lines 201-203), or (b) append exhausts the remaining capacity and allocates a new array. Under steady non-empty churn neither happens promptly, so consumed slots and slack capacity remain pinned, and append reallocates/copies periodically under requeueMu.

### Proof

- **`src/amqp-go/broker/queue_dispatch.go` : 189-206** — The pop advances the slice header (requeueTags[1:]) rather than using a read index. The backing array is only released (set to nil) when the ring fully drains to zero. While the ring stays non-empty, the consumed prefix pointed past by the header remains reachable and cannot be GC'd.

```go
tag := qs.requeueTags[0]
	qs.requeueTags = qs.requeueTags[1:]
	qs.requeueCount.Add(-1)
	if len(qs.requeueTags) == 0 {
		qs.requeueTags = nil
	}
```

- **`src/amqp-go/broker/queue_dispatch.go` : 177-187** — Requeue appends to the same slice whose header has been advanced by pops. When remaining capacity (original cap minus advanced offset minus current len) is exhausted, append reallocates a new array (~2x len) and copies — periodic allocation churn on a mutex-protected path. It also re-allocates the initial 4096-slot array on every empty(nil)->non-empty transition.

```go
if qs.requeueTags == nil {
		qs.requeueTags = make([]uint64, 0, 4096)
	}
	qs.requeueTags = append(qs.requeueTags, tag)
```

- **`src/amqp-go/broker/queue_dispatch.go` : 121-140** — tryPopRequeue runs on the dispatcher hot path (Claim) and also from basic.get (storage_broker.go:1452), so the pop pattern executes on the common delivery path, making the periodic realloc/copy a hot-path cost under requeue-heavy workloads.

```go
if t, ok := qs.tryPopRequeue(); ok {
		return t, true
	}
```

- **`src/amqp-go/broker/storage_broker.go` : 1286-1361** — Mass-requeue call sites: channel close / consumer cancel / nack-requeue paths push many tags at once, which is exactly what creates a large burst that then pins a wide backing array during slow re-consumption — the worst case for this issue.

```go
queueState.Requeue(deliveryTag)
```


### Fix

Replace the reslice-and-append FIFO with an index-based ring buffer so the backing array is reused in place and consumed slots do not pin memory. Track a read-head index and a live count over a single []uint64. On Requeue: if full (count == len(buf)), grow (linearize into a 2x buffer copying live elements to index 0); otherwise write at (head+count)%len(buf) and increment count. On tryPopRequeue: read buf[head], advance head=(head+1)%len(buf), decrement count. Never reslice the backing array header; do not set it to nil on drain (keep it for reuse — this also removes the make([]uint64,0,4096) re-allocation on every empty->non-empty transition seen at lines 179-181). Optionally shrink an oversized idle buffer back toward the initial 4096 cap when count drops below cap/4 to cap idle memory after a huge burst. Keep all buffer mutation under requeueMu; requeueCount atomic stays the lock-free source of truth for the fast-path check at line 190 and must stay exactly in sync with the live count. Update Recover (242-245) and PurgeQueue (storage_broker.go:1514-1517) to reset head=count=0 (optionally release an oversized buffer) instead of requeueTags=nil, still honoring requeueCount.Store(0). Lower-risk alternative if a full rewrite is too invasive: keep the slice, add a headIdx int, pop via requeueTags[headIdx]/headIdx++, and when headIdx grows large relative to remaining live count (e.g. headIdx*2 >= len(requeueTags)) compact via copy(requeueTags, requeueTags[headIdx:]); requeueTags=requeueTags[:live]; headIdx=0 so the consumed prefix is dropped and the array reused. Recommend the ring for cleanliness.

**Files to change:**
- `src/amqp-go/broker/queue_dispatch.go` — Replace requeueTags []uint64 field with an index-based ring (requeueBuf []uint64, requeueHead int, requeueLen int). Rewrite Requeue (177-187) and tryPopRequeue (189-206) to use the ring; update Recover (242-245) to reset requeueHead=requeueLen=0 (optionally drop an oversized buffer) instead of requeueTags=nil. RequeueDepth (line 341) is unchanged (still reads requeueCount).
- `src/amqp-go/broker/storage_broker.go` — Update PurgeQueue reset block (1514-1517) and any other requeueTags=nil reset to zero the new ring cursors (requeueHead=requeueLen=0, optionally release buffer) instead of qs.requeueTags = nil, keeping requeueCount.Store(0).

**Code sketch:**

```go
// Fields (replace requeueTags []uint64):
//   requeueBuf  []uint64
//   requeueHead int
//   requeueLen  int

func (qs *QueueState) Requeue(tag uint64) {
	qs.requeueMu.Lock()
	if qs.requeueBuf == nil {
		qs.requeueBuf = make([]uint64, 4096)
	}
	if qs.requeueLen == len(qs.requeueBuf) {
		nb := make([]uint64, len(qs.requeueBuf)*2)
		for i := 0; i < qs.requeueLen; i++ {
			nb[i] = qs.requeueBuf[(qs.requeueHead+i)%len(qs.requeueBuf)]
		}
		qs.requeueBuf = nb
		qs.requeueHead = 0
	}
	qs.requeueBuf[(qs.requeueHead+qs.requeueLen)%len(qs.requeueBuf)] = tag
	qs.requeueLen++
	qs.requeueCount.Add(1)
	qs.requeueMu.Unlock()
	qs.inflight.Add(-1)
	qs.NotifyNewMessage()
}

func (qs *QueueState) tryPopRequeue() (uint64, bool) {
	if qs.requeueCount.Load() <= 0 {
		return 0, false
	}
	qs.requeueMu.Lock()
	if qs.requeueLen == 0 {
		qs.requeueMu.Unlock()
		return 0, false
	}
	tag := qs.requeueBuf[qs.requeueHead]
	qs.requeueHead = (qs.requeueHead + 1) % len(qs.requeueBuf)
	qs.requeueLen--
	qs.requeueCount.Add(-1)
	qs.requeueMu.Unlock()
	return tag, true
}
```


**Test (TDD):** Add TestDispatch_RequeueRingReuse in queue_dispatch_test.go with a test-only accessor for the ring's backing cap/len. (1) Correctness: extend/mirror TestDispatch_RequeueUnbounded (N=4096+1000) but interleave pop/push to keep the ring non-empty and assert FIFO order preserved with no duplicate redeliveries. (2) Steady-state churn: push 100, then loop 100000 times doing Claim(pop)+Requeue and assert the backing buffer cap stabilizes (no repeated growth) and allocs-per-op via testing.AllocsPerRun is ~0 after warmup — this fails today because the reslice+append reallocates roughly every ~120 cycles. (3) Burst-then-drain memory: Requeue 1,000,000 tags, pop 999,999 while trickling to keep len small, and assert the live backing buffer is bounded (not still ~1M/135K wide) — passes with the ring because slots are reused in place, fails today because the header pins the wide array.

**Regression risk:** Medium. The requeue ring is on the delivery hot path (Claim) and basic.get, and is exercised by concurrent tests (TestDispatch_ConcurrentPublishClaimRequeue, unregister_requeue_test.go). Risks: off-by-one in modular head/count arithmetic; breaking FIFO redelivery ordering; missing a reset site (Recover/PurgeQueue) leaving stale live entries after purge/recover; and losing the invariant that requeueCount (read lock-free at line 190) stays exactly in sync with requeueLen (under mutex). Mitigations: keep all buffer mutation under requeueMu exactly as today, keep requeueCount as the only lock-free signal, run existing race/concurrent tests with -race, and add the FIFO-order assertion. The optional idle-shrink logic adds risk and can be omitted for a minimal, safest change.

### Debate & agreed resolution

Investigator verdict: `confirmed-severity-adjusted` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the claim that the proposed ring makes idle memory bounded: re-implemented the exact code sketch and ran a 1M-tag burst then drained to len=1 — the ring buffer stays 1048576 wide (never shrinks), which is WORSE than today's post-drain cap of 135617 and worse than today's nil-on-drain full reclaim. The fix's test #3 asserting 'not still ~1M/135K wide passes with the ring' is false for the recommended minimal ring; it only holds if the OPTIONAL idle-shrink is added — a contradiction with the regressionRisk note that idle-shrink can be omitted. Resolution: idle-shrink must be mandatory, or test #3's memory assertion must be dropped.
- Challenged whether nil-on-drain reclaim is being silently regressed: confirmed current line 202 (requeueTags=nil on full drain) means an idle queue that fully drains reclaims ALL requeue memory today. The fix explicitly keeps the buffer on drain for reuse, trading GC reclamation for reuse. Acceptable for the churn win but the trade-off should be stated; resolved by recommending idle-shrink to cap post-burst idle memory.
- Attempted to refute the steady-state realloc-period and cap numbers as overstated: instead reproduction confirmed them almost exactly (125-cycle period vs claimed ~120, cap 224 vs claimed ~220, first realloc at cycle 3996 once initial 4096 headroom exhausts). Could not refute; colleague's numbers are accurate.
- Verified the concurrency invariant I was most worried about (requeueCount read lock-free at line 190 vs mutex-protected requeueLen): the fix mutates both under requeueMu together and re-checks rlen under the lock, matching the existing pattern; -race stress with 8 goroutines quiesced to count=rlen=0 with no data race. No refutation.
- Checked for missed reset sites: grep of requeueTags across the module confirms only queue_dispatch.go and storage_broker.go:1515 touch it, so the fix's filesToChange list is complete and no purge/recover reset is missed.

**Fix concerns:**
- Test #3 (burst-then-drain bounded-buffer assertion) will FAIL against the recommended minimal ring because a ring does not release capacity on drain — it stays the full burst width (verified: 1048576 wide after a 1M burst). Either make the optional idle-shrink mandatory or remove that assertion; do not ship the test as written with the minimal fix.
- Memory regression on the burst-then-fully-drain pattern: today's code nils the slice on full drain (line 202) reclaiming everything; the ring keeps its buffer for reuse. Without idle-shrink, a queue that once saw a huge requeue burst holds that wide buffer indefinitely afterward — worse idle footprint than today. Recommend mandatory shrink-toward-4096 when count < cap/4.
- The requeueCount atomic must be incremented/decremented under requeueMu in exact lockstep with requeueLen (as the sketch does); any refactor that moves count.Add outside the lock would break the lock-free fast-path check at line 190. Keep count mutation strictly inside requeueMu.
- Off-by-one risk in modular head/count arithmetic and the grow-linearize copy loop; the added FIFO-order and no-duplicate assertions plus running existing -race concurrent tests (TestDispatch_ConcurrentPublishClaimRequeue/_Race, unregister_requeue_test.go) are necessary regression guards — I verified the sketch's arithmetic is correct, but the real edit must be re-verified against these tests.

**Agreed resolution (authoritative):**

Real but low-severity performance issue, confirmed by reading src/amqp-go/broker/queue_dispatch.go:177-206. tryPopRequeue advances the slice header via requeueTags = requeueTags[1:] instead of an index cursor, and Requeue appends to that advanced header. Two verified costs under sustained partial-requeue churn (ring never fully drains): (1) periodic append realloc+copy on the delivery hot path, and (2) transient wide-array pinning after a mass-requeue burst. I reproduced both empirically with a faithful sim of the exact code: steady-state live-set ~100 reallocates every 125 cycles at cap ~224 (colleague said ~120 cycles / cap ~220 — essentially exact); draining a 1,000,000-tag burst to len=1 pins cap=135617 (~1MB) exactly as claimed. The 'not unbounded' framing is correct: line 202 sets requeueTags=nil on full drain and append reallocation drops the old array to GC, so it is bounded transient waste, not a leak. Severity 'low' stands (no correctness impact, no unbounded growth). The index-based ring fix is functionally correct: I re-implemented the exact code sketch and verified under -race that FIFO order is preserved across the grow/linearize path, steady-state churn does zero reallocations (cap stable at 4096), the requeueCount/requeueLen invariant holds, and concurrent 8-goroutine stress quiesces to count=rlen=0. Fix needs one revision before landing: the proposed test #3 ('assert the live backing buffer is bounded, not still ~1M/135K wide — passes with the ring') is WRONG for the recommended minimal ring. A ring never releases capacity on drain, so after a 1M burst the buffer stays 1,048,576 wide (verified) — actually a memory REGRESSION versus today's nil-on-drain behavior which reclaims everything and versus today's 135617 post-drain cap. That test only passes if the OPTIONAL idle-shrink is implemented, which the fix lists as optional and the regressionRisk section says can be omitted — a self-contradiction. Agreed resolution: adopt the index-based ring (correctly eliminates the hot-path realloc churn — the primary, recurring cost), but EITHER (a) make the idle-shrink mandatory and keep test #3, OR (b) drop test #3's bounded-buffer assertion and accept that idle memory after a huge burst is unchanged/slightly worse, keeping only the steady-state no-realloc assertion (test #2) which is the real win. Recommend (a) shrink-when-count<cap/4-back-toward-4096 so both the churn and the burst-idle costs are fixed. Field/reset-site inventory in the fix is complete (only queue_dispatch.go field+Requeue+tryPopRequeue+Recover:243 and storage_broker.go PurgeQueue:1515; RequeueDepth:341 reads requeueCount and is unaffected).

---

## [I25] Load-bearing pooled-object aliasing invariant (stored Message.Body/Headers alias recycled PendingMessage) is undocumented at the fragile sites and untested

**Rank 31 · P3** · Severity: ⚪ Low · Category: tech-debt · Effort: small · ⚡ Quick win

### Impact
No current runtime defect. This is a latent-fragility / tech-debt gap: correctness of the publish hot path depends on an implicit, undocumented invariant that the byte-slice backing Message.Body and the map backing Message.Headers are NEVER recycled or mutated after processCompleteMessage returns. The stored *Message (in the in-memory disruptor ring) aliases the PendingMessage's Body slice and Header.Headers map. Today safety holds only because (a) PutPendingMessage sets Body=nil and PutContentHeader does *h=ContentHeader{} (drop references, not recycle), and (b) the inbound Body is a plain make([]byte,...) and Headers is a freshly-decoded map — neither is pooled. The moment a future perf change wires the already-present-but-unused GetMediumBodyBuffer/PutMediumBodyBuffer into the body path (or reuses/mutates the Headers map), every message still sitting in the in-memory ring gets its Body/Headers silently corrupted or zeroed. Because the WAL path serializes to bytes, only the fast in-memory (transient / not-yet-spilled) messages are affected, so such a regression would be intermittent and extremely hard to diagnose. There is also a related pre-existing hazard: fanout shallow-copy (msgCopy := *message) makes all fanned-out stored copies share ONE Headers map, so any future in-place Headers mutation corrupts sibling deliveries.

### Root cause
processCompleteMessage constructs Message by directly aliasing pooled PendingMessage state (Body=pendingMsg.Body, Headers=pendingMsg.Header.Headers), and StorageBroker.PublishMessage stores that pointer (i==0) or a shallow copy (i>0) into the disruptor ring, which retains the pointer without deep-copying. The safety of returning the PendingMessage/ContentHeader to their sync.Pools immediately afterward (basic_handlers.go:199-201, 265-267) rests entirely on the fact that no pool currently recycles the underlying Body array or Headers map. That invariant is real and load-bearing but is neither documented at the pool Put sites / buffer_pool.go, nor enforced by any test.

### Proof


### Fix

Convert the implicit invariant into an explicit, documented, and tested contract. Three coordinated changes: (1) DOCUMENT the invariant at every fragile site so a future optimizer is warned. Add a comment at the Message construction in processCompleteMessage (basic_handlers.go:296) stating that Body/Headers alias pooled PendingMessage/ContentHeader state and that the broker retains this pointer, therefore neither the Body byte array nor the Headers map may be recycled or mutated after processCompleteMessage returns. Add a mirrored warning comment above PutPendingMessage (buffer_pool.go:225), PutContentHeader (buffer_pool.go:245), and PutMediumBodyBuffer (buffer_pool.go:104) explaining WHY they must only drop references / must not be wired into the inbound publish Body path. (2) HARDEN storage_broker.go:852-856: for fanout tails, deep-copy the Headers map (and optionally the Body) into each msgCopy so sibling stored deliveries never share mutable state — this removes the second, sharper hazard and makes the fanout path robust to any future in-place Headers mutation. Update the storage_broker comment block (847-855) to reference the full invariant rather than only the i==0 allocation note. (3) TEST the invariant with a TDD regression test that (a) publishes a real message through processCompleteMessage into an in-memory broker, then explicitly zeroes/overwrites the source PendingMessage.Body and Header.Headers (simulating pool recycling / mutation), and asserts the message subsequently retrieved from storage still has the correct Body and Headers; and (b) a fanout test that publishes one message to N bound queues, mutates one stored delivery's Headers, and asserts the other deliveries are unaffected. If the team prefers full defensiveness over documentation-only, the strongest variant of (1) is to make processCompleteMessage own its Body/Headers by copying them out of the pooled objects before Put — but that adds an allocation on the hot path, so the recommended minimal fix is document+test+fanout-deep-copy, keeping the i==0 zero-copy fast path intact behind an explicit contract.

**Files to change:**
- `src/amqp-go/server/basic_handlers.go` — Add an INVARIANT comment above the Message literal at line 296 documenting that Body/Headers alias pooled state and must not be recycled/mutated after PublishMessage returns. Optionally add a one-line reminder at the pool-Put block (lines 199-201 and 265-267).
- `src/amqp-go/protocol/buffer_pool.go` — Add WHY-comments above PutPendingMessage (225), PutContentHeader (245), and PutMediumBodyBuffer (104) stating the array/map must not be recycled into the inbound publish Body/Headers path because the broker retains aliases.
- `src/amqp-go/broker/storage_broker.go` — In the fanout tail (852-856) deep-copy the Headers map (and optionally Body) per stored copy so sibling deliveries do not share mutable state. Expand the comment block (847-855) to describe the full pooled-aliasing invariant, not just the i==0 allocation note.
- `src/amqp-go/server/basic_handlers_alias_test.go` — New test file: add TestProcessCompleteMessage_StoredBodyHeadersSurvivePoolRecycle and TestFanout_StoredDeliveriesDoNotShareHeaders (or add to broker package if broker-level API is easier to drive).

**Code sketch:**

```go
// basic_handlers.go, above line 296:
// INVARIANT: message.Body and message.Headers alias the pooled
// PendingMessage / ContentHeader backing storage. The broker stores this
// pointer (and, for fanout, shallow copies of it) into the in-memory ring
// WITHOUT deep-copying. Therefore, after PublishMessage returns, the pooled
// objects may be recycled ONLY by dropping references (Body=nil, *h={}); the
// underlying byte array and map MUST NOT be reused or mutated. Do NOT wire
// GetMediumBodyBuffer/PutMediumBodyBuffer into pendingMsg.Body without also
// deep-copying Body here.
message := &protocol.Message{ Body: pendingMsg.Body, Headers: pendingMsg.Header.Headers, ... }

// storage_broker.go, fanout tail (replace shallow-copy block):
} else {
    msgCopy := *message
    if message.Headers != nil {
        h := make(map[string]interface{}, len(message.Headers))
        for k, v := range message.Headers { h[k] = v }
        msgCopy.Headers = h
    }
    msgCopy.DeliveryTag = msgID
    storeMsg = &msgCopy
}

// buffer_pool.go, above PutMediumBodyBuffer:
// WARNING: Do NOT use this pool for inbound publish message bodies. Published
// Message.Body is aliased by messages stored in the broker ring; recycling the
// array here would corrupt in-flight stored messages. See processCompleteMessage.
```


**Test (TDD):** TDD regression test (server or broker package): (1) Build an in-memory StorageBroker, declare a durable-off queue bound to the default/direct exchange. Construct a PendingMessage with a known Body ([]byte("payload")) and Headers ({"k":"v"}). Call srv.processCompleteMessage(conn, ch, pendingMsg) (there is an existing makePendingMessage helper at transaction_integration_test.go:47 and many callers to mirror). After it returns, simulate pool recycling/mutation by overwriting the SOURCE arrays: for i := range pendingMsg.Body { pendingMsg.Body[i] = 0 } and delete(pendingMsg.Header.Headers, "k"). Then retrieve the stored message from the broker/storage (GetMessage / consume) and assert its Body still equals "payload" and Headers still contains {"k":"v"}. With the current code this test PASSES (documenting/locking the invariant); if a future change wires body pooling in, the test FAILS — which is the regression guard the claim asks for. (2) Fanout test: publish one message to 3 bound queues, retrieve all three stored copies, mutate copy[0].Headers, and assert copy[1]/copy[2] Headers are unchanged (fails today because of the shared shallow-copied map; passes after the deep-copy fix).

**Regression risk:** Documentation and test additions carry essentially zero runtime risk. The only behavioral change is the fanout Headers deep-copy in storage_broker.go, which adds one small map allocation per fanout-tail delivery (i>0 only; the common single-queue and i==0 path is untouched, so no hot-path regression for the dominant case). Deep-copying is a strict correctness improvement and cannot break existing consumers. Risk is limited to the fanout allocation cost, which is negligible relative to the per-message storage work and only occurs for multi-queue fanout.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged whether this is a 'real bug': verified there is NO runtime defect today — both the pool-recycle corruption and the fanout shared-map corruption require a FUTURE code change to trigger. Confirmed via grep that no production path mutates a stored Message.Headers in place (only codec.go:375, on a freshly-allocated decode map). Resolved: agree it is a real, load-bearing, undocumented+untested INVARIANT worthy of a doc+test guard, correctly categorized as low/tech-debt rather than an active bug.
- Challenged the severity for possible upgrade: could a current code path already corrupt in-ring messages? Verified inbound Body is fresh make([]byte)+append (basic_handlers.go:179,234) and Headers is freshly decodeFieldTable'd (content.go:104, methods.go:706) — neither is pooled/recycled, and Put sites only drop references. No current corruption path exists. Resolved: low severity stands, no upgrade.
- Challenged the completeness of the fix's fanout deep-copy: it copies Headers but explicitly leaves Body shared ('optionally Body'). Confirmed Body is read-only in delivery/serialization, so leaving it shared carries the same latent fragility as i==0 — the deep-copy only closes the Headers hazard (the one with a codec.go:375 in-place-write precedent). Resolved: acceptable for a low-severity item, but the invariant documentation must cover Body as well, since the doc is the actual load-bearing mitigation and the fanout copy is secondary hardening.
- Challenged test feasibility: there is no broker-level GetMessage on StorageBroker (only on DisruptorStorage). Confirmed the proposed test can still be driven via srv.processCompleteMessage + makePendingMessage (transaction_integration_test.go:47) then reading back through the storage layer's GetMessage/consume — matching existing test patterns. Resolved: test is feasible as described.

**Fix concerns:**
- Fanout Headers deep-copy is a hazard-prevention change, not a bug fix — it will not change any observable behavior until some future code mutates a stored message's Headers in place (none does today). It should be framed as forward-defensive hardening, not a correctness bug fix.
- The fix leaves Body aliasing entirely unaddressed (i==0 store and all fanout Body slices still alias pooled/inbound memory). The documentation must explicitly state the Body invariant, not just the Headers one, or the primary latent hazard (wiring GetMediumBodyBuffer into the inbound Body path) remains under-signposted.
- The value of this fix rests almost entirely on the documentation + regression test; the code change is minor. Reviewers should not over-weight the storage_broker.go edit — the doc comments at basic_handlers.go:296 and the buffer_pool.go Put sites plus the TDD guard are what actually protect against the future regression.

**Agreed resolution (authoritative):**

Confirmed as a real, load-bearing, undocumented + untested invariant (tech-debt / latent fragility), NOT a runtime defect today. Verified in source: processCompleteMessage (server/basic_handlers.go:296-298) constructs the stored *Message by directly aliasing pendingMsg.Body and pendingMsg.Header.Headers; StorageBroker.PublishMessage (broker/storage_broker.go:846-856) stores that pointer for i==0 and a shallow copy (shared Headers map + shared Body) for fanout tails; DisruptorStorage.StoreMessage (storage/disruptor_storage.go:275) puts the *Message pointer straight into the in-memory ring and GetMessage/LoadByTag returns that same pointer. The WAL path (ds.wal.Write) serializes to bytes, so only in-ring transient messages alias — matching the claim. Safety holds today ONLY because PutPendingMessage sets Body=nil (buffer_pool.go:218,228), PutContentHeader does *h=ContentHeader{} (line 246) — drop references, not recycle — and the inbound Body is a fresh make+append while Headers is a freshly decodeFieldTable'd map (protocol/content.go:60,104). Independently verified GetMediumBodyBuffer/PutMediumBodyBuffer are genuinely unused (zero references outside buffer_pool.go; GetBufferForSize only routes outbound delivery frames). The fanout shared-Headers-map hazard is real but purely LATENT: grep confirms NO in-place Headers mutation anywhere in production (the only Headers[...]= write is storage/codec.go:375, populating a freshly-make'd map during WAL/segment decode of a distinct Message). Agreed final position: low severity, tech-debt, no defect today. The recommended fix (document the invariant at basic_handlers.go:296 and the pool Put sites in buffer_pool.go; deep-copy the fanout Headers map in storage_broker.go; add TDD regression tests for pool-recycle survival and fanout Headers isolation) is sound and essentially zero runtime risk (one small map alloc only on fanout tails i>0; single-queue/i==0 hot path untouched). Recommended refinements: (a) the fanout Headers deep-copy is cosmetic until some future code mutates a stored message's Headers in place — it prevents a hazard rather than fixing a bug, and should be labeled as such; (b) the fix leaves Body aliasing untouched (both i==0 store and all fanout Body slices still alias pooled/inbound memory), so the documentation MUST explicitly cover the Body invariant too, not just Headers — the doc+test are the load-bearing parts of this fix, and the fanout deep-copy is a secondary hardening.

---

## [I34] Two ~6.9MB compiled crash_test_tool binaries committed to git (~13.9MB build artifacts, one a stray duplicate)

**Rank 32 · P3** · Severity: ⚪ Low · Category: tech-debt · Effort: trivial · ⚡ Quick win

### Impact
The two committed Mach-O arm64 executables add ~13.9MB of uncompressed binary data to the repository (and permanently to git history via commit 389ca66). This bloats every clone/fetch, cannot be meaningfully diffed or reviewed, and is redundant because the tool is trivially rebuildable from cmd/crash_test_tool/main.go via `go build`. The root-level src/amqp-go/crash_test_tool copy is a stray build output with no adjacent source — it is architecture-specific (arm64) so it is useless to anyone on amd64/Windows and will silently rot as main.go evolves. No runtime, correctness, or security impact — purely repo hygiene / developer-experience cost, hence low severity.

### Root cause
The compiled binaries were accidentally committed (default `go build` in a package directory emits an executable named after the directory, and a bare `go build ./cmd/crash_test_tool` from the module root writes crash_test_tool into the cwd). Neither .gitignore excludes the extensionless filename `crash_test_tool`: the repo-root .gitignore covers *.exe/*.so/bin/dist/amqp-server but not this name, and src/amqp-go/.gitignore covers amqp-go/amqp-server/*.test but not crash_test_tool. So `git add` picked them up and they shipped in commit 389ca66 (feat: Complete storage rewrite ...).

### Proof

- **`(git index)` : n/a** — `git ls-files -s` shows both paths are tracked as executable (mode 100755) blobs. `git cat-file -s` reports each blob is 6948210 bytes (~6.9MB), ~13.9MB combined. `file` identifies both on disk as 'Mach-O 64-bit executable arm64'.

```go
100755 ff3e213325b6f02a429bd1933482168e76f5487d 0	src/amqp-go/cmd/crash_test_tool/crash_test_tool
100755 1289df097020320f4b94d72bcf04081b64760cc2 0	src/amqp-go/crash_test_tool
```

- **`src/amqp-go/cmd/crash_test_tool/main.go` : 1-30** — The Go source for the tool is present and self-contained, so the binary is trivially rebuildable on demand — committing the compiled output provides no value.

```go
package main

import (
	"flag"
...
	amqp "github.com/rabbitmq/amqp091-go"
)
```

- **`.gitignore` : 1-40** — Repo-root .gitignore has no pattern matching the extensionless filename `crash_test_tool`, so nothing prevents the binaries from being tracked.

```go
*.exe
*.so
...
bin/
dist/
/amqp-go
/amqp-server
amqp-server-*
```

- **`src/amqp-go/.gitignore` : 1-15** — The module-level .gitignore lists specific build-output names (amqp-go, amqp-server, benchmark binaries) but omits crash_test_tool, confirming the ignore rules simply never covered this artifact. `git check-ignore` returns no match for either path.

```go
amqp-go
amqp-server
/dist
/bin
/benchmark/benchmark
/benchmark/perftest
```

- **`(git log)` : n/a** — `git log --oneline -- <both paths>` shows both binaries entered the repo in a single unrelated feature commit, confirming an accidental inclusion rather than an intentional vendored artifact.

```go
389ca66 feat: Complete storage rewrite with three-tier architecture and YAML config
```


### Fix

1) Remove both binaries from tracking without deleting the source: `git rm --cached src/amqp-go/cmd/crash_test_tool/crash_test_tool src/amqp-go/crash_test_tool` (use plain `git rm` for the root-level src/amqp-go/crash_test_tool since it has no source and should not exist at all). 2) Add ignore rules so they cannot be re-committed. In src/amqp-go/.gitignore add an entry for the tool binaries; the safest patterns are both the specific name and the path: add lines `crash_test_tool` and `/cmd/crash_test_tool/crash_test_tool`. Because the same name can be emitted at the module root, also add `/crash_test_tool`. Mirror this in the repo-root .gitignore (add `crash_test_tool`) as defense-in-depth. 3) (Optional but recommended) Document/support on-demand builds: add a Makefile target or a line in build.sh, e.g. `go build -o bin/crash_test_tool ./cmd/crash_test_tool`, so contributors build into the already-ignored bin/ directory instead of the package dir. 4) Commit the removal. Note: this only stops future bloat; the ~13.9MB stays in history. History rewrite (git filter-repo --path src/amqp-go/crash_test_tool --path src/amqp-go/cmd/crash_test_tool/crash_test_tool --invert-paths) is optional and higher-risk (requires force-push and coordination with all clones); given low severity, recommend NOT rewriting history unless repo size becomes a concern.

**Files to change:**
- `src/amqp-go/cmd/crash_test_tool/crash_test_tool` — git rm --cached to untrack this 6.9MB compiled binary (keep on local disk optionally; do not delete main.go)
- `src/amqp-go/crash_test_tool` — git rm (untrack AND delete) — stray build artifact at a path with no source
- `src/amqp-go/.gitignore` — Add patterns `crash_test_tool`, `/cmd/crash_test_tool/crash_test_tool`, and `/crash_test_tool` so the binaries cannot be re-added
- `.gitignore` — Add `crash_test_tool` as a defense-in-depth ignore at repo root
- `build.sh` — Optional: add/document `go build -o bin/crash_test_tool ./cmd/crash_test_tool` so the tool is built on demand into the ignored bin/ dir

**Code sketch:**

```go
# from module root src/amqp-go
git rm --cached cmd/crash_test_tool/crash_test_tool
git rm crash_test_tool            # stray duplicate: delete outright, no source here

# src/amqp-go/.gitignore (append)
# crash_test_tool CLI build output
crash_test_tool
/cmd/crash_test_tool/crash_test_tool
/crash_test_tool

# optional: build into ignored bin/ on demand
#   go build -o bin/crash_test_tool ./cmd/crash_test_tool
```


**Test (TDD):** Add a lightweight repo-hygiene guard that fails CI if compiled binaries reappear. TDD: (a) write a test/script asserting `git ls-files 'src/amqp-go/**/crash_test_tool' 'src/amqp-go/crash_test_tool'` returns empty — run it BEFORE the fix to see it fail, then after `git rm` to see it pass. (b) Verify ignore rules with `git check-ignore src/amqp-go/cmd/crash_test_tool/crash_test_tool` (must now print the path) and by running `go build ./cmd/crash_test_tool` from src/amqp-go and confirming `git status --porcelain` shows no new tracked/untracked binary (i.e., the freshly built artifact is ignored). Optionally codify as a shell test in scripts/ or a `go test` in a hygiene package that shells out to `git ls-files` and fails on any tracked file whose git blob size exceeds e.g. 1MB and is not a known-good extension.

**Regression risk:** Very low. Removing untracked-from-index build artifacts does not affect any Go build, import path, or runtime behavior — the source (main.go) is untouched and the tool rebuilds identically with `go build`. The only user-visible change is that `git pull` will delete the two local binary files from working copies (harmless; they are regenerable). No history rewrite is proposed, so no force-push or clone-invalidation risk. The added .gitignore patterns are scoped to the exact filename and paths, so they will not mask any source file (crash_test_tool has no source with that bare name; main.go is unaffected).

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged 'one a stray duplicate': verified the two blob hashes differ (ff3e213... vs 1289df0..., cmp differs at char 217) despite both being exactly 6,948,210 bytes. They are two distinct arm64 builds, not a byte-identical duplicate — wording should be corrected but the substance (root copy is a stray) holds.
- Challenged the proposed bare `crash_test_tool` .gitignore pattern for over-matching. Built a throwaway repo and proved a bare `crash_test_tool` line matches the directory cmd/crash_test_tool/ too: check-ignore reports cmd/crash_test_tool/main.go as ignored (exit 0) and `git add cmd/crash_test_tool/main.go` is blocked with 'paths are ignored'. This is a regression the fix would introduce. Resolved by mandating only the path-anchored patterns (/cmd/crash_test_tool/crash_test_tool and /crash_test_tool), which I verified ignore both binaries while keeping main.go visible and addable.
- Challenged whether the root binary is truly a stray (no adjacent source). Confirmed there are zero non-test .go files at module root; go list reports package main only because the _test.go files declare package main, so `go build .` does not produce a crash_test_tool binary there. The root artifact has no legitimate build path — git rm (outright delete) is the right call.
- Verified severity is not undersold: these are the only tracked blobs >1MB and there is no runtime/correctness/security dimension, so LOW / tech-debt is accurate; no upgrade warranted.

**Fix concerns:**
- The bare `crash_test_tool` .gitignore pattern in the codeSketch and filesToChange MUST be removed — it ignores the cmd/crash_test_tool/ directory including main.go and blocks git add of the source. Use only anchored patterns: /cmd/crash_test_tool/crash_test_tool and /crash_test_tool in src/amqp-go/.gitignore.
- Repo-root .gitignore should use paths relative to root (/src/amqp-go/crash_test_tool and /src/amqp-go/cmd/crash_test_tool/crash_test_tool). A bare `crash_test_tool` at repo root has the same directory-swallowing hazard for cmd/crash_test_tool/main.go and would be a redundant/over-broad rule.
- Cosmetic: 'stray duplicate' is inaccurate — the two blobs are not byte-identical (verified different hashes / cmp mismatch), they are two distinct same-size arm64 builds. Impact description should say 'a second, redundant build of the same tool at a source-less path' rather than 'duplicate'.
- History bloat (~13.9MB) remains in commit 389ca66; agree with NOT rewriting history at this severity, but the fix's own note that the size 'stays in history' should be surfaced to reviewers so expectations on clone size are set.

**Agreed resolution (authoritative):**

Real bug, correctly rated LOW / tech-debt. Independently verified: both src/amqp-go/cmd/crash_test_tool/crash_test_tool and src/amqp-go/crash_test_tool are tracked at mode 100755, each blob is exactly 6,948,210 bytes (git cat-file -s), 13,896,420 bytes (~13.9MB) total, both Mach-O arm64 executables, both introduced in commit 389ca66, and git check-ignore returns no match for either. main.go is tracked and self-contained, so the binaries are trivially rebuildable — no runtime/correctness/security impact, purely repo hygiene. The module root has no non-test .go source (all root .go files are package main _test.go), so the root-level crash_test_tool is a genuine stray with no build path producing that name there; git rm (delete) is correct. Fix DIRECTION is right (git rm --cached the cmd binary, git rm the stray root binary, add ignore rules, no history rewrite given low severity) but the .gitignore portion NEEDS REVISION: the proposed bare `crash_test_tool` pattern must be dropped. I empirically proved a bare `crash_test_tool` line also matches the DIRECTORY cmd/crash_test_tool/, which silently ignores cmd/crash_test_tool/main.go and blocks `git add` of the source (git errors: 'paths are ignored ... cmd/crash_test_tool'). Use ONLY the path-anchored patterns the colleague also proposed: `/cmd/crash_test_tool/crash_test_tool` and `/crash_test_tool` in src/amqp-go/.gitignore (verified to ignore both binaries while leaving main.go visible and addable). At repo-root .gitignore, anchor as `/src/amqp-go/crash_test_tool` and `/src/amqp-go/cmd/crash_test_tool/crash_test_tool` rather than a bare name. Minor wording nit: 'one a stray duplicate' — the two blobs are NOT byte-identical (different hashes, cmp differs at char 217), they are two distinct arm64 builds of the same tool that happen to share a size; the root copy is stray, not a duplicate. The suggested CI hygiene guard (fail on tracked blobs >1MB) is a reasonable optional addition.

---

## [I17] Multi-step SASL challenge/response (connection.secure / connection.secure-ok) is unimplemented

**Unranked by council · P3** · Severity: ⚪ Low · Category: protocol-compliance · Effort: large

> ⚠️ **Confirmed real by the debate but omitted from the council's explicit ranking** — placed here by agreed severity.

### Impact
The broker cannot perform any multi-round SASL exchange. AMQP 0.9.1 authentication is a challenge/response loop: after connection.start-ok the server MAY send connection.secure (10.20) with a challenge, to which the client replies connection.secure-ok (10.21) with a response, repeating until the mechanism completes. This broker only supports single-shot mechanisms (all credentials delivered in the connection.start-ok "response" field). Consequently, multi-round mechanisms — SCRAM-SHA-1/256, GSSAPI/Kerberos, EXTERNAL-with-challenge, or any custom challenge/response mechanism — cannot be added or negotiated: even if such a Mechanism were registered, the server has no way to emit a challenge or consume a secure-ok, and if a client ever sent a secure-ok frame (10.21) during the post-handshake loop it would fall into processConnectionMethod's default branch and be treated as an unknown method, erroring the connection. Real-world impact is minimal today because the only registered mechanisms are PLAIN and ANONYMOUS, both single-round, and every mainstream AMQP 0.9.1 client (Go amqp091, Java, Pika, etc.) uses PLAIN. The gap is a spec-completeness/extensibility limitation, not a defect affecting current PLAIN/ANONYMOUS flows.

### Root cause
The SASL layer was designed as single-round only. The auth.Mechanism interface exposes just Authenticate(response []byte, authenticator) (*User, error) — it consumes the full response in one call and returns a terminal user, with no way to return an intermediate challenge or maintain per-connection SASL state across rounds. Correspondingly, the connection state machine in processConnectionFrames drives a fixed linear handshake (start -> start-ok -> tune -> tune-ok -> open -> open-ok) and handleConnectionStartOK finalizes authentication in one step. The ConnectionSecure=20 / ConnectionSecureOK=21 method IDs are declared in protocol/methods.go but have no corresponding *Method structs, no Serialize/Parse functions, no sender (there is no sendConnectionSecure), and no case in processConnectionMethod.

### Proof

- **`src/amqp-go/protocol/methods.go` : 12-13** — The method-ID constants for the SASL challenge/response pair exist. A repo-wide grep for ConnectionSecure/ConnectionSecureOK returns ONLY these two lines — there are no ConnectionSecureMethod / ConnectionSecureOKMethod struct definitions, no Serialize, and no Parse functions anywhere in the codebase, proving the wire methods are never encoded or decoded.

```go
ConnectionSecure   = 20
ConnectionSecureOK = 21
```

- **`src/amqp-go/server/connection_handlers.go` : 314-338** — processConnectionMethod handles methods 11, 31, 41, 50 but has NO case for protocol.ConnectionSecureOK (21). A secure-ok frame would hit the default branch and error the connection as 'unknown connection method ID', confirming the receive path is unimplemented.

```go
switch methodID {
case protocol.ConnectionStartOK: ...
case protocol.ConnectionTuneOK: ...
case protocol.ConnectionOpenOK: ...
case protocol.ConnectionClose: ...
default:
    s.Log.Warn("Unknown connection method ID", ...)
    return fmt.Errorf("unknown connection method ID: %d", methodID)
```

- **`src/amqp-go/auth/mechanism.go` : 10-17** — The Mechanism abstraction is single-round: it takes the full response in one call and returns a terminal *User. There is no method to produce a challenge or carry SASL state across rounds, so even a registered multi-round mechanism could not drive a secure/secure-ok exchange.

```go
type Mechanism interface {
	Name() string
	Authenticate(response []byte, authenticator interfaces.Authenticator) (*interfaces.User, error)
}
```

- **`src/amqp-go/server/connection_handlers.go` : 268-308** — handleConnectionStartOK authenticates in a single step directly from the start-ok Response and immediately finalizes (or rejects) — it never sends a connection.secure challenge nor loops. This is the entirety of the auth flow.

```go
userInterface, err := mechanism.Authenticate(startOK.Response, s.Authenticator)
if err != nil { ... return s.sendConnectionClose(...) }
... conn.User = user ...
```

- **`src/amqp-go/server/server.go` : 244-297** — The handshake loop goes straight from start-ok to sending connection.tune with no conditional secure/secure-ok round-trip. The state machine is hard-coded linear, so no multi-round SASL negotiation is possible regardless of the mechanism selected.

```go
if classID == 10 && methodID == protocol.ConnectionStartOK {
    if err := s.handleConnectionStartOK(conn, frame.Payload[4:]); err != nil { ... return }
}
// Send tune parameters
if err := s.sendConnectionTune(conn); err != nil { ... }
```

- **`src/amqp-go/auth/mechanism.go` : 59-65** — Only PLAIN and ANONYMOUS are registered, and both (see plain.go and anonymous.go) complete in a single Authenticate call. This is why the missing secure/secure-ok path does not affect any currently supported mechanism — hence low severity.

```go
func DefaultRegistry() *Registry {
	registry := NewRegistry()
	registry.Register(&PlainMechanism{})
	registry.Register(&AnonymousMechanism{})
	return registry
}
```


### Fix

Implement the connection.secure / connection.secure-ok pair and extend the SASL layer to support multi-round challenge/response, while keeping the existing single-round PLAIN/ANONYMOUS path fully backward-compatible. Steps: (1) In protocol/methods.go add ConnectionSecureMethod{ Challenge []byte } with Serialize() (encode Challenge as a long string) and ParseConnectionSecure(data) for symmetry, and ConnectionSecureOKMethod{ Response []byte } with Serialize() (encode Response as a long string) and ParseConnectionSecureOK(data) that decodes the long string. (2) Introduce an optional multi-round capability in auth without breaking the existing Mechanism interface: define a new interface ChallengeMechanism interface { Name() string; Start(initialResponse []byte, authenticator) (user *interfaces.User, challenge []byte, done bool, err error); Step(response []byte, authenticator) (user *interfaces.User, challenge []byte, done bool, err error) }. Single-round mechanisms keep implementing the current Mechanism interface unchanged. (3) In server/connection_handlers.go, refactor handleConnectionStartOK so that after looking up the mechanism it type-asserts for ChallengeMechanism; if it is one and returns done=false, the server sends connection.secure with the challenge and returns control so the handshake loop can read the next frame. For the plain Mechanism (single-round) path, behavior is identical to today. (4) In server/server.go processConnectionFrames, replace the single 'read start-ok then send tune' segment with a small loop: send start, read start-ok, then while auth not complete: read next method frame, if it is 10.21 (secure-ok) parse it and feed the response into the mechanism's Step (sending another connection.secure if not done, or completing), otherwise error with 503 COMMAND_INVALID; once auth completes, proceed to send tune. (5) Add a sendConnectionSecure(conn, challenge) helper in connection_handlers.go that serializes ConnectionSecureMethod and writes frame 10.20. (6) Add a case protocol.ConnectionSecureOK in processConnectionMethod that returns a channel-error/connection-error for an out-of-sequence secure-ok (received after handshake) so it is not silently swallowed. Keep the number of rounds bounded (e.g., max 10) to prevent an abusive client from looping. If the intent is only to document current scope rather than add SCRAM/GSSAPI, an acceptable minimal alternative is to at least implement the receive-side handling (item 6) and a clear COMMAND_INVALID rejection, but the full implementation above is what makes the broker spec-complete for multi-round SASL.

**Files to change:**
- `src/amqp-go/protocol/methods.go` — Add ConnectionSecureMethod (Serialize + ParseConnectionSecure) and ConnectionSecureOKMethod (Serialize + ParseConnectionSecureOK) using existing encodeLongString/decodeLongString helpers.
- `src/amqp-go/auth/mechanism.go` — Add optional ChallengeMechanism interface (Start/Step) for multi-round mechanisms; leave the existing Mechanism interface and Registry unchanged so PLAIN/ANONYMOUS keep working.
- `src/amqp-go/server/connection_handlers.go` — Refactor handleConnectionStartOK to detect a ChallengeMechanism and, when done=false, send connection.secure and defer completion; add sendConnectionSecure helper; add a case protocol.ConnectionSecureOK in processConnectionMethod that rejects an out-of-sequence secure-ok with 503 COMMAND_INVALID instead of the generic 'unknown method' error.
- `src/amqp-go/server/server.go` — In processConnectionFrames, replace the fixed 'read start-ok -> send tune' segment with a bounded loop that reads connection.secure-ok frames (10.21) and feeds them into the mechanism's Step until authentication completes, then proceeds to send tune; error on out-of-sequence or malformed frames.

**Code sketch:**

```go
// protocol/methods.go
type ConnectionSecureMethod struct{ Challenge []byte }
func (m *ConnectionSecureMethod) Serialize() ([]byte, error) { return encodeLongString(string(m.Challenge)), nil }

type ConnectionSecureOKMethod struct{ Response []byte }
func ParseConnectionSecureOK(data []byte) (*ConnectionSecureOKMethod, error) {
    s, _, err := decodeLongString(data, 0)
    if err != nil { return nil, err }
    return &ConnectionSecureOKMethod{Response: []byte(s)}, nil
}

// auth/mechanism.go
type ChallengeMechanism interface {
    Name() string
    Start(initial []byte, a interfaces.Authenticator) (u *interfaces.User, challenge []byte, done bool, err error)
    Step(resp []byte, a interfaces.Authenticator) (u *interfaces.User, challenge []byte, done bool, err error)
}

// server/connection_handlers.go
func (s *Server) sendConnectionSecure(conn *protocol.Connection, challenge []byte) error {
    m := &protocol.ConnectionSecureMethod{Challenge: challenge}
    data, err := m.Serialize(); if err != nil { return err }
    return protocol.WriteFrameToConnection(conn, protocol.EncodeMethodFrame(10, protocol.ConnectionSecure, data))
}

case protocol.ConnectionSecureOK: // out-of-band after handshake
    return s.sendConnectionClose(conn, 503, "COMMAND_INVALID", "unexpected connection.secure-ok")
```


**Test (TDD):** TDD, protocol layer first: in protocol/methods_test.go add TestConnectionSecure_RoundTrip that Serializes a ConnectionSecureMethod{Challenge: []byte("chal")} and asserts ParseConnectionSecure round-trips the bytes, plus TestConnectionSecureOK_RoundTrip for the OK direction. Then an integration test in server (e.g. server/auth_multiround_test.go) registering a fake two-step ChallengeMechanism: it drives a real Connection through processConnectionFrames and asserts the server emits a connection.secure (10.20) frame with the expected challenge after start-ok, accepts the client's connection.secure-ok (10.21), and only then sends connection.tune (10.30) — proving the multi-round loop works. Add a negative test asserting that a connection.secure-ok received after the handshake (in processConnectionMethod) is rejected with 503 COMMAND_INVALID rather than falling through the default 'unknown connection method' branch. Also add a regression test that the existing single-round PLAIN path still completes start-ok -> tune with no secure frame emitted.

**Regression risk:** Medium. This touches the connection handshake state machine (server.go processConnectionFrames), the highest-risk area since every connection traverses it. The main risk is breaking the current linear PLAIN/ANONYMOUS flow when introducing the auth loop — mitigated by keeping single-round mechanisms on the unchanged Mechanism interface and gating the new loop strictly behind a ChallengeMechanism type assertion (so the default path is byte-for-byte identical to today). Adding a case for ConnectionSecureOK in processConnectionMethod is low risk (previously it fell to default and errored anyway). Care needed to keep the secure-ok read loop bounded to avoid a client holding a connection open indefinitely.

### Debate & agreed resolution

Investigator verdict: `confirmed` · Skeptic agrees it's real: `True` · Fix verdict: `sound` · Confidence: `high`

**Challenges raised:**
- Challenged real-world impact: the 'erroring the connection on a stray secure-ok' scenario is theoretical — no compliant client sends secure-ok unless the server first sends secure, which this server never does. Resolved: it is a real code path (server.go:568-569 -> processConnectionMethod default branch) but not client-triggerable today, reinforcing low severity.
- Challenged whether the secure-ok handling belongs in processConnectionMethod: the multi-round read loop must live in the handshake segment (server.go:249-264), while processConnectionMethod should only add an out-of-sequence rejection (item 6). The colleague's fix separates these correctly, but the two 10.21 read paths must not conflict.
- Challenged regression coverage: the colleague's regression note covers the PLAIN path but not the auth-disabled / no-authenticator early-return paths (connection_handlers.go:259-265). The type-assertion refactor must preserve those early returns or it regresses the auth-disabled flow. Flagged as a required test/care item.
- Verified wire-format correctness: challenge/response are SASL longstr blobs; the fix's use of encodeLongString/decodeLongString is correct and byte-preserving in Go. No concern remaining.
- Challenged effort-vs-value: 'large' effort for near-zero impact. Resolved by agreeing the full SCRAM/GSSAPI implementation is optional/deferrable and only item 6 (clean 503 rejection) is the minimal worthwhile increment.

**Fix concerns:**
- Full multi-round implementation is 'large' effort with essentially no current user affected (no registered or mainstream client uses multi-round SASL); recommend deferring the full path and shipping only the item-6 hardening unless SCRAM/GSSAPI is an actual roadmap requirement.
- The refactor of handleConnectionStartOK must preserve the auth-disabled and no-authenticator early returns (connection_handlers.go:259-265, 303-308); the colleague's regression plan does not explicitly test these paths.
- The multi-round read loop must be added to the handshake segment in server.go (lines 249-264), kept distinct from the out-of-sequence secure-ok case in processConnectionMethod, to avoid two conflicting readers of 10.21 frames.
- Must handle a ChallengeMechanism that completes in Start (done=true, client sent full response up front) — the sketch's done bool covers this, but it needs an explicit test.
- Bound the number of secure/secure-ok rounds (colleague suggests max ~10) so an abusive client cannot hold a half-open handshake indefinitely — correctly noted by the colleague and should be enforced.

**Agreed resolution (authoritative):**

Confirmed as a real but low-severity spec-completeness/extensibility gap, not a defect in any currently supported flow. Verified directly in source: protocol/methods.go:12-13 declares only the ConnectionSecure=20 / ConnectionSecureOK=21 constants (repo-wide grep returns nothing else — no *Method structs, Serialize, or Parse); auth/mechanism.go:11-17 exposes a single-round Mechanism.Authenticate(response, authenticator) (*User, error) with no challenge/state carry; server/connection_handlers.go:280 finalizes auth in one Authenticate call; server/server.go:249-264 hard-codes start-ok -> sendConnectionTune with no secure loop; only PLAIN and ANONYMOUS are registered (mechanism.go:59-65), both single-round (verified plain.go, anonymous.go). server.go:568-569 routes class-10 method frames to processConnectionMethod (connection_handlers.go:314-338), whose default branch returns 'unknown connection method ID' — so a stray post-handshake secure-ok would error the connection, though no compliant client can trigger this since the server never sends a secure challenge. Severity low is correct (arguably a documented won't-fix limitation): every mainstream AMQP 0.9.1 client uses single-round PLAIN, so nothing is broken today. The proposed fix is architecturally sound: a separate optional ChallengeMechanism interface plus a type-assertion gate keeps the single-round path byte-for-byte identical, and all referenced helpers (encodeLongString/decodeLongString/EncodeMethodFrame/WriteFrameToConnection) exist. Agreed the full implementation (large effort) is optional/deferrable given near-zero real-world impact; the minimal acceptable increment is item 6 — adding a ConnectionSecureOK case in processConnectionMethod that returns a clean 503 COMMAND_INVALID for an out-of-sequence secure-ok instead of the generic 'unknown method' error. If the full multi-round path is built, the secure-ok read loop must live in the handshake segment (server.go:249-264), not be driven through processConnectionMethod, and the refactor must preserve the auth-disabled / no-authenticator early returns (connection_handlers.go:259-265) to avoid regressing those paths.

---

## [I29] LifecycleManager.Start spawns a second system-metrics collector via Server.Start(), causing double collection and a leaked collector goroutine after lifecycle shutdown

**Unranked by council · P3** · Severity: ⚪ Low · Category: observability · Effort: small

> ⚠️ **Confirmed real by the debate but omitted from the council's explicit ranking** — placed here by agreed severity.

### Impact
When a server is driven through its LifecycleManager (the intended managed path — every builder-constructed server has one, builder.go:270), TWO independent system-metrics collectors run concurrently. Both call collectSystemMetrics() every 60s. Consequences: (1) GC-pause distortion — RecordGCPause() feeds a prometheus.Summary via .Observe() (metrics.go:83,616), so the same GC pause value is observed twice every cycle, doubling the summary's _count/_sum and skewing its quantiles. (2) Doubled filesystem work every 60s: two filepath.Walk() over the whole data dir plus two os.ReadDir() sweeps of the WAL and per-queue segment dirs (system_metrics.go:107,116,142,177,192). (3) Goroutine/resource leak on shutdown: LifecycleManager.Stop/forceShutdown never call Server.Stop(), so the collector spawned inside Server.Start() (whose cancel is stored only in s.metricsCancel) is never cancelled and keeps running its 60s ticker after the lifecycle-managed server has "stopped". It is also not tracked by lm.wg, so wg.Wait() does not block on it. Gauge metrics (.Set) are idempotent so absolute values are not corrupted, which keeps overall severity low; the summary distortion + leaked goroutine are the real defects.

### Root cause
Server.Start() unconditionally starts its own system-metrics collector (server.go:82-90). LifecycleManager.Start() also starts one (lifecycle.go:150-154) and then calls lm.server.Start() (lifecycle.go:161), so both fire. The two code paths were written independently and neither is aware the other launches the collector. Compounding it, the lifecycle shutdown path (Stop, forceShutdown) tears down the listener and connections directly instead of calling Server.Stop(), so s.metricsCancel — the only handle to the Server.Start()-spawned collector — is never invoked, leaking that goroutine. The candidate's phrasing "only the server.Start one is cancellable" is inverted: in the lifecycle path the lifecycle collector (lm.ctx) IS cancelled by lm.cancel(), while the Server.Start() collector is the one that is NOT cancelled.

### Proof

- **`src/amqp-go/server/lifecycle.go` : 149-171** — LifecycleManager.Start spawns collector #1 with lm.ctx, then in the next goroutine calls lm.server.Start(), which spawns collector #2. Only these two goroutines are tracked by lm.wg; the collector launched *inside* Server.Start() is not.
- **`src/amqp-go/server/server.go` : 82-90** — Server.Start() unconditionally launches its own startSystemMetricsCollection goroutine using a context derived from context.Background(), with the cancel func stored in s.metricsCancel. This is collector #2 when reached via LifecycleManager.Start.
- **`src/amqp-go/server/server.go` : 611-627** — Server.Stop() is the only code that calls s.metricsCancel. LifecycleManager.Stop/forceShutdown never call Server.Stop(), so collector #2's context is never cancelled through the lifecycle path — the goroutine leaks past shutdown.
- **`src/amqp-go/server/lifecycle.go` : 183-235** — LifecycleManager.Stop cancels lm.ctx (stops collector #1) and closes listener/connections directly, but never invokes Server.Stop(); wg.Wait() only waits on the two tracked goroutines, not collector #2.
- **`src/amqp-go/server/system_metrics.go` : 13-31** — Each collector runs a 60s ticker calling collectSystemMetrics(); two collectors means every metric method is invoked twice per interval, including the filesystem-walking disk/WAL/segment scans.
- **`src/amqp-go/metrics/metrics.go` : 83,614-617** — MemoryGCPause is a prometheus.Summary and RecordGCPause calls .Observe(); duplicate observation of the same pause value each cycle inflates the summary's count/sum and distorts quantiles (unlike the idempotent .Set() gauges).
- **`src/amqp-go/server/builder.go` : 269-270** — Every server built via the builder gets server.Lifecycle = NewLifecycleManager(...), so the double-collection path is live for any operator using the lifecycle-managed start.

### Fix

Make Server.Start() the single owner of the system-metrics collector, and have LifecycleManager stop delegating to Server for teardown so the collector is properly cancelled. Concretely: (1) Remove the metrics-collector spawn from LifecycleManager.Start (lifecycle.go:149-154) entirely, since Server.Start() already starts it. This eliminates the duplicate collector. (2) Fix the shutdown leak: in LifecycleManager.Stop and LifecycleManager.forceShutdown, call lm.server.Stop() (which cancels s.metricsCancel and closes the listener) instead of, or in addition to, the current direct listener.Close()/Shutdown-flag manipulation. Prefer routing all teardown through Server.Stop() to keep a single shutdown path; if forceShutdown still needs to force-close individual connections, call lm.server.Stop() first (to cancel metrics + close listener) then force-close connections. (3) Optional hardening: guard Server.Start()'s collector launch with sync.Once or a bool so repeated Start calls cannot double-spawn. With the collector removed from LifecycleManager.Start, the lm.wg no longer needs the metrics Add/Done pair there. Verify no other caller relied on LifecycleManager owning the metrics context (grep shows lm.ctx is only used for hooks and the server-start goroutine).

**Files to change:**
- `src/amqp-go/server/lifecycle.go` — Delete the block at lines 149-154 that does lm.wg.Add(1) + go lm.server.startSystemMetricsCollection(lm.ctx). In Stop() (around 198-201) and forceShutdown() (around 257-263), replace/augment the direct listener close with a call to lm.server.Stop() so s.metricsCancel is invoked and the Server.Start()-owned collector is cancelled.
- `src/amqp-go/server/server.go` — (Optional hardening) Wrap the collector launch in Start() (lines 83-90) in a sync.Once or a started flag so it is spawned exactly once even if Start is called again. No functional change strictly required once the lifecycle duplicate is removed, but ensures idempotency.

**Code sketch:**

```go
// lifecycle.go Start(): remove this block entirely
// lm.wg.Add(1)
// go func() { defer lm.wg.Done(); lm.server.startSystemMetricsCollection(lm.ctx) }()

// lifecycle.go Stop(): route teardown through Server.Stop so metricsCancel fires
if lm.cancel != nil { lm.cancel() }
_ = lm.server.Stop() // cancels s.metricsCancel + closes listener

// lifecycle.go forceShutdown(): call Server.Stop() first, then force-close conns
_ = lm.server.Stop()
lm.server.Mutex.Lock()
lm.server.Shutdown = true
for _, conn := range lm.server.Connections { if conn.Conn != nil { conn.Conn.Close() } }
lm.server.Mutex.Unlock()
```


**Test (TDD):** Add a TDD test in server (e.g. lifecycle_metrics_test.go) using a counting fake MetricsCollector that increments an atomic each time UpdateServerUptime (or a dedicated collectSystemMetrics-only hook) is called. (1) Build a server with a short-circuited collector interval or invoke the collector directly: assert that after LifecycleManager.Start(ctx) exactly ONE collector goroutine exists — e.g. compare runtime.NumGoroutine() deltas, or instrument startSystemMetricsCollection to increment a counter on entry and assert it equals 1 after Start. (2) Leak test: capture the collector's goroutine (via the entry counter and a matching exit counter), call LifecycleManager.Stop(ctx), then assert the exit counter equals the entry counter (collector actually returned) — this currently FAILS because the Server.Start collector is never cancelled by the lifecycle Stop path. After the fix (removing the duplicate + calling Server.Stop in teardown), both assertions pass: entry==1 and entry==exit after Stop.

**Regression risk:** Low-to-moderate. Removing the lifecycle-spawned collector is safe because Server.Start() already provides one. The riskier part is routing LifecycleManager teardown through Server.Stop(): Server.Stop() closes the listener and sets Shutdown, which forceShutdown/Stop already do — need to ensure no double-close of the listener panics (net.Listener.Close is safe to call once; guard against calling it twice, e.g. only call listener.Close in one place). Also confirm the existing lifecycle graceful-shutdown timing (wg.Wait vs shutdownCtx) still behaves, since the metrics goroutine is no longer in lm.wg at all. Run the existing server/lifecycle test suites (go test ./server/...) to catch shutdown-timeout regressions.

### Debate & agreed resolution

Investigator verdict: `confirmed-severity-adjusted` · Skeptic agrees it's real: `True` · Fix verdict: `needs-revision` · Confidence: `high`

**Challenges raised:**
- Challenged the impact framing 'the intended managed path — every builder-constructed server has one (builder.go:270)... the double-collection path is live for any operator.' VERIFIED FALSE as stated: I grepped the entire module and LifecycleManager.Start()/.Stop() and the server.Lifecycle field are NEVER invoked anywhere outside their definition. Production main.go (cmd/amqp-server/main.go:220) calls amqpServer.Start() directly and shuts down via server.Stop() (line 244) — it bypasses the LifecycleManager entirely. No test drives the server through the lifecycle either (the only *_test.go matching 'lifecycle' is storage/log_compaction_integration_test.go, a name coincidence for 'FullLifecycle'). So in the shipped binary and all tests today, exactly ONE collector runs and metricsCancel IS cancelled on shutdown. The double-collection and goroutine leak are latent, not live. Colleague conceded the framing overstated live impact; we agree severity stays low but is a latent/dead-path defect with currently-zero runtime impact.
- Confirmed the two mechanical defects independently: (a) lifecycle.go:150-154 spawns collector #1 then lifecycle.go:161 calls server.Start() which spawns collector #2 at server.go:82-90; (b) RecordGCPause is a prometheus.Summary.Observe (metrics.go:83,615-617), non-idempotent, so a double-observe would distort count/sum/quantiles while .Set gauges stay correct; (c) Server.Stop() (server.go:611-627) is the sole caller of s.metricsCancel and lifecycle Stop/forceShutdown (lifecycle.go:183-292) never call it, so the Server.Start()-spawned collector (ctx from context.Background(), server.go:84) would leak IF the lifecycle path were ever used. All three hold. Colleague's correction of the original candidate's inverted 'which collector is cancellable' claim is also correct.
- Challenged the fix's forceShutdown handling: forceShutdown already closes lm.server.Listener (lifecycle.go:259-263) and Server.Stop() also closes the listener (server.go:623-624). The codeSketch adds a Server.Stop() call but does not remove the existing listener.Close() in forceShutdown, causing a double Close(). net.Listener.Close on second call returns an error (no panic) but forceShutdown routes that error into setError -> flips state to StateError spuriously. Fix must consolidate to a single close site and swallow/ignore the already-closed error. Colleague acknowledged this was flagged in regressionRisk but not carried through in the codeSketch — fix needs revision to actually remove the duplicate close.

**Fix concerns:**
- forceShutdown double-close: Server.Stop() (server.go:623-624) and forceShutdown (lifecycle.go:259-263) both close lm.server.Listener. The codeSketch adds Server.Stop() without deleting the pre-existing listener.Close(), so the second Close returns an error that forceShutdown feeds to setError, spuriously transitioning to StateError. Must pick ONE close site and ignore the ErrClosed on the redundant path.
- The sync.Once/started-flag hardening on Server.Start()'s collector is marked 'optional' but should be treated as recommended: the double-spawn is exactly the failure mode, and guarding Start() defends against any future caller (including the lifecycle path) re-triggering it regardless of the lifecycle-side deletion.
- The proposed leak test must actually call lm.Start(ctx)/lm.Stop(ctx) — since nothing else in the repo does, the test is the only thing exercising this path; it should assert both entry==1 collector after Start and entry==exit after Stop, and be written knowing it validates an otherwise-dead API.
- Neither engineer's write-up flags that the simplest alternative fix is to delete or wire up the LifecycleManager. Since it is entirely unused, an equally valid resolution is to route main.go through the LifecycleManager (making it live and then the fix matters) OR remove the dead manager. The chosen fix (patch the manager in place) is fine but should note the manager is currently dead code.

**Agreed resolution (authoritative):**

Real but LATENT/dead-path bug in LifecycleManager. Mechanically confirmed: LifecycleManager.Start double-spawns the system-metrics collector (lifecycle.go:150-154 + server.go:82-90 via the server.Start call at lifecycle.go:161), and lifecycle Stop/forceShutdown never call Server.Stop() so the Server.Start-owned collector (the only handle being s.metricsCancel, server.go:616) would leak. RecordGCPause is a non-idempotent prometheus.Summary.Observe (metrics.go:615-617), so double-collection would distort that summary while .Set gauges stay correct. CRITICAL CORRECTION to the validation's impact: the whole path is currently unreachable — grep of the entire module shows LifecycleManager.Start/.Stop and the server.Lifecycle field are never invoked outside their definition; production main.go uses server.Start()/server.Stop() directly (cmd/amqp-server/main.go:220,244) and no test drives the lifecycle. So today exactly one collector runs and it is cancelled correctly; impact is zero until someone wires the server through the (builder-provided but unused) LifecycleManager. Agreed severity: low, explicitly qualified as latent (bug in an unused public API), which is a downgrade from the validation's implied 'live for any operator' framing. Fix direction (remove the duplicate spawn from LifecycleManager.Start; route lifecycle teardown through Server.Stop) is sound but NEEDS REVISION: the codeSketch's forceShutdown change double-closes the listener (Server.Stop closes it and forceShutdown still closes it, the second Close error flips state to StateError via setError) and must be consolidated to a single close site with ErrClosed ignored; the sync.Once idempotency guard on Server.Start's collector should be treated as recommended not optional; and the test must actually invoke lm.Start/lm.Stop since nothing else does. Should also note the alternative of either wiring up or deleting the currently-dead LifecycleManager.

---

