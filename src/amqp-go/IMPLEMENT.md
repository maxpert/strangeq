# IMPLEMENT — tx-transient-race fix

## Discussion

The "always-reserve-at-mint" fix (dd01730) closed the transient-vs-transient
visibility race in `PublishMessage` and `dead_letter.go`'s `republishToTargets`
by routing every publish through `FrontierReserve` + `FrontierComplete`.
`PublishMessageTx` was NOT fixed: it minted its delivery tag lock-free
(`b.globalDeliveryTag.Add(1)`, not through `FrontierReserve`) and made it
visible post-commit via the old 3-way check (`FrontierPublishTransient` if
frontier-active, else `Publish`).

The race: Tx A mints tag L (lock-free), stages to txnStore. Concurrent
`PublishMessage` B does `FrontierReserve(H > L)`, stores, `FrontierComplete(H,
true)` — head advances past L. Consumer claims L, `GetMessage` misses (Tx A
hasn't committed) — gap-skip. Tx A commits, deferred closure runs
`FrontierPublishTransient(L)` — `waiting.Add(1)`, but head/tail already past L
— L stranded, WaitingCount leaks +1.

## What was implemented

1. **FrontierReserve at stage time**: `PublishMessageTx` now mints the tag
   inside `FrontierReserve`'s `assign` callback (same as `PublishMessage`),
   registering it as pending on the per-queue frontier. A concurrent publish's
   `FrontierComplete` can never advance head past this still-pending tag.

2. **FrontierComplete in deferred closure**: The deferred visibility closure
   now calls `qs.FrontierComplete(id, commit)` — `true` on commit (message is
   durable/stored, head advances, waiting incremented), `false` on abort
   (frontier slot released, head advances past the tag, never counted ready,
   consumer gap-skips it).

3. **Interface change []func() -> []func(bool)**: The deferred closure
   contract changed from `func()` to `func(commit bool)`. The transaction
   manager's `executeAtomically` calls `apply(true)` on commit and
   `apply(false)` on abort. This is required because the frontier slot must be
   released on abort — previously the closures were simply not run on error,
   which would strand the reserved slot forever.

4. **Mid-fanout error cleanup**: If `PublishMessageTx` fails after reserving
   frontier slots (WaitForCapacity fails, or StoreMessage fails on a later
   queue in a fanout), all already-reserved slots are released via
   `FrontierComplete(tag, false)` before returning the error.

## Files changed

- `broker/storage_broker.go` — `PublishMessageTx`: FrontierReserve at stage
  time, FrontierComplete in deferred closure, reserved-slot tracking + cleanup
  on error, updated doc comment.
- `transaction/broker_executor.go` — `txPublishBroker` interface and
  `ExecutePublishStaged` return type: `[]func()` -> `[]func(bool)`.
- `transaction/manager.go` — `txStagingExecutor` interface,
  `executeAtomically` (apply(true)/apply(false) on commit/abort),
  `executeStaged` signature, updated doc comment.
- `server/broker_adapters.go` — `StorageBrokerAdapter.PublishMessageTx`
  return type.
- `broker/max_length_test.go` — `publishTxToLiveStore` return type,
  `d()` -> `d(true)`.
- `broker/queue_ttl_recovery_test.go` — `fn()` -> `fn(true)`.
- `broker/queue_reaper_test.go` — `fn()` -> `fn(true)`.
- `broker/tx_frontier_race_test.go` — new test file:
  `TestFrontierFlip_TxSibling_NoStrand` (the race test, failing before fix),
  `TestFrontierFlip_TxAbort_ReleasesFrontierSlot` (abort path guard).

## Current status

Complete. All tests pass:
- `go test ./broker/ -race -count=3 -timeout 300s` (159s)
- `go test ./... -race -timeout 300s` (all packages pass)
- `gofmt -l .` clean
- `go vet ./...` clean

## Notes

- `FrontierPublishTransient` (queue_dispatch.go) is now dead code in
  production — the tx path was its only production caller. It remains
  exercised by `TestFrontier_TransientBehindPendingDurable`
  (publish_async_test.go) which tests the frontier's FIFO behavior directly.
  Whether to remove it is a separate cleanup decision.
