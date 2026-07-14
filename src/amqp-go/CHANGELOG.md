## [2026-07-13 14:30]
- Fixed transient-vs-tx visibility race in PublishMessageTx: tag now reserved
  on the frontier at stage time via FrontierReserve (same pattern as
  PublishMessage), and the deferred visibility closure calls
  FrontierComplete(tag, true) on commit or FrontierComplete(tag, false) on
  abort. Previously the tag was minted lock-free and made visible via
  FrontierPublishTransient/Publish, allowing a concurrent PublishMessage to
  advance head past the tx tag before commit, causing a permanent
  WaitingCount leak (stranded tag).
- Changed deferred closure contract from []func() to []func(bool): the
  transaction manager calls apply(true) on commit and apply(false) on abort.
  On abort, FrontierComplete(tag, false) releases the frontier slot so the
  tag is gap-skipped, never stranded.
- Added mid-fanout error cleanup: if PublishMessageTx fails after reserving
  frontier slots, all reserved slots are released before returning the error.
- Files affected:
  - broker/storage_broker.go (PublishMessageTx: FrontierReserve + FrontierComplete + error cleanup)
  - transaction/broker_executor.go (interface: []func() -> []func(bool))
  - transaction/manager.go (executeAtomically: apply(true)/apply(false), executeStaged, txStagingExecutor interface)
  - server/broker_adapters.go (PublishMessageTx return type)
  - broker/max_length_test.go, broker/queue_ttl_recovery_test.go, broker/queue_reaper_test.go (f() -> f(true))
  - broker/tx_frontier_race_test.go (new: TestFrontierFlip_TxSibling_NoStrand, TestFrontierFlip_TxAbort_ReleasesFrontierSlot)
