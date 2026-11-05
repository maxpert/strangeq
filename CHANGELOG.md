# Changelog

## [Unreleased]

### Changed
- The default listener now binds `127.0.0.1:5672` instead of all interfaces. Use
  `network.address` (or `AMQP_NETWORK_ADDRESS`) to expose the broker, and read
  the startup security warnings before doing so.
- `AMQP_*` environment variables always apply, with or without `--config`;
  precedence is defaults → file → environment → explicitly set flags.
- Config files are layered onto the defaults, so a minimal file (for example
  only `storage.path`) no longer fails validation. Snake_case keys are accepted.
- `network.port` is honoured: it rewrites the port in `network.address` and a
  conflicting pair is rejected with an explicit error.
- The `authmechanisms` list is authoritative. A list without `PLAIN` no longer
  silently offers PLAIN; unknown names are reported at startup.
- Telemetry defaults to loopback (`127.0.0.1`) and pprof is opt-in.
- The shipped sample config's `engine.walcleanupcheckintervalms` is 1000 ms,
  matching the code default. The reclaimed files' inventory is a memory bound,
  so a slow reclaim retains more resident memory.

### Added
- Connection access control: `allowedhosts`/`blockedhosts` (IP, CIDR, or
  `localhost`) and `allowedusers`/`blockedusers`, plus per-peer failed-auth
  limiting (`authfailurelimit`, `authfailurewindowms`).
- `--telemetry-address`, `--enable-pprof`, and `--healthcheck` flags.
- `/health` is now a readiness probe (503 until recovery and listener bind,
  503 during shutdown) and reports resource alarms without leaving rotation.
- Version, commit, and build date are stamped through every build path and
  shown by `--version`.
- Prometheus metrics for auth failures, resource alarms, build info, and
  queue ready depth.
- systemd unit, environment example, shipped config, smoke script, Dockerfile,
  and `.dockerignore`.

### Fixed
- `--telemetry-port` and `--tls-*` flags no longer override config with their
  defaults when the operator did not pass them.
- Unknown usernames no longer return measurably faster than wrong passwords.
- Crash/stress scripts use real flags, free ports, and readiness checks, and
  only stop the broker they started.
- `--version` reports the stamped version instead of a hard-coded constant.
- WAL fsync on macOS issues `F_FULLFSYNC` through a preemptible path, so the
  committer's OS thread no longer pins a Go P for the whole drive-cache flush.
- With `storage.fsync: false`, the WAL directory fsync is skipped: a
  no-fsync run no longer pays a directory barrier whose contents it never
  promised to make durable (transactions still fsync unconditionally).
- WAL bookkeeping is bounded by the in-flight (not-yet-acknowledged) set, and
  the WAL index shard maps shrink once the unacked set drains, cutting peak
  RSS on the 1 KiB workloads.

## [0.1.0] - TBD

### Added
- Full AMQP 0.9.1 protocol implementation
- Exchange types: direct, fanout, topic, headers
- Exchange-to-exchange bindings with cycle detection
- Three-tier storage architecture (ring buffer, WAL, segments)
- Persistent and non-persistent messages with crash recovery
- TLS/SSL encryption with mutual TLS support
- SASL authentication (PLAIN, ANONYMOUS)
- Per-vhost authorization (RabbitMQ-style configure/write/read permissions)
- Publisher confirms (`confirm.select`)
- Transactions (`tx.select` / `tx.commit` / `tx.rollback`)
- `basic.get`, `basic.return`, `basic.recover`
- Message and queue TTL (`x-message-ttl`, per-message `expiration`, `x-expires`)
- Dead-letter exchanges with full `x-death` / `x-first-death-*` headers
- Queue length limits (`x-max-length`, `x-max-length-bytes`, `x-overflow`)
- Resource alarms (memory and disk watermarks, `connection.blocked`/`unblocked`)
- Prometheus metrics and pprof profiling
- Lock-free per-queue ring buffer with per-slot CAS
- Batch TCP writes and ACK offloading
- Vectored delivery for large messages (writev)

### Performance
- See the [Performance section](README.md#performance) for the current measured
  matrix against RabbitMQ 4.4 Classic, including both durability modes, the
  validity gates, and the cross-session caveat.
- Headline (shipped `fsync:on` mode): wins on the 1 KiB workloads and on peak
  RSS; the 64 KiB `fsync:on` rows remain losses against RabbitMQ's
  best-measured configuration (F3 0.81x, F4 0.87x).

[Unreleased]: https://github.com/maxpert/strangeq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/maxpert/strangeq/releases/tag/v0.1.0
