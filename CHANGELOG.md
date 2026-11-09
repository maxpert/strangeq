# Changelog

## [Unreleased]

### Changed
- Metrics-tab correctness, from a post-release review of the metric surface (see
  [docs/METRICS.md](docs/METRICS.md)):
  - Five families the broker actually emits no longer fall into the `Other`
    catch-all (which is now reserved for genuinely unknown families):
    `amqp_consumer_register_errors_total`, `amqp_deliveries_dropped_total`,
    `amqp_deliveries_requeued_total`, `amqp_delivery_send_errors_total`,
    `amqp_publish_refused_total`. A test now fails if any emitted family lands in
    `Other`.
  - The `Unroutable` tile is `warn`, not `bad`: a mandatory publish with no
    matching binding is a normal caller outcome.
  - The `WAL full flushes` tile is `bad` only in `barrier` durability mode, and
    only for traffic-path flush sites. Under `storage.durability: flush` a full
    flush per confirm is the configured behaviour, so it is informational there
    (the durability mode is now passed to the UI; an absent or unrecognised mode
    falls back to the strict `barrier` reading).
- The tile outcome rule has four documented cases: the family is absent ⇒ `—`
  with `not emitted by this broker`; the family is present ⇒ the number; the
  family is present and a **filtered sum** (`sum(<family>{<label>…})`) matches no
  series ⇒ `0`; and the family is present but any other selector form — a bare
  name with a matcher, or `max(…)` with a matcher — matches no series ⇒ `—`
  (no aggregate value is defined there, so that title applies to it too). A
  malformed `=~`/`!~` pattern is never read as `0` — it degrades the tile to `—`
  and names the offending pattern; a *valid* pattern that drops the whole
  excluded domain is rejected by the static gate, not at runtime.

### Fixed
- `config.sample.yaml` and `interfaces/config.go` claimed all three
  `storage.durability` values use fdatasync on Linux, "so any value is power-loss
  safe". Only `barrier` and `flush` do; `none` issues no group-commit barrier and
  is not power-loss safe.
- The headless-browser test harness used a fixed 30 s per-tab deadline that flaked
  under host CPU contention, and applied it only *after* tab creation. The bound
  is now 90 s clamped to the go-test budget, covers tab setup as well, and names
  the test, the stage and the bound on timeout.

## [0.14.0] - 2026-11-08

### Added
- Management **control plane**: an optional RabbitMQ-4.4-management-subset REST API and an
  embedded single-binary web UI (assets `go:embed`-ed; no separate folder, build step, or
  CDN). Off by default and loopback-only by default; enabled with `--management` /
  `management.enabled`. It has a two-gate auth model (a capability gate plus a per-resource
  vhost gate, deny-by-default, tags→capabilities), server-side sessions with CSRF
  protection, loopback/host ACLs, fixed-window rate limits, and a startup refusal matrix
  that refuses unsafe `management.authentication` combinations. See
  [docs/MANAGEMENT.md](docs/MANAGEMENT.md).
- Management UI **Metrics** tab (`/metrics`): the SPA renders the broker's
  Prometheus matrix — a 45-tile vital-stats panel, a grouped and searchable
  family list, and a raw-text view — from a new same-origin, auth-gated
  `GET /api/metrics` on the management listener. It re-serves the same process
  registry the telemetry `/metrics` endpoint exposes, so the two never drift.
  No new config key; when telemetry is disabled the route answers `501` and the
  screen shows a truthful notice. The telemetry listener, CORS and the CSP
  (`connect-src 'self'`) are unchanged.
- Telemetry **broker outage families**: recovery faults/discards/duration/messages
  recovered, WAL and segment durability failures, publish refusal, consumer-registration
  errors, depth-gate/reader-pause back-pressure, deliveries requeued/dropped,
  connections-closed-by-cause, shutdown result/duration, resource-alarm trips, and
  recovered panics — every label domain a closed enum. The full `amqp_*` set is pinned by
  a golden inventory test and documented in [docs/METRICS.md](docs/METRICS.md).
- `--management`, `--management-address`, and `--management-port` flags.

### Changed
- `amqp_wal_settle_drained_total` is renamed `amqp_wal_settle_drained`: it is a
  gauge, not a counter, so the `_total` suffix was wrong. Update any dashboard or
  alert referring to the old name.

### Fixed
- `amqp_message_delivery_duration_seconds` was never observed — its only producer
  sat on a code path no delivery took. Both batched delivery writers now time the
  batch write.
- `amqp_channels_total` leaked on connection teardown while channels were still
  open; the teardown path now records each channel close.
- A batched `basic.ack`/`basic.nack` with `multiple=true` counted a single settled
  delivery instead of every delivery it settled.
- `amqp_consumers_total` stayed stale when a channel was closed without the
  connection being closed.
- Deleting a queue did not reclaim its segment or hot-cache per-queue series, so
  the metric surface grew without bound.
- Exclusive-queue teardown did not increment `amqp_queues_deleted_total`.
- Messages published through the management API were not counted in
  `amqp_messages_published_total`.
- The bundled Grafana dashboard queried `amqp_messages_dropped_total`, a family the
  broker never writes; the panel now uses `amqp_deliveries_dropped_total` and
  `amqp_publish_refused_total`. New repo gates parse every dashboard expression
  against the metric inventory, and every VITALS tile selector too.
- The `Management 5xx` tile selected `status_class="5xx"`, a label value that can
  never occur (the class is the bare digit), so the tile was permanently `—`.
- The AMQP handshake wrote `conn.Username`/`conn.Vhost` after the connection was published
  into the registry and outside `Server.Mutex`, racing the management read path (which
  reads them under `Mutex.RLock`). The handshake assignments now hold `Server.Mutex`, and
  the management connection-close path reads the username inside its `RLock` window.
- A binding was counted as removed even when `DeleteBinding`/`DeleteExchangeBinding`
  returned an error, so `object_totals.bindings` could fall below `len(ListBindings())`.
  Only a successful delete now contributes to the binding delta.
- The management `POST /api/exchanges/{vhost}/{name}/publish` endpoint now applies the full
  `basic.properties` set, so `properties.delivery_mode: 2` produces a persistent message that
  survives a restart (previously transient even on a durable queue); the raw properties map is
  no longer copied into message headers.
- Management requests refused before routing (the in-flight shed-load 503) are now counted in
  `amqp_management_requests_total` under `status_class="5"` with the bounded `pre_route`
  endpoint, instead of being invisible to the counter.
- A delivery drained into the fan-in extras slice whose consumer had been removed is now
  counted as `amqp_deliveries_dropped_total{reason="consumer_removed"}` rather than dropped
  silently.

## [0.12.0] - 2026-11-07

### Changed
- Build and release toolchain is Go 1.27.3: `go.mod` (both modules), all four
  CI `setup-go` steps, the Dockerfile and the docs moved together.
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
- CI is green again: four `basic.get` tests asserted cross-connection visibility
  of a fire-and-forget publish, which AMQP does not guarantee, and a gated WAL
  fsync parked the commit loop while broker teardown joined it, turning a 30 s
  assertion failure into a 10-minute package timeout. The test step now carries
  an explicit 25-minute timeout and `setup-go` is given the module path so its
  Go cache key resolves.
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
- Three-tier storage architecture (in-memory hot cache, WAL, segments)
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
