# Telemetry and Metrics

Enable the telemetry listener to scrape Prometheus metrics and check broker readiness.
It is separate from the [management HTTP API and UI](MANAGEMENT.md) and works in
standalone and cluster mode. Management can re-serve the same process registry when
enabled on a standalone broker.

## Serving telemetry

The telemetry listener is **off by default** and binds **loopback only** by default.

```bash
amqp-server --enable-telemetry \
  --telemetry-address 127.0.0.1 \
  --telemetry-port 9419
```

| Flag | Config key | Default | Description |
|------|-----------|---------|-------------|
| `--enable-telemetry` | `telemetry.enabled` | `false` | Enable the telemetry listener |
| `--telemetry-address` | `telemetry.address` | `127.0.0.1` | Bind address |
| `--telemetry-port` | `telemetry.port` | `9419` | Bind port |
| `--enable-pprof` | `telemetry.pprofenabled` | `false` | Register `/debug/pprof/*` |

Environment equivalents are `AMQP_TELEMETRY_ENABLED`, `AMQP_TELEMETRY_ADDRESS`,
`AMQP_TELEMETRY_PORT`, and `AMQP_TELEMETRY_PPROFENABLED`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus text exposition (`text/plain; version=0.0.4`), unauthenticated. |
| `/health` | GET | Readiness probe. `200 OK` when ready; `503 NOT READY: <reason>` until recovery completes and the AMQP listener is bound, and during shutdown. During a resource alarm it still returns `200` with `OK (resource alarm active: memory,disk)`. |
| `/debug/pprof/*` | GET | Go profiling, only when `--enable-pprof`. Heap and goroutine dumps are unauthenticated — keep the listener on loopback or behind a firewall. |

Check the listener after startup.

```sh
curl --fail http://127.0.0.1:9419/health
curl --fail http://127.0.0.1:9419/metrics
```

In cluster mode, readiness includes enrollment, not proof that every shard has quorum.
Verify a queue's write availability with an AMQP publish and publisher confirm. See
[cluster readiness](CLUSTERING.md#readiness-and-quorum).

The queue poller refreshes per-queue gauges every 15 seconds. Once the snapshot exceeds
2,000 queues it stops updating per-queue gauges and updates only the aggregate
`amqp_queues_total`. Previously emitted per-queue values may be stale.

### Management `GET /api/metrics` and the Metrics tab

On the management listener, `GET /api/metrics` returns the same scrape wrapped as JSON
(`{"text": "..."}`), so the same-origin SPA can render it under the `connect-src 'self'`
CSP. The route requires authentication and can return these statuses.

- `200` with the exposition.
- `501` when telemetry is disabled (no process registry).
- `503` on a scrape failure (with `Retry-After`).

The [Metrics tab](MANAGEMENT.md#metrics-tab) provides vital-stat tiles, a searchable
family browser, and raw exposition. Missing values display an em dash. An emitted zero
displays `0`. Management is unavailable in cluster mode. Scrape the telemetry listener
directly there.

## Metric reference

Every emitted family uses the `amqp_` namespace. Types are Prometheus `counter`,
`gauge`, `histogram`, or `summary`. `gauge (func)` marks a `GaugeFunc` read at scrape
time. Label domains are closed enums unless noted. Per-queue families explicitly list
`queue` or `vhost` labels. Fault families use bounded classifications.

Registration does not imply that every storage backend updates a family. The WAL and
segment families below describe the standalone storage engine. They do not establish the
health or durable commit state of a cluster's shared Raft journal. Use broker logs and
queue-level AMQP checks alongside telemetry.

### Startup, build, and process

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_build_info` | gauge | `version`, `commit`, `go_version` | Build identity; the value is always `1`. |
| `amqp_server_uptime_seconds` | gauge | — | Server uptime in seconds. |
| `amqp_memory_used_bytes` | gauge | — | Total memory used by the Go runtime. |
| `amqp_memory_heap_bytes` | gauge | — | Heap memory in use. |
| `amqp_memory_gc_pause_seconds` | summary | — | GC pause duration (p50/p90/p99). |
| `amqp_goroutines_total` | gauge | — | Number of active goroutines. |
| `amqp_file_descriptors_open` | gauge | — | Open process file descriptors from `/proc/self/fd` on Linux. Always `0` on macOS and Windows. |
| `amqp_disk_free_bytes` | gauge | — | Free disk space in the data directory. |
| `amqp_disk_usage_bytes` | gauge | — | Total disk used by server data. |
| `amqp_unsafe_recovery_discarded_artifacts` | gauge | — | Artifacts discarded at boot because `--unsafe-recovery` was set; `0` on a clean boot. **Non-zero means the broker is serving from data it could not fully recover.** |

### Connections

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_connections_total` | gauge | — | Current number of active connections. |
| `amqp_connections_created_total` | counter | — | Connections created since start. |
| `amqp_connections_closed_total` | counter | — | Connections closed since start. |
| `amqp_connection_errors_total` | counter | `error_type` | Connection errors by type. Observed values: `read`, `reader_overflow`, `handshake_backlog`, `oversized_ack`; protocol errors use the stringified AMQP reply code. |
| `amqp_connections_closed_by_cause_total` | counter | `cause` | Connections closed by bounded cause. `cause` ∈ `client_eof`, `protocol_error`, `heartbeat_timeout`, `forced_shutdown`, `reader_overflow`, `oversized_ack`, `body_stall`, `handshake_backlog`, `write_error`, `lease_close`. |

### Channels

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_channels_total` | gauge | — | Current number of active channels. |
| `amqp_channels_created_total` | counter | — | Channels created since start. |
| `amqp_channels_closed_total` | counter | — | Channels closed since start. |
| `amqp_channel_errors_total` | counter | `error_type` | Channel errors by type; the label is the stringified AMQP reply code. |

### Consumers

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_consumers_total` | gauge | — | Current number of active consumers. |
| `amqp_consumer_register_errors_total` | counter | — | `basic.consume` registrations refused by the broker. |

### Exchanges

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_exchanges_total` | gauge | — | Current number of exchanges. |
| `amqp_exchanges_declared_total` | counter | — | Exchanges declared since start. |
| `amqp_exchanges_deleted_total` | counter | — | Exchanges deleted since start. |

### Queues

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_queues_total` | gauge | — | Current number of queues. |
| `amqp_queues_declared_total` | counter | — | Queues declared since start. |
| `amqp_queues_deleted_total` | counter | — | Queues deleted since start. |
| `amqp_queue_messages_ready` | gauge | `queue`, `vhost` | Messages ready to be delivered in the queue. |
| `amqp_queue_messages_unacknowledged` | gauge | `queue`, `vhost` | Messages delivered but not yet acknowledged. Emitted only when the broker's unacked depth is known. |
| `amqp_queue_messages_total` | gauge | `queue`, `vhost` | Ready + unacknowledged. Emitted only when both are known. |
| `amqp_queue_consumers` | gauge | `queue`, `vhost` | Number of consumers on the queue. |
| `amqp_queue_depth_high_water` | gauge | `queue`, `vhost` | Maximum queue depth (waiting + inflight) since queue creation (diagnostic). |

### Messages through and depth

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_messages_published_total` | counter | — | Messages published since start. |
| `amqp_messages_published_bytes_total` | counter | — | Bytes published since start. |
| `amqp_messages_delivered_total` | counter | — | Messages delivered to consumers since start. |
| `amqp_messages_delivered_bytes_total` | counter | — | Bytes delivered since start. |
| `amqp_messages_acknowledged_total` | counter | — | Messages acknowledged since start. |
| `amqp_messages_redelivered_total` | counter | — | Messages redelivered since start. |
| `amqp_messages_rejected_total` | counter | — | Messages rejected since start. |
| `amqp_messages_unroutable_total` | counter | — | Unroutable messages since start. |
| `amqp_messages_inflight` | gauge (func) | — | Messages delivered but not yet settled across every queue. |
| `amqp_messages_dropped_total` | counter | `queue`, `reason` | Deprecated; no production writer. Use `amqp_publish_refused_total` and `amqp_deliveries_dropped_total`. |
| `amqp_message_publish_duration_seconds` | histogram | — | Time taken to publish a message. |
| `amqp_message_delivery_duration_seconds` | histogram | — | Time taken to deliver a message to a consumer. |

### Transactions

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_transactions_started_total` | counter | — | Transactions started since start. |
| `amqp_transactions_committed_total` | counter | — | Transactions committed since start. |
| `amqp_transactions_rolledback_total` | counter | — | Transactions rolled back since start. |

### Publish refusal, back-pressure, and parking

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_publish_refused_total` | counter | `reason` | Publishes refused by the broker after routing. `reason` ∈ `x_max_length`, `queue_closed`. (Unroutable is carried by `amqp_messages_unroutable_total`.) |
| `amqp_publish_lease_used_bytes` | gauge (func) | — | Declared publish body bytes (+ framing) currently leased. |
| `amqp_publish_lease_budget_bytes` | gauge (func) | — | Publish-inflight lease budget. |
| `amqp_publish_lease_waiters` | gauge (func) | — | Connections currently queued for a publish lease. |
| `amqp_publish_lease_waits_total` | counter | — | Blocking publish-lease waits (diagnostic). |
| `amqp_publish_lease_oversized_total` | counter | — | Oversized (> budget) leases granted at `used == 0`. |
| `amqp_publish_lease_overbudget_closes_total` | counter | — | Connections closed `506` because a nested header could not be leased. |
| `amqp_publish_lease_body_stall_closes_total` | counter | — | Connections closed for no leased-body progress within the write timeout. |
| `amqp_publish_lease_leaked_total` | counter | — | Publish leases recovered by the pending-message path instead of a proper release; **must stay 0**. |
| `amqp_publish_lease_wait_seconds` | histogram | — | Time spent blocked waiting for a publish lease. |
| `amqp_publisher_parked_total` | counter | — | Completed queue-depth reader parks (one = one pacing episode). |
| `amqp_publisher_park_seconds_total` | counter | — | Total seconds the publisher reader spent parked on queue-depth backpressure. |
| `amqp_publisher_park_duration_seconds` | histogram | — | Duration of one queue-depth reader park. |
| `amqp_publisher_parked_current` | gauge (func) | — | Queue-depth reader parks currently in progress. |
| `amqp_depth_gate_armed_total` | counter | — | Queue-depth reader gates armed on a producer-only connection. |
| `amqp_depth_gated_current` | gauge (func) | — | Connections currently gated by queue-depth reader backpressure. |
| `amqp_reader_paused_total` | counter | — | Times a reader parked on the frame-budget hand-off. |
| `amqp_reader_paused_current` | gauge (func) | — | Readers currently parked on the frame-budget hand-off. |

### Delivery

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_deliveries_requeued_total` | counter | `reason` | Deliveries requeued after a delivery-path failure. `reason` = `write_error`. |
| `amqp_deliveries_dropped_total` | counter | `reason` | Deliveries dropped without requeue — a **message-loss signal**. `reason` = `consumer_removed`. |
| `amqp_delivery_send_errors_total` | counter | — | `basic.deliver` batch write failures to a consumer. |
| `amqp_hot_cache_delivery_hits_total` | counter | `queue` | Deliveries served from the hot cache. |
| `amqp_hot_cache_delivery_cold_reads_total` | counter | `queue` | Deliveries that missed the hot cache and read a cold tier. |
| `amqp_hot_cache_delivery_preads_total` | counter | `queue` | Positional reads the delivery WAL parse issues. |

### WAL and storage

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_wal_size_bytes` | gauge | — | Total size of WAL files. |
| `amqp_wal_write_total` | counter | — | Messages written to the WAL. |
| `amqp_wal_fsync_total` | counter | — | fsync operations on the WAL. |
| `amqp_wal_fsync_duration_seconds` | histogram | — | Time taken for WAL fsync operations. |
| `amqp_wal_write_errors_total` | counter | — | WAL write failures (page-cache write). |
| `amqp_wal_fsync_errors_total` | counter | — | Failed WAL durability barriers (fdatasync / `F_BARRIERFSYNC` / `F_FULLFSYNC`); distinct from a failed write. |
| `amqp_wal_roll_errors_total` | counter | — | Failed WAL roll (open the next file); the current file stays active. |
| `amqp_wal_compaction_errors_total` | counter | — | Failed WAL compactions; the file keeps its bytes and index unchanged. |
| `amqp_wal_reclaim_fsync_errors_total` | counter | — | Failed directory fsync after WAL reclamation. |
| `amqp_wal_files` | gauge | — | Number of `.wal` files in the shared WAL directory (unreclaimed-file leak signal). |
| `amqp_wal_full_flush_total` | counter | `site` | `fcntl(F_FULLFSYNC)` drive-cache flushes issued, by durability site. In `barrier` mode this must stay `0` across the measured window. Site labels: `group_commit`, `tx_confirm`, `tx_dir`, `roll_dir`, `preopen_flush`, `reclaim_dir`, `compaction_file`, `compaction_dir`, `segment_seal`, `segment_temp`, `segment_dir`, `segment_close`, `segment_mkdir`, `metadata_file`, `metadata_dir`, `repair`, `boot_tail`, `boot_mkdir`, `close`, `dir_full`, `fence_fallback`. |
| `amqp_wal_inventory_records` | gauge (func) | — | Records held in the active WAL file's inventory (diagnostic). |
| `amqp_wal_index_copies` | gauge (func) | — | Physical copies across every WAL index shard (diagnostic). |
| `amqp_wal_settle_drained` | gauge (func) | — | Settled records deleted from the active WAL inventory (diagnostic; a gauge, **do not `rate()`**). |
| `amqp_storage_close_errors_total` | counter | — | Failures closing the storage tier during shutdown. |

### Segments

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_segment_count` | gauge | `queue` | Number of segment files per queue. |
| `amqp_segment_size_bytes` | gauge | `queue` | Total size of segment files per queue. |
| `amqp_segment_compaction_total` | counter | — | Segment compactions performed. |
| `amqp_segment_compaction_failures_total` | counter | — | Segments withdrawn from compaction because they cannot be rewritten (no data is lost). |
| `amqp_segment_read_errors_total` | counter | — | Segment read failures. |
| `amqp_segment_roll_errors_total` | counter | — | Failed segment file create/open (roll). |
| `amqp_segment_fsync_errors_total` | counter | — | Failed segment fsync barriers (seal / temp-file) during a rewrite. |

### Recovery

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_recovery_faults_total` | counter | `stage`, `outcome` | Classified recovery faults. `stage` ∈ `storage-validation`, `metadata`, `exchange-recovery`, `queue-recovery`, `message-recovery`, `wal-scan`, `pending-acks`, `ordinal-allocator`; `outcome` ∈ `benign`, `degraded`, `fatal`. A non-zero `degraded` or `fatal` value records an incomplete recovery; a fatal fault can prevent startup. |
| `amqp_recovery_discarded_records_total` | counter | `stage` | Recovered records deliberately discarded or quarantined at boot, by stage (same enum). |
| `amqp_recovery_duration_seconds` | histogram | — | Wall-clock duration of the boot recovery pass. |
| `amqp_recovery_messages_recovered` | gauge | — | Persistent messages recovered into the running broker at boot. |

### Resources, alarms, and authentication

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_resource_alarm_active` | gauge | `resource` | `1` while the named alarm is tripped. `resource` ∈ `memory`, `disk`. |
| `amqp_resource_alarm_trips_total` | counter | `resource` | Alarms raised on the clear→tripped edge. `resource` ∈ `memory`, `disk`. |
| `amqp_auth_failures_total` | counter | `reason` | Refused authentication attempts. `reason` ∈ `host_blocked`, `host_not_allowed`, `user_blocked`, `user_not_allowed`, `rate_limited`, `bad_credentials`, `unsupported_mechanism`. |

### Shutdown

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_shutdown_total` | counter | `result` | Broker shutdowns. `result` ∈ `clean`, `forced`, `incomplete`. |
| `amqp_shutdown_duration_seconds` | histogram | — | Wall-clock duration of `Server.Shutdown`. |
| `amqp_shutdown_connections_forced_total` | counter | — | Connections told to close by broker shutdown. |

### Panics

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_panics_total` | counter | `component` | Recovered panics by bounded component. `component` ∈ `reader`, `processor`, `delivery`, `forwarder`, `heartbeat`, `cleanup`, `other`. |

### Management control plane (`amqp_management_*`)

These families are registered **only** when the management surface is enabled. With it
disabled, none is registered. Every label is bounded.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `amqp_management_requests_total` | counter | `endpoint`, `method`, `status_class` | Requests by bounded route template, HTTP method, and status class. `endpoint` is the route template (e.g. `/api/queues/{vhost}/{name}`) or `pre_route` for a request refused before routing; `status_class` ∈ `2`, `3`, `4`, `5`. |
| `amqp_management_request_duration_seconds` | histogram | `endpoint` | Request latency by route template. |
| `amqp_management_in_flight` | gauge | — | Requests currently being served. |
| `amqp_management_sessions` | gauge | — | Session-store usage. |
| `amqp_management_auth_failures_total` | counter | `reason` | Refused management authentication attempts. `reason` = `bad_credentials`. |
| `amqp_management_rate_limited_total` | counter | — | Requests refused by a rate or in-flight limit. |
| `amqp_management_snapshot_partial_total` | counter | `endpoint` | Degraded snapshots by route template. |
| `amqp_management_snapshot_duration_seconds` | histogram | `endpoint` | Snapshot cost by route template. |
| `amqp_management_snapshot_lock_wait_seconds` | histogram | — | Wait on the permitted `Server.Mutex` read lock during a snapshot. |
| `amqp_management_forbidden_lock_acquired_total` | counter | `lock` | Acquisitions of a forbidden hot-path lock; **structurally 0** (see below). |
| `amqp_management_publish_backpressure_total` | counter | — | HTTP publishes refused `503` by a queue high-water gate. |
| `amqp_management_jobs` | gauge | `state` | Async job pool by state. `state` ∈ `queued`, `running`, `done`, `failed`. |
| `amqp_management_audit_dropped_total` | counter | — | Audit-hook failures; the production logger hook does not report write failures. |
| `amqp_management_capability_denied_total` | counter | `capability` | Requests denied by the capability gate, by capability. |
| `amqp_management_unauthenticated` | gauge | — | `1` when the management surface runs with authentication disabled. |
| `amqp_management_insecure_bind` | gauge | — | `1` when the management listener is non-loopback without TLS. |
| `amqp_management_ui_requests_total` | counter | `status_class` | UI asset/route requests by status class. |
| `amqp_management_panics_total` | counter | `endpoint` | Management handler panics recovered by the dispatch guard, by route template; **must stay 0**. |

## Alerting: "is something wrong?"

The queries below are starting points. Tune thresholds and windows to your traffic and
select the families your backend updates. Standalone WAL/segment alerts do not monitor
cluster journal durability.

**Degraded boot / data loss.**

```promql
# The broker discarded artifacts during unsafe recovery.
amqp_unsafe_recovery_discarded_artifacts > 0

# Classified recovery faults that are not benign.
sum by (stage, outcome) (amqp_recovery_faults_total{outcome!="benign"}) > 0

# Records discarded or quarantined at boot.
increase(amqp_recovery_discarded_records_total[1h]) > 0
```

**Durability failures (WAL and segments).**

```promql
increase(amqp_wal_write_errors_total[5m]) > 0
increase(amqp_wal_fsync_errors_total[5m]) > 0
increase(amqp_wal_roll_errors_total[5m]) > 0
increase(amqp_wal_compaction_errors_total[5m]) > 0
increase(amqp_wal_reclaim_fsync_errors_total[5m]) > 0
increase(amqp_segment_fsync_errors_total[5m]) > 0
increase(amqp_segment_roll_errors_total[5m]) > 0
increase(amqp_segment_read_errors_total[5m]) > 0
increase(amqp_segment_compaction_failures_total[5m]) > 0

# A growing number of unreclaimed WAL files.
amqp_wal_files > 100
```

**Message loss / refusal / no-route.**

```promql
increase(amqp_deliveries_dropped_total[5m]) > 0      # silent message loss
increase(amqp_publish_refused_total[5m]) > 0         # x_max_length / queue_closed
increase(amqp_messages_unroutable_total[5m]) > 0     # mandatory with no route
increase(amqp_delivery_send_errors_total[5m]) > 0
increase(amqp_deliveries_requeued_total[5m]) > 0
```

**Back-pressure and pacing.**

```promql
amqp_publisher_parked_current > 0
amqp_depth_gated_current > 0
amqp_reader_paused_current > 0
rate(amqp_publisher_park_seconds_total[5m]) > 0
```

**Resource alarms.**

```promql
max by (resource) (amqp_resource_alarm_active) == 1
increase(amqp_resource_alarm_trips_total[5m]) > 0
```

**Shutdown and storage teardown.**

```promql
increase(amqp_shutdown_total{result="incomplete"}[1h]) > 0
increase(amqp_storage_close_errors_total[1h]) > 0
increase(amqp_shutdown_connections_forced_total[1h]) > 0
```

**Panics and connection faults.**

```promql
increase(amqp_panics_total[5m]) > 0
increase(amqp_connection_errors_total[5m]) > 0
increase(amqp_channel_errors_total[5m]) > 0
increase(amqp_auth_failures_total{reason="bad_credentials"}[5m]) > 10
```

**Management control plane.**

```promql
# Management 5xx responses (status_class is the literal "5").
sum(rate(amqp_management_requests_total{status_class="5"}[5m])) > 0
sum(rate(amqp_management_requests_total{status_class="4"}[5m])) > 0

increase(amqp_management_rate_limited_total[5m]) > 0
increase(amqp_management_panics_total[5m]) > 0
increase(amqp_management_audit_dropped_total[5m]) > 0
increase(amqp_management_capability_denied_total[5m]) > 0

amqp_management_insecure_bind == 1     # non-loopback without TLS
amqp_management_unauthenticated == 1   # auth disabled
```

## What is deliberately NOT emitted, and why

Some missing or constant values have specific meanings.

- `amqp_message_age_seconds` and `amqp_segment_checkpoint_errors_total` are not
  registered because their former paths no longer have production writers.
  For standalone segment durability, use `amqp_segment_fsync_errors_total` and
  `amqp_segment_roll_errors_total`.
- `amqp_management_forbidden_lock_acquired_total` is registered but always zero
  in the current implementation. Static checks and deadline tests guard the
  management lock boundary. This metric alone does not prove isolation.
- `amqp_recovery_faults_total{stage="recovery"}` is absent. Faults use the
  specific stages listed in the recovery table.
- `amqp_messages_dropped_total` remains registered for compatibility, without
  a production writer. Use the refusal and delivery-drop counters instead.
- `amqp_queue_messages_unacknowledged` and `amqp_queue_messages_total` are
  emitted only when the broker knows the required depths. Missing series do
  not mean zero.
- Above 2,000 queues, the queue poller updates only `amqp_queues_total`.
  Previously emitted per-queue series can retain stale values.
