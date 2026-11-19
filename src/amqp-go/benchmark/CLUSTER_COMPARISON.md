# StrangeQ and RabbitMQ cluster comparison

Run `scripts/cluster_compare.py` from `src/amqp-go` to compare StrangeQ with RabbitMQ 4.4.0. The default `--topology quorum3` compares three StrangeQ voters with three RabbitMQ brokers running quorum queues. `--topology singleton` compares one local-journal StrangeQ broker with one RabbitMQ broker running durable Classic queues. The same RabbitMQ PerfTest 2.25.0 client drives both engines. The broker processes share one test host and one scratch volume. The results characterize that host and workload; keep the two topologies in separate result sets.

| Payload | Producers | Consumers | Queues |
| --- | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 1 |
| 1 KiB | 10 | 10 | 10 |
| 64 KiB | 10 | 10 | 10 |

The client uses round-robin connection allocation, a named direct exchange, persistent mandatory publishes, a confirm window of 64 per publisher, and manual acknowledgments with prefetch 100 in both modes. The RabbitMQ quorum command window of 32 is explicit in `quorum3`; it does not apply to Classic. Use `--rabbit-window` to record a separate quorum tuning trial. Use `--rate-per-producer` to repeat any workload in either mode at a fixed offered rate. Accept a matched-load RSS comparison only after checking each broker sustained that load on both its confirm and delivery paths.

`--payload-profile perftest` is the default and preserves the command's built-in PerfTest bodies. In [PerfTest 2.25.0](https://github.com/rabbitmq/rabbitmq-perf-test/blob/v2.25.0/src/main/java/com/rabbitmq/perf/TimeSequenceMessageBodySource.java), these contain changing sequence/timestamp bytes with a zero-filled remainder, so they are highly compressible. For a separate high-entropy run, add `--payload-profile high-entropy`. The runner creates one deterministic SHAKE-256 binary fixture per selected case, passes its path with `--body`, and reuses identical bytes for both engines and every repeat. These are reused payloads, not unique bodies per message. `environment.json` records profile, body size, fixture path, seed and SHA-256. The default profile has no single body digest because its sequence/timestamp varies, recorded as a null SHA with that reason. Keep payload profiles in separate result sets.

## Recorded ARM64 runs

These historical normal-workload measurements precede the replicated dead-lettering
results below. The [historical README results](../../../README.md#historical-normal-workloads) are medians of three paired runs per workload from StrangeQ `f02275a` and RabbitMQ 4.4.0 on the same eight-core ARM64 Linux VM and scratch volume. Both systems used three voters per queue, persistent mandatory publishes, manual acknowledgments, and the same high-entropy payload files supplied to RabbitMQ PerfTest 2.25.0. Trial order alternated, and the volume idled for 45 seconds before each timed run.

The 1 KiB single-queue case ran for 24 seconds with one publisher and one consumer. The 64 KiB ten-queue case ran for 23 seconds with ten publishers, ten consumers, and an offered rate of 500 messages/s per publisher. The throughput medians exclude the first four seconds of each run. Peak RSS is the highest simultaneous sum across the three broker processes for each run.

StrangeQ used 32 shards, the experimental segmented journal, synchronous range writes, consumer assignments, and both body compression options. The 64 KiB case also used two journal streams per broker. To replay these historical measurements, use both the runner and broker binary from `f02275a`, with that revision's `--strangeq-shards 32 --strangeq-journal --strangeq-range-sync --strangeq-assignments --strangeq-journal-body-compression --strangeq-journal-small-body-compression` settings. For the 64 KiB case, add `--strangeq-dual-stream --rate-per-producer 500`. The current runner requires the unconditional segmented journal and no longer accepts `--strangeq-journal`; a new run of the current code is a separate result set.

## Replicated dead-lettering runs

On November 19, 2026, StrangeQ `bf52074` and RabbitMQ 4.4.0 ran three paired,
order-alternating active dead-lettering trials on the same native eight-core
ARM64 Linux VM and local scratch volume. Both used three durable replicas per
queue, ten source queues and ten dead-letter targets. StrangeQ targets were on
separate shards from their sources. RabbitMQ used quorum queues with
`dead-letter-strategy=at-least-once` and `overflow=reject-publish`.

A separate reconciliation client used ten producers, ten source consumers, and
ten target consumers. It published persistent 65,536-byte high-entropy messages,
with an offered limit of 500 messages/s per producer, a confirm window of 64,
64 end-to-end credits per queue, and prefetch 100. Each source delivery received
a separate NACK with requeue disabled. The client reused one high-entropy body
with distinct message IDs. Rates cover seconds 4–12 of each 12-second run.
Peak RSS is the highest simultaneous sum across the three broker processes.
Each trial used fresh storage, the same between-trial filesystem flush, and a
five-second topology quiet period before client start.

StrangeQ used 32 shards, the experimental dual-stream segmented journal,
synchronous range writes, consumer assignments, and both body compression
settings. The measurements cover short steady-state windows on this host and
storage configuration, using one reused payload body. Crash recovery, failover,
and unique payload bodies require separate measurements.
Replay this separate reconciliation workload with the recorded `bf52074` binary
and original reconciliation harness/configuration. The normal-workload runner
below does not reproduce this DLQ client.

| Pair | StrangeQ confirms/s | StrangeQ DLQ deliveries/s | StrangeQ RSS (MiB) | RabbitMQ confirms/s | RabbitMQ DLQ deliveries/s | RabbitMQ RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,915 | 1,921 | 634 | 1,663 | 1,673 | 1,835 |
| 2 | 1,941 | 1,941 | 618 | 1,347 | 1,352 | 1,930 |
| 3 | 1,821 | 1,822 | 627 | 1,672 | 1,684 | 1,882 |
| Median | 1,915 | 1,921 | 627 | 1,663 | 1,673 | 1,882 |

StrangeQ had higher confirms and DLQ deliveries and lower peak RSS in each pair.
After draining, each run reconciled all sent, confirmed, source-rejected, and
DLQ-delivered messages, with zero observed duplicates. Replicated dead-lettering
remains [at-least-once](../../../docs/DEAD_LETTERING.md#replicated-queues). Retrying
a transfer after an uncertain destination outcome can produce duplicates.

Two separate same-cluster controls enabled an idle policy on half the source
queues, then swapped the enabled half. All target queues remained declared.
Messages were acknowledged normally. Each control ran for ten seconds, measuring
seconds 4–10. Enabled versus disabled queues differed by -0.04% in confirms and
-0.06% in deliveries in the first control, and -0.04% and +0.16% in the second.
All publications reconciled. These controls compare queue groups within each run.

[Recorded per-run counts, rates, RSS, and environment](REPLICATED_DLQ_RESULTS.json)
include the active trials and idle-policy lane counts used for these figures.

## Set up an isolated comparison

Use Python 3.11 or later, Java, and a release build of `amqp-server`. Obtain and verify the official [RabbitMQ 4.4.0 generic UNIX archive](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.4.0), [Erlang/OTP 28.5.0.8](https://github.com/erlang/otp/releases/tag/OTP-28.5.0.8), and [RabbitMQ PerfTest 2.25.0 shaded JAR](https://github.com/rabbitmq/rabbitmq-perf-test/releases/tag/v2.25.0). The three paths below point to locally extracted or installed copies. The script checks the versions reported by the client and running RabbitMQ nodes. It does not fetch or install packages.

On Linux, build the StrangeQ binary and run both brokers, the client, and the RSS sampler inside the same host, dedicated VM, or unprivileged container. Use the same local scratch filesystem for both brokers. Avoid comparing a native broker against a broker in a VM. Record the filesystem, cache and flush behavior, CPU limits, and memory limits alongside the results. RabbitMQ uses its shipping durability settings. StrangeQ uses the explicit `--strangeq-durability` profile, defaulting to the unchanged shipping `barrier` setting.

```sh
python3 scripts/cluster_compare.py \
  --strangeq-bin /path/to/amqp-server \
  --rabbitmq-home /path/to/rabbitmq_server-4.4.0 \
  --erlang-bin /path/to/otp-28.5.0.8/bin \
  --perftest-jar /path/to/perf-test-2.25.0.jar \
  --scratch /path/on/test-volume \
  --output /path/to/benchmark-evidence
```

Add `--topology singleton` to this command for the configured [local-journal pilot](../../../docs/CLUSTERING.md#pilot-start-locally-then-activate-peers). It writes `cluster: {local: true}` with the requested `--strangeq-shards`, starts once without an initialization command, and supplies no peer endpoint or credential. RabbitMQ starts one node with Classic as the explicit default; the client omits its quorum-queue declaration flag. This measures the local journal, not the legacy standalone engine.

`--strangeq-durability {barrier,flush,process}` writes the selected value to `storage.durability` and records it in `environment.json` and each timed result. `barrier` preserves the default synchronous journal behavior; `flush` requests the full drive-cache flush profile where the platform distinguishes it. Both use synchronous durability on Linux. `process` is allowed only with `--topology singleton`: eligible local-journal confirms follow completed kernel writes without a per-confirm disk fence. It protects against broker-process crashes while the OS remains running; it does not promise survival of an OS crash or power loss. Required structural fences remain enabled. Peer activation permanently upgrades the local journal to full durability. Keep `process` results separate from full-durability comparisons with RabbitMQ Classic.

For a raw 64 KiB high-entropy singleton A/B on a native ARM64 Linux host, build and verify native ARM64 binaries, then run the following once with `process` and once with `barrier`, saving distinct evidence directories. Leave every other setting and the scratch device unchanged. The 65,536-byte fixture is reused for both brokers; journal compression, range sync, dual stream, assignments, and replica bodies are all off in this profile. Each invocation still contains its own three order-alternating StrangeQ/RabbitMQ pairs.

```sh
python3 scripts/cluster_compare.py \
  --strangeq-bin /path/to/native-arm64/amqp-server \
  --rabbitmq-home /path/to/rabbitmq_server-4.4.0 \
  --erlang-bin /path/to/native-arm64/otp-28.5.0.8/bin \
  --perftest-jar /path/to/perf-test-2.25.0.jar \
  --scratch /path/on/test-volume \
  --output /path/to/benchmark-evidence/process-raw64k \
  --topology singleton --strangeq-shards 32 \
  --strangeq-durability process --payload-profile high-entropy \
  --case 65536-10x10-10q --duration 24 --warmup 4 --repeats 3
```

This is a saturation profile with ten producers, ten consumers, ten queues, confirm window 64 and prefetch 100. A separate matched-load run must add the same `--rate-per-producer` to both durability invocations and check achieved confirm and delivery rates. `environment.json` records host architecture (`machine`), the exact payload digest, binary hashes and selected settings; verify the binaries' architectures separately because the host architecture alone does not prove native execution.

The prerequisites and topology can be checked without publishers using `--validate-only --case 1024-1x1-1q --repeats 1` in either mode. A measured run defaults to three interleaved repeats of all three workloads. Trials run serially and alternate which broker starts the pair. Use `--case` for a targeted run. Each trial uses fresh data directories and loopback application listeners; `quorum3` also configures peer listeners and a cluster credential. RabbitMQ has a private EPMD process in both modes. The script stops its own processes and removes temporary trial data on exit. Logs and measurements remain under `--output`.

The current runner always requires the segmented Raft journal and emits no backend opt-in setting. Every StrangeQ broker must have a journal identity, a storage mode and provision marker, a segmented journal manifest and extent files, and open replicas for the catalog and workload shards. These checks always run before and after each trial, including `--validate-only`; legacy per-group Bolt files are rejected. `environment.json` records `strangeq_storage: segmented_journal`, the binary hash and every active option below. Provision fresh storage; existing bbolt Raft stores and flat journals are not converted. The journal rolls into 256 MiB extents and reclaims sealed files once a durable recovery index, live messages, and readers no longer need them. The 8 GiB budget counts retained extent and index bytes per node and is shared across streams. Cumulative appends can exceed that budget as files are reclaimed. Snapshot blobs have a separate 8 GiB budget per stream. Ordinary standalone storage is unchanged by this Raft backend transition.

Optional settings remain explicit and default off: `--strangeq-range-sync`, `--strangeq-dual-stream`, `--strangeq-ack-and-claim`, `--strangeq-assignments`, `--strangeq-replica-bodies`, `--strangeq-journal-body-compression`, and `--strangeq-journal-small-body-compression`. Dual stream requires range sync; ACK-and-claim and assignments use separate modes; replica bodies and compression require assignments; small-body compression requires body compression. The checker verifies the selected storage mode on every broker before and after the trial. See [journal configuration and restart requirements](../../../docs/CLUSTERING.md#shared-segmented-journal). `--strangeq-shards` controls fresh-cluster shard count in both topologies.

## macOS durability comparison

`--darwin-diagnostic` explicitly enables same-host native runs. Report stock macOS and controlled full-flush trials as separate result sets. The historical checkout used macOS `F_FULLFSYNC` for StrangeQ's bbolt sync and `F_BARRIERFSYNC` for the stock Erlang `_efile_sync` implementation. A controlled comparison can use a separately verified local Erlang build that calls `F_FULLFSYNC`, passed through `--erlang-bin`. The runner records the active Erlang executable's hash and, on arm64 macOS with `otool` available, saves the `_efile_sync` disassembly and identifies a recognized `fcntl` selector 85 or 51. Check the actual StrangeQ storage path and relevant RabbitMQ runtime calls before establishing flush parity. The runner labels all macOS data diagnostic.

## Read the results

`summary.json` reports the median and range for sent publishes/s, confirmed publishes/s, deliveries/s, highest combined broker RSS, and both p99 latencies. Throughput averages complete one-second samples after the configured warmup. Compare actual send rates when interpreting capped runs. PerfTest's p99 covers its full timed run. The existing `peak_cluster_rss_bytes` field is the peak simultaneous sum of three broker PIDs in `quorum3` or the single broker PID in `singleton`, excluding client and EPMD. `environment.json` and each timed result record topology, process count and RSS scope. Each run keeps its raw PerfTest log, RSS samples, command and before/after placement checks.

Every timed Linux trial, for both brokers and every durability profile, also retains `storage-samples.json`: a baseline before client start, roughly one sample per second, and a final sample after client exit. It reads small kernel counter files without launching another sampler process. The counters have distinct scopes:

| Counter | Meaning |
| --- | --- |
| `peak_cluster_rss_bytes` / `rss-samples.json` | Broker-process resident memory, excluding the client and EPMD. This is not a measurement of filesystem page cache. |
| `scratch_device_written_bytes_delta` | End-minus-start Linux scratch backing block-device written sectors, converted with 512 bytes per sector. Includes unrelated I/O to that device; does not prove drive-cache flush or stable-media persistence. Raw samples also retain cumulative read/write bytes and the result records the read-byte delta. |
| `host_cache_dirty_bytes` / `peak_host_cache_dirty_bytes` | Host-wide `/proc/meminfo` Dirty pages awaiting writeback, in bytes. |
| `host_cache_writeback_bytes` / `peak_host_cache_writeback_bytes` | Host-wide `/proc/meminfo` Writeback pages currently being written, in bytes. |

Dirty and Writeback are separate counters and exclude clean page cache; neither is attributable to an individual broker PID. The device identity and scope are recorded in the file's context. Filesystems without a `/sys/dev/block` entry, including some container filesystems, retain Dirty/Writeback evidence and an explicit reason for unavailable device counters. The result's cache peaks and I/O deltas cover the full client lifetime, including warmup, and omit any background writeback after client exit. Storage and RSS samples survive rejected-workload parsing so the paired evidence remains inspectable. Use a quiet dedicated host and inspect the raw cache trajectory when comparing `process` against `barrier`; short runs may end with deferred kernel writes and do not establish sustained storage throughput.

In `quorum3`, RabbitMQ checks retain queue durability/type, three online members, actual leader distribution and pinned versions. StrangeQ retains three enrolled catalog voters and three committed voters with local storage for every workload shard; evidence records preferred leaders. In `singleton`, RabbitMQ must report one pinned-version node and unique durable Classic queues in `running` state, using the documented [queue inspection fields](https://www.rabbitmq.com/docs/man/rabbitmqctl.8#list_queues). StrangeQ must have a private `Local:true` persistent identity, its generated lineage and sole blank-address seed matching the catalog, no peer-activation intent, sole-voter committed membership reports with positive configuration indexes, and open journal groups for catalog/workload shards. Those reports are a persisted observation rather than a live Raft-status query; the unactivated local lifecycle provides no peer membership ingress. The server exposes no external per-shard live leader/membership query in this harness.

A measured run requires every interval after warm-up, positive confirms and deliveries, and zero observed negative confirms, returns, or client errors across the log. Missing warm-up intervals are counted in each trial's `result.json`. PerfTest measures rates during a time window. Use separate functional tests for publish-to-delivery reconciliation, durable restart, and failover. Compare peak RSS both at saturation and at matched achieved throughput. Repeat with controlled backlogs separately when assessing queue paging.

Run the offline parser and placement fixtures with `python3 -B -m unittest discover -s scripts -p test_cluster_compare.py`.
