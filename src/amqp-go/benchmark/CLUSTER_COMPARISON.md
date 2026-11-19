# StrangeQ and RabbitMQ cluster comparison

Run `scripts/cluster_compare.py` from `src/amqp-go` to compare three StrangeQ brokers with three RabbitMQ 4.4.0 brokers running quorum queues. The same RabbitMQ PerfTest 2.25.0 client drives both. The broker processes share one test host and one scratch volume. The results characterize that host and workload.

| Payload | Producers | Consumers | Queues |
| --- | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 1 |
| 1 KiB | 10 | 10 | 10 |
| 64 KiB | 10 | 10 | 10 |

The client uses round-robin connection allocation, a named direct exchange, persistent mandatory publishes, a confirm window of 64 per publisher, and manual acknowledgments with prefetch 100. The RabbitMQ default quorum command window of 32 is explicit. Use `--rabbit-window` to record a separate RabbitMQ tuning trial. Use `--rate-per-producer` to repeat a workload at a fixed offered rate. Accept a matched-load RSS comparison only after checking each broker sustained that load on both its confirm and delivery paths.

## Recorded ARM64 runs

The [README results](../../../README.md#performance) are medians of three paired runs per workload from StrangeQ `f02275a` and RabbitMQ 4.4.0 on the same eight-core ARM64 Linux VM and scratch volume. Both systems used three voters per queue, persistent mandatory publishes, manual acknowledgments, and the same high-entropy payload files supplied to RabbitMQ PerfTest 2.25.0. Trial order alternated, and the volume idled for 45 seconds before each timed run.

The 1 KiB single-queue case ran for 24 seconds with one publisher and one consumer. The 64 KiB ten-queue case ran for 23 seconds with ten publishers, ten consumers, and an offered rate of 500 messages/s per publisher. The throughput medians exclude the first four seconds of each run. Peak RSS is the highest simultaneous sum across the three broker processes for each run.

StrangeQ used 32 shards, the experimental segmented journal, synchronous range writes, consumer assignments, and both body compression options. The 64 KiB case also used two journal streams per broker. Use `--strangeq-shards 32 --strangeq-journal --strangeq-range-sync --strangeq-assignments --strangeq-journal-body-compression --strangeq-journal-small-body-compression` to select the same broker settings. For the 64 KiB case, add `--strangeq-dual-stream --rate-per-producer 500`.

## Set up an isolated comparison

Use Python 3.11 or later, Java, and a release build of `amqp-server`. Obtain and verify the official [RabbitMQ 4.4.0 generic UNIX archive](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.4.0), [Erlang/OTP 28.5.0.8](https://github.com/erlang/otp/releases/tag/OTP-28.5.0.8), and [RabbitMQ PerfTest 2.25.0 shaded JAR](https://github.com/rabbitmq/rabbitmq-perf-test/releases/tag/v2.25.0). The three paths below point to locally extracted or installed copies. The script checks the versions reported by the client and running RabbitMQ nodes. It does not fetch or install packages.

On Linux, build the StrangeQ binary and run both brokers, the client, and the RSS sampler inside the same host, dedicated VM, or unprivileged container. Use the same local scratch filesystem for both brokers. Avoid comparing a native broker against a broker in a VM. Record the filesystem, cache and flush behavior, CPU limits, and memory limits alongside the results. The script uses the binaries' shipping durability settings.

```sh
python3 scripts/cluster_compare.py \
  --strangeq-bin /path/to/amqp-server \
  --rabbitmq-home /path/to/rabbitmq_server-4.4.0 \
  --erlang-bin /path/to/otp-28.5.0.8/bin \
  --perftest-jar /path/to/perf-test-2.25.0.jar \
  --scratch /path/on/test-volume \
  --output /path/to/benchmark-evidence
```

The prerequisites and topology can be checked without publishers using `--validate-only --case 1024-1x1-1q --repeats 1`. A measured run defaults to three interleaved repeats of all three workloads. Trials run serially and alternate which broker starts the pair. Use `--case` for a targeted run. Each trial uses newly created loopback listeners, data directories, a cluster credential, and a RabbitMQ-local EPMD process. The script stops its own processes and removes each trial's temporary data on exit. Logs and measurements remain under `--output`.

Add `--strangeq-journal` to select the experimental Raft journal on a supporting StrangeQ binary. For a topology smoke check, combine the flag with `--validate-only --case 1024-1x1-1q --repeats 1`. Each StrangeQ broker must have a journal identity, a storage mode and provision marker, a segmented journal manifest and extent files, and open replicas for the catalog and workload shards. The checker rejects per-group Bolt files in this mode. `environment.json` records which StrangeQ storage engine ran. Provision fresh storage for the segmented journal. It rolls into 256 MiB extents and reclaims sealed files once a durable recovery index, live messages, and readers no longer need them. The 8 GiB budget counts retained extent and index bytes per node and is shared across streams. Cumulative appends can exceed that budget as files are reclaimed. Snapshot blobs have a separate 8 GiB budget per stream.

Add `--strangeq-ack-and-claim` with `--strangeq-journal` to measure combined manual ACK and delivery refill. The checker verifies the corresponding storage mode on all three brokers before and after the trial. The mode is recorded in `environment.json`.

## macOS durability comparison

`--darwin-diagnostic` explicitly enables same-host native runs. Report stock macOS and controlled full-flush trials as separate result sets. The historical checkout used macOS `F_FULLFSYNC` for StrangeQ's bbolt sync and `F_BARRIERFSYNC` for the stock Erlang `_efile_sync` implementation. A controlled comparison can use a separately verified local Erlang build that calls `F_FULLFSYNC`, passed through `--erlang-bin`. The runner records the active Erlang executable's hash and, on arm64 macOS with `otool` available, saves the `_efile_sync` disassembly and identifies a recognized `fcntl` selector 85 or 51. Check the actual StrangeQ storage path and relevant RabbitMQ runtime calls before establishing flush parity. The runner labels all macOS data diagnostic.

## Read the results

`summary.json` reports the median and range for sent publishes/s, confirmed publishes/s, deliveries/s, highest combined broker RSS, and both p99 latencies. Throughput averages complete one-second samples after the configured warmup. Compare the actual send rates when interpreting capped runs. PerfTest's p99 covers its full timed run. RSS is the maximum of the sums observed in simultaneous three-PID samples from client launch through client exit. It excludes the client and EPMD. Each run keeps its raw PerfTest log, RSS samples, command, and before/after placement checks.

RabbitMQ checks include queue durability and type, three online members, actual leader distribution, and pinned release versions. StrangeQ checks confirm three enrolled catalog voters and three committed voters with local storage for every queue's shard. Its placement evidence records preferred leaders using the catalog's ranking algorithm. The existing server exposes no external per-shard leader query for verifying the actual leaders in this harness.

A measured run requires every interval after warm-up, positive confirms and deliveries, and zero observed negative confirms, returns, or client errors across the log. Missing warm-up intervals are counted in each trial's `result.json`. PerfTest measures rates during a time window. Use separate functional tests for publish-to-delivery reconciliation, durable restart, and failover. Compare peak RSS both at saturation and at matched achieved throughput. Repeat with controlled backlogs separately when assessing queue paging.

Run the offline parser and placement fixtures with `python3 -B -m unittest discover -s scripts -p test_cluster_compare.py`.
