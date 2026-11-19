#!/usr/bin/env python3
"""Compare local-journal or three-voter StrangeQ with RabbitMQ using one pinned client."""

import argparse
from collections import Counter
from contextlib import ExitStack
from dataclasses import dataclass
import hashlib
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time


RABBITMQ_VERSION = "4.4.0"
OTP_VERSION = "28.5.0.8"
PERF_TEST_VERSION = "2.25.0"
LOOPBACK = "127.0.0.1"
VOTERS = {1, 2, 3}
TOPOLOGIES = {"quorum3": 3, "singleton": 1}
CATALOG_GROUP = (1 << 64) - 1
EXCHANGE = "bench-exchange"
PAYLOAD_SEED_VERSION = "strangeq-cluster-compare/high-entropy/v1"


class BenchmarkError(RuntimeError):
    pass


class PendingMembershipReport(BenchmarkError):
    """Valid singleton topology whose asynchronous committed reports are not ready."""


class RejectedWorkload(BenchmarkError):
    pass


@dataclass(frozen=True)
class Case:
    size: int
    clients: int
    queues: int

    @property
    def name(self):
        return f"{self.size}-{self.clients}x{self.clients}-{self.queues}q"

    @property
    def queue_names(self):
        if self.queues == 1:
            return [f"{self.name}-queue"]
        return [f"{self.name}-{i}" for i in range(1, self.queues + 1)]


CASES = {case.name: case for case in (Case(1024, 1, 1), Case(1024, 10, 10), Case(65536, 10, 10))}
NUMBER = r"(\d+(?:\.\d+)?)"
RATE_LINE = re.compile(
    rf"^id: [^,]+, time {NUMBER} s, sent: {NUMBER} msg/s, "
    rf"returned: {NUMBER} msg/s, confirmed: {NUMBER} msg/s, nacked: {NUMBER} msg/s, received: {NUMBER} msg/s",
    re.MULTILINE,
)
LATENCY_LINE = re.compile(
    r"^id: [^,]+, (consumer|confirm) latency min/median/75th/95th/99th/max "
    r"\d+/\d+/\d+/\d+/(\d+)/\d+ [µμ]s$",
    re.MULTILINE,
)
CLIENT_ERROR = re.compile(
    r"\b(?:ERROR|Exception|[A-Za-z.]+Exception|PRECONDITION_FAILED|NOT_FOUND|RESOURCE_ERROR)\b"
)
ERLANG_BEAM_PATH = 'filename:join([code:root_dir(), "erts-" ++ erlang:system_info(version), "bin", "beam.smp"])'


def parse_perf_test(output, duration, warmup):
    if CLIENT_ERROR.search(output) or "test stopped (Reached time limit)" not in output:
        raise BenchmarkError("PerfTest reported an error or stopped before the configured time")
    samples = {}
    for elapsed, sent, returned, confirmed, nacked, received in RATE_LINE.findall(output):
        if float(nacked) or float(returned):
            raise RejectedWorkload("PerfTest saw a negative confirm or mandatory return")
        timestamp = float(elapsed)
        second = round(timestamp)
        if 1 <= second <= duration and abs(timestamp - second) <= 0.35:
            if second in samples:
                raise BenchmarkError(f"duplicate PerfTest sample for second {second}")
            samples[second] = [float(sent), float(confirmed), float(received)]
    steady_seconds = range(warmup + 1, duration + 1)
    missing_steady = [second for second in steady_seconds if second not in samples]
    if missing_steady:
        raise BenchmarkError(f"PerfTest did not report complete measured intervals for seconds {missing_steady}")
    steady = [samples[second] for second in steady_seconds]
    rates = dict(zip(("sent", "confirmed", "received"), map(statistics.mean, zip(*steady))))
    p99 = {kind: int(value) for kind, value in LATENCY_LINE.findall(output)}
    if not all(rates.values()) or set(p99) != {"consumer", "confirm"}:
        raise BenchmarkError("PerfTest lacks positive send, confirm, delivery, or latency evidence")
    return {
        "rates": rates, "p99_us": p99,
        "steady_intervals": len(steady), "missing_warmup_intervals": warmup + len(steady) - len(samples),
    }


def peak_rss(samples, broker_count=3):
    if not samples or any(len(values) != broker_count or min(values) <= 0 for _, values in samples):
        raise BenchmarkError(f"RSS requires simultaneous samples from all {broker_count} broker processes")
    return max(sum(values) for _, values in samples)


def parse_rabbit_queues(output, case, nodes, *, topology="quorum3"):
    queues = {}
    if topology == "singleton" and len(nodes) != 1:
        raise BenchmarkError("RabbitMQ Classic comparison requires exactly one broker")
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        if topology == "singleton":
            if len(fields) != 4:
                raise BenchmarkError(f"unexpected RabbitMQ Classic queue listing: {line}")
            name, kind, durable, state = fields
            if name in queues or kind != "classic" or durable != "true" or state != "running":
                raise BenchmarkError(f"{name} is not a unique durable running Classic queue")
            queues[name] = {"leader": next(iter(nodes)), "type": "classic", "durable": True}
            continue
        if len(fields) != 6:
            raise BenchmarkError(f"unexpected RabbitMQ queue listing: {line}")
        name, kind, durable, leader, members, online = fields
        listed = [entry.strip("[]' \"") for entry in members.split(",")]
        live = [entry.strip("[]' \"") for entry in online.split(",")]
        if (name in queues or kind != "quorum" or durable != "true" or leader not in nodes
                or len(listed) != 3 or set(listed) != nodes or len(live) != 3 or set(live) != nodes):
            raise BenchmarkError(f"{name} is not a unique durable quorum queue with three online voters")
        queues[name] = {"leader": leader, "voters": listed}
    if set(queues) != set(case.queue_names):
        raise BenchmarkError("RabbitMQ queue names differ from the requested topology")
    return queues


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def prepare_payloads(profile, cases, output):
    payloads = {}
    for case in cases:
        if profile == "high-entropy":
            seed = f"{PAYLOAD_SEED_VERSION}/{case.name}"
            body = hashlib.shake_256(seed.encode("ascii")).digest(case.size)
            path = output / f"payload-{case.name}.bin"
            path.write_bytes(body)
            payloads[case.name] = {"size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                                   "path": str(path), "seed": seed, "source": "SHAKE-256 reused binary fixture"}
        else:
            payloads[case.name] = {"size_bytes": case.size, "sha256": None, "path": None,
                                   "source": "PerfTest time/sequence body with zero-filled remainder",
                                   "sha256_reason": "sequence and timestamp vary per publish"}
    return payloads


def free_ports(count):
    # Hold the reservations together to keep distinct ports within a trial.
    # The actual broker binds them during startup and launch errors fail the trial.
    with ExitStack() as sockets:
        listeners = [sockets.enter_context(socket.socket()) for _ in range(count)]
        for listener in listeners:
            listener.bind((LOOPBACK, 0))
        return [listener.getsockname()[1] for listener in listeners]


def stop_process(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    except ProcessLookupError:
        process.wait()


def spawn(stack, command, cwd, env, log):
    with log.open("wb") as stream:
        process = subprocess.Popen(
            command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
            stdout=stream, stderr=subprocess.STDOUT, start_new_session=True,
        )
    stack.callback(stop_process, process)
    return process


def run_logged(command, cwd, env, log, timeout=40):
    with ExitStack() as stack:
        process = spawn(stack, command, cwd, env, log)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise BenchmarkError(f"command timed out; see {log}") from error
    if code:
        raise BenchmarkError(f"command exited {code}; see {log}")
    return log.read_text(encoding="utf-8", errors="replace")


def wait_until_ready(process, port, log, *, health=False):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline and process.poll() is None:
        try:
            if not health:
                with socket.create_connection((LOOPBACK, port), timeout=0.5):
                    return
            else:
                connection = HTTPConnection(LOOPBACK, port, timeout=0.5)
                try:
                    connection.request("GET", "/health")
                    response = connection.getresponse()
                    if response.status == 200 and response.read().startswith(b"OK"):
                        return
                finally:
                    connection.close()
        except OSError:
            pass
        time.sleep(0.1)
    raise BenchmarkError(f"process {process.pid} did not reach readiness on port {port}; see {log}")


def launch_strangeq(args, stack, scratch, output, env):
    local = args.topology == "singleton"
    count = TOPOLOGIES[args.topology]
    ports = free_ports(count * (2 if local else 3))
    amqp = ports[:count]
    health = ports[count:] if local else ports[2 * count:]
    peers = [None] if local else ports[count:2 * count]
    if not local:
        token = scratch / "peer.token"
        token.write_text(secrets.token_urlsafe(32), encoding="ascii")
    seeds = [] if local else [f"{i}@{LOOPBACK}:{port}" for i, port in enumerate(peers, 1)]
    paths, configs = [], []
    for i, (amqp_port, peer_port, health_port) in enumerate(zip(amqp, peers, health), 1):
        path = scratch / f"node-{i}"
        config = scratch / f"node-{i}.yaml"
        cluster_config = {"local": True} if local else {
            "id": scratch.name, "node_id": i, "peer_address": f"{LOOPBACK}:{peer_port}",
            "seeds": seeds, "token_file": str(token), "allow_loopback_plaintext": True,
        }
        if args.strangeq_range_sync:
            cluster_config["experimental_raft_range_sync"] = True
        if args.strangeq_dual_stream:
            cluster_config["experimental_raft_journal_dual_stream"] = True
        if args.strangeq_ack_and_claim:
            cluster_config["experimental_ack_and_claim"] = True
        if args.strangeq_assignments:
            cluster_config["experimental_assignments"] = True
        if args.strangeq_replica_bodies:
            cluster_config["experimental_replica_bodies"] = True
        if args.strangeq_journal_body_compression:
            cluster_config["experimental_journal_body_compression"] = True
        if args.strangeq_journal_small_body_compression:
            cluster_config["experimental_journal_small_body_compression"] = True
        if args.strangeq_shards is not None:
            cluster_config["shards"] = args.strangeq_shards
        write_json(config, {
            "network": {"address": f"{LOOPBACK}:{amqp_port}"},
            "storage": {"path": str(path), "durability": args.strangeq_durability},
            "telemetry": {"enabled": True, "address": LOOPBACK, "port": health_port},
            "cluster": cluster_config,
        })
        if not local:
            run_logged([args.strangeq_bin, "--config", config, "--initialize-cluster"], scratch, env,
                       output / f"init-{i}.log")
        paths.append(path)
        configs.append(config)
    logs = [output / f"broker-{i}.log" for i in range(1, count + 1)]
    brokers = [spawn(stack, [args.strangeq_bin, "--config", config], scratch, env, log)
               for config, log in zip(configs, logs)]
    for broker, port, log in zip(brokers, health, logs):
        wait_until_ready(broker, port, log, health=True)
    seed_peers = None if local else [{"ID": i, "Address": f"{LOOPBACK}:{port}"} for i, port in enumerate(peers, 1)]
    return brokers, amqp, (paths, seed_peers)


def verify_journal_group(cluster, group):
    opened = cluster / f"opened-{group}"
    if (not opened.is_file() or opened.read_bytes() != b"\x01"
            or os.path.lexists(cluster / f"retired-{group}")):
        raise BenchmarkError(f"StrangeQ journal lacks a local open replica of group {group} at {cluster}")


def verify_journal_layout(cluster, ack_and_claim, assignments=False, body_compression=False, dual_stream=False,
                          *, small_body_compression=False):
    mode = cluster / "raft-storage-mode-v1"
    provisioned = cluster / "journal-provisioned-v1"
    expected_mode = b"journal-ack-and-claim" if ack_and_claim else b"journal"
    if assignments:
        expected_mode = b"journal-assignments"
    if body_compression:
        expected_mode = b"journal-assignments-compressed-bodies"
    if small_body_compression:
        expected_mode = b"journal-assignments-small-compressed-bodies"
    expected_mode = b"segmented-" + expected_mode
    if dual_stream:
        expected_mode += b"-two-stream-parity-v1"
    expected_mode += b"-dead-letter-v2"
    if (not mode.is_file() or mode.read_bytes() != expected_mode
            or not provisioned.is_file() or provisioned.read_bytes() != b"\x01"):
        raise BenchmarkError(f"StrangeQ journal mode or provision marker is invalid at {cluster}")
    locations = ("raft-journal-0", "raft-journal-1") if dual_stream else ("raft-journal",)
    journals = {cluster / name for name in locations}
    if ({path.parent for path in cluster.rglob("segments.manifest")} != journals
            or any(not any(file.is_file() and file.stat().st_size > 0
                           for file in directory.glob("extent-*.raft-journal")) for directory in journals)
            or any(path.parent not in journals for path in cluster.rglob("extent-*.raft-journal"))
            or next(cluster.rglob("active.raft-journal"), None) is not None):
        raise BenchmarkError(f"StrangeQ segmented journal files do not match expected paths {journals}")
    if os.path.lexists(cluster / "identity.json") or next(cluster.rglob("raft.db"), None) is not None:
        raise BenchmarkError(f"StrangeQ journal and per-group Bolt storage coexist at {cluster}")
    verify_journal_group(cluster, CATALOG_GROUP)


def verify_strangeq_placement(context, case, *, ack_and_claim=False, assignments=False,
                              body_compression=False, dual_stream=False, small_body_compression=False,
                              expected_shards=None, topology="quorum3"):
    paths, seeds = context
    if topology == "singleton":
        return verify_strangeq_singleton(
            context, case, ack_and_claim=ack_and_claim, assignments=assignments,
            body_compression=body_compression, dual_stream=dual_stream,
            small_body_compression=small_body_compression, expected_shards=expected_shards,
        )
    expected_cluster = paths[0].parent.name
    clusters = [path / "cluster" for path in paths]
    views = []
    for cluster, seed in zip(clusters, seeds, strict=True):
        verify_journal_layout(cluster, ack_and_claim, assignments, body_compression, dual_stream,
                              small_body_compression=small_body_compression)
        identity_path = cluster / "journal-identity-v1.json"
        if small_body_compression:
            if os.path.lexists(identity_path):
                raise BenchmarkError(f"StrangeQ small-body journal and legacy identities coexist at {cluster}")
            identity_path = cluster / "journal-small-body-identity-v1.json"
        if not identity_path.is_file():
            raise BenchmarkError(f"StrangeQ local identity is missing at {identity_path}")
        identity = json.loads(identity_path.read_text())
        view = json.loads((cluster / "catalog-cache").read_text())
        # Original voters have an empty identity nonce. The catalog nonce map
        # is allocated only when a peer joins after the initial provisioning.
        if (identity["Cluster"] != expected_cluster or identity["Peer"] != seed
                or identity["Seeds"] != seeds or identity["Nonce"] != ""
                or identity["Shards"] != view["Shards"] or view["Cluster"] != expected_cluster
                or view["Nodes"] != seeds or view["Nonces"] not in (None, {})
                or view["Catalog"]["Voters"] != sorted(VOTERS) or view["Catalog"]["Learners"]
                or view["CatalogTarget"] != sorted(VOTERS)):
            raise BenchmarkError("StrangeQ catalog or local identity differs from the three original voters")
        views.append(view)
    shards = views[0]["Shards"]
    if any(view["Shards"] != shards for view in views):
        raise BenchmarkError("StrangeQ catalogs disagree on the shard count")
    if expected_shards is not None and shards != expected_shards:
        raise BenchmarkError(f"StrangeQ trial requires {expected_shards} shards but catalog has {shards}")
    tables = [{row["Group"]: row for row in view["Placements"]} for view in views]
    placements = {}
    for name in case.queue_names:
        digest = hashlib.sha256(name.encode()).digest()
        group = int.from_bytes(digest[:8], "big") % shards + 1
        if any(set(table[group]["Voters"]) != VOTERS or table[group]["Learners"] for table in tables):
            raise BenchmarkError(f"{name} lacks three catalog-committed StrangeQ shard voters")
        for cluster in clusters:
            verify_journal_group(cluster, group)
        preferred = max(sorted(VOTERS), key=lambda node: hashlib.sha256(
            group.to_bytes(8, "big") + node.to_bytes(8, "big")
        ).digest()[:8])
        placements[name] = {"shard": group, "preferred_leader": preferred, "voters": sorted(VOTERS)}
    return placements


def verify_strangeq_singleton(context, case, *, ack_and_claim=False, assignments=False,
                              body_compression=False, dual_stream=False, small_body_compression=False,
                              expected_shards=None):
    paths, seeds = context
    if len(paths) != 1 or seeds is not None:
        raise BenchmarkError("local StrangeQ comparison requires exactly one unseeded broker")
    cluster = paths[0] / "cluster"
    verify_journal_layout(cluster, ack_and_claim, assignments, body_compression, dual_stream,
                          small_body_compression=small_body_compression)
    identity_path = cluster / ("journal-small-body-identity-v1.json" if small_body_compression
                               else "journal-identity-v1.json")
    if (not identity_path.is_file() or identity_path.is_symlink()
            or identity_path.stat().st_mode & 0o077
            or os.path.lexists(cluster / "peer-activation-v1.json")):
        raise BenchmarkError("local StrangeQ requires a private identity and no peer activation")
    other_identity = "journal-identity-v1.json" if small_body_compression else "journal-small-body-identity-v1.json"
    if os.path.lexists(cluster / other_identity):
        raise BenchmarkError("local StrangeQ journal identities coexist")
    identity = json.loads(identity_path.read_text())
    view = json.loads((cluster / "catalog-cache").read_text())
    if not isinstance(identity, dict) or not isinstance(view, dict):
        raise BenchmarkError("local StrangeQ catalog or identity is malformed")
    peer, shards = identity.get("Peer"), identity.get("Shards")
    node = peer.get("ID") if isinstance(peer, dict) else None
    if (identity.get("Local") is not True or type(node) is not int or not 0 < node < (1 << 63)
            or peer != {"ID": node, "Address": ""}
            or not isinstance(identity.get("Cluster"), str)
            or not re.fullmatch(r"[0-9a-f]{32}", identity["Cluster"])
            or identity.get("Seeds") != [peer] or identity.get("Nonce") != ""
            or type(shards) is not int or not 1 <= shards <= 1024
            or view.get("Cluster") != identity["Cluster"] or view.get("Shards") != shards
            or view.get("Nodes") != [peer] or view.get("Nonces") not in (None, {})
            or view.get("CatalogTarget") != [node]):
        raise BenchmarkError("local StrangeQ catalog or identity is not an unactivated singleton")
    if expected_shards is not None and shards != expected_shards:
        raise BenchmarkError(f"StrangeQ trial requires {expected_shards} shards but catalog has {shards}")
    if not isinstance(view.get("Catalog"), dict) or not isinstance(view.get("Placements"), list):
        raise BenchmarkError("local StrangeQ catalog membership is malformed")
    rows = [view["Catalog"], *view["Placements"]]
    if (len(rows) != shards + 1
            or any(not isinstance(row, dict) or type(row.get("Group")) is not int for row in rows)
            or {row["Group"] for row in rows} != {CATALOG_GROUP, *range(1, shards + 1)}
            or any(row.get("Voters") != [node] or row.get("Learners") for row in rows)
            or type(view["Catalog"].get("Index")) is not int or view["Catalog"]["Index"] < 0):
        raise BenchmarkError("local StrangeQ lacks sole catalog-committed voters")
    table = {row["Group"]: row for row in rows}
    pending = [] if view["Catalog"]["Index"] > 0 else ["catalog"]
    placements = {}
    for name in case.queue_names:
        group = int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big") % shards + 1
        if type(table[group].get("Index")) is not int or table[group]["Index"] < 0:
            raise BenchmarkError(f"{name} lacks a committed local membership report")
        verify_journal_group(cluster, group)
        if table[group]["Index"] == 0:
            pending.append(name)
        placements[name] = {"shard": group, "preferred_leader": node, "voters": [node],
                            "cluster_id": identity["Cluster"], "local": True}
    # Validate every workload group before classifying a zero report index as
    # transient: a later conflicting/retired/missing group must fail immediately.
    if pending:
        raise PendingMembershipReport(f"committed local membership reports pending for {', '.join(pending)}")
    return placements


def wait_for_strangeq_placement(context, case, *, timeout=10, **options):
    """Wait only before timing for valid singleton membership reports to commit."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return verify_strangeq_placement(context, case, **options)
        except PendingMembershipReport as error:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BenchmarkError(f"membership readiness timed out after {timeout}s: {error}") from error
            time.sleep(min(0.05, remaining))


def launch_rabbitmq(args, stack, scratch, output, env):
    count = TOPOLOGIES[args.topology]
    ports = free_ports(count * 2 + 1)
    amqp, distribution, epmd_port = ports[:count], ports[count:2 * count], ports[-1]
    otp_bin = args.erlang_bin
    cookie = secrets.token_hex(24)
    rack = secrets.token_hex(4)
    base_env = dict(env, PATH=f"{otp_bin}{os.pathsep}{env['PATH']}",
                    ERL_EPMD_PORT=str(epmd_port), ERL_EPMD_ADDRESS=LOOPBACK)
    epmd_log = output / "epmd.log"
    epmd = spawn(stack, [otp_bin / "epmd", "-port", str(epmd_port)], scratch, base_env, epmd_log)
    wait_until_ready(epmd, epmd_port, epmd_log)
    brokers, node_envs = [], []
    for i, (amqp_port, dist_port) in enumerate(zip(amqp, distribution), 1):
        path = scratch / f"node-{i}"
        path.mkdir()
        name = f"sqbench{rack}_{i}@{LOOPBACK}"
        config = path / "rabbitmq.conf"
        queue_config = ("default_queue_type = classic\n" if args.topology == "singleton" else
                        "quorum_queue.initial_cluster_size = 3\n"
                        f"quorum_queue.commands_soft_limit = {args.rabbit_window}\n")
        config.write_text(
            f"listeners.tcp.1 = {LOOPBACK}:{amqp_port}\n"
            f"distribution.listener.interface = {LOOPBACK}\n"
            "queue_leader_locator = balanced\n"
            + queue_config,
            encoding="ascii",
        )
        plugins = path / "enabled_plugins"
        plugins.write_text("[].\n", encoding="ascii")
        node_env = dict(
            base_env, RABBITMQ_NODENAME=name, RABBITMQ_USE_LONGNAME="true",
            RABBITMQ_CONFIG_FILE=str(config), RABBITMQ_CONF_ENV_FILE=str(path / "rabbitmq-env.conf"),
            RABBITMQ_CONFIG_FILES=str(path / "conf.d"),
            RABBITMQ_ADVANCED_CONFIG_FILE=str(path / "advanced.config"),
            RABBITMQ_MNESIA_BASE=str(path), RABBITMQ_MNESIA_DIR=str(path / "db"),
            RABBITMQ_LOG_BASE=str(path / "log"),
            RABBITMQ_PID_FILE=str(path / "pid"), RABBITMQ_ENABLED_PLUGINS_FILE=str(plugins),
            RABBITMQ_PLUGINS_EXPAND_DIR=str(path / "plugins"), RABBITMQ_DIST_PORT=str(dist_port),
            RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=f"-setcookie {cookie}",
            RABBITMQ_CTL_ERL_ARGS=f"-setcookie {cookie}", ERL_CRASH_DUMP=str(path / "erl_crash.dump"),
            RUNNING_UNDER_SYSTEMD="true", RABBITMQ_LOGS="-",
        )
        log = output / f"broker-{i}.log"
        broker = spawn(stack, [args.rabbitmq_home / "sbin" / "rabbitmq-server"], path, node_env, log)
        brokers.append(broker)
        node_envs.append(node_env)
    for i, (broker, port, node_env) in enumerate(zip(brokers, amqp, node_envs), 1):
        wait_until_ready(broker, port, output / f"broker-{i}.log")
        actual = rabbit_cmd(args, node_env, scratch, output / f"pid-{i}.log", "eval", "os:getpid().")
        if actual.strip().strip('"') != str(broker.pid):
            raise BenchmarkError("RabbitMQ foreground command did not exec the broker process")
        beam = rabbit_cmd(args, node_env, scratch, output / f"beam-{i}.log", "eval", f"{ERLANG_BEAM_PATH}.")
        if Path(beam.strip().strip('"')).resolve() != args.erlang_beam:
            raise BenchmarkError("RabbitMQ started a different Erlang runtime than the probe")
    seed = node_envs[0]["RABBITMQ_NODENAME"]
    for i, node_env in enumerate(node_envs[1:], 2):
        rabbit_cmd(args, node_env, scratch, output / f"join-{i}.log", "join_cluster", seed)
    return brokers, amqp, node_envs


def rabbit_cmd(args, env, scratch, log, *command):
    return run_logged([
        args.rabbitmq_home / "sbin" / "rabbitmqctl", "--quiet", "--longnames", "--node",
        env["RABBITMQ_NODENAME"], *command,
    ], scratch, env, log)


def verify_rabbit_placement(args, case, environments, scratch, output, phase):
    nodes = {env["RABBITMQ_NODENAME"] for env in environments}
    seed = environments[0]
    status = rabbit_cmd(args, seed, scratch, output / f"cluster-{phase}.log", "cluster_status")
    if args.topology == "singleton" and (len(environments) != 1 or len(re.findall(
            r"^([^\s:]+): RabbitMQ (\S+) on Erlang (\S+)$", status, re.MULTILINE)) != 1):
        raise BenchmarkError("RabbitMQ Classic comparison requires one reported broker")
    members = {
        match[0] for match in re.findall(r"^([^\s:]+): RabbitMQ (\S+) on Erlang (\S+)$", status, re.MULTILINE)
        if match[1:] == (RABBITMQ_VERSION, OTP_VERSION)
    }
    if nodes != members:
        raise BenchmarkError(f"RabbitMQ cluster lacks {TOPOLOGIES[args.topology]} nodes reporting the pinned RabbitMQ and OTP versions")
    fields = ("name", "type", "durable", "state") if args.topology == "singleton" else (
        "name", "type", "durable", "leader", "members", "online")
    queues = rabbit_cmd(args, seed, scratch, output / f"queues-{phase}.log", "list_queues",
                        "--no-table-headers", *fields)
    placement = parse_rabbit_queues(queues, case, nodes, topology=args.topology)
    leaders = Counter(value["leader"] for value in placement.values())
    if case.queues > 1 and max(leaders.values()) - min(leaders.get(node, 0) for node in nodes) > 1:
        raise BenchmarkError("RabbitMQ quorum queue leaders are not balanced across the three brokers")
    return placement


def perftest_args(args, case, ports, rabbitmq, prepare=False):
    uris = ",".join(f"amqp://guest:guest@{LOOPBACK}:{port}/%2f" for port in ports)
    command = [
        *args.perftest, "--uris", uris, "--producers", str(0 if prepare else case.clients),
        "--consumers", str(case.clients), "--size", str(case.size), "--time", str(2 if prepare else args.duration),
        "--flag", "persistent", "--flag", "mandatory", "--auto-delete", "false", "--confirm", str(args.window),
        "--qos", str(args.prefetch), "--interval", "1", "--type", "direct", "--exchange", EXCHANGE,
        "--connection-allocation", "round-robin", "--disable-connection-recovery",
    ]
    if case.queues == 1:
        command.extend(["--queue", case.queue_names[0]])
    else:
        command.extend(["--queue-pattern", f"{case.name}-%d", "--queue-pattern-from", "1",
                        "--queue-pattern-to", str(case.queues)])
    if rabbitmq and args.topology == "quorum3":
        command.append("--quorum-queue")
    if args.payload_profile == "high-entropy":
        command.extend(["--body", args.payloads[case.name]["path"]])
    if args.rate_per_producer is not None and not prepare:
        command.extend(["--rate", str(args.rate_per_producer)])
    return command


def read_rss(brokers):
    pids = [broker.pid for broker in brokers]
    snap = subprocess.run(["ps", "-o", "pid=", "-o", "rss=", "-p", ",".join(map(str, pids))],
                          capture_output=True, text=True, timeout=2)
    values = {int(pid): int(kib) * 1024 for pid, kib in (line.split() for line in snap.stdout.splitlines())}
    if snap.returncode or set(values) != set(pids):
        raise BenchmarkError("a broker process exited or did not provide an RSS sample")
    return [values[pid] for pid in pids]


def linux_storage_context(scratch, *, sys_devices=Path("/sys/dev/block")):
    device = scratch.stat().st_dev
    number = f"{os.major(device)}:{os.minor(device)}"
    stat = sys_devices / number / "stat"
    return {
        "scratch_device_major_minor": number,
        "scratch_device_stat_path": str(stat) if stat.is_file() else None,
        "scratch_device_scope": "whole scratch backing block device; includes unrelated host I/O",
        "scratch_device_unavailable_reason": None if stat.is_file() else "no Linux block-device stat for scratch filesystem",
        "host_cache_scope": "host-wide /proc/meminfo Dirty and Writeback; excludes clean page cache; not broker RSS",
        "sampling_interval_seconds": 1,
    }


def read_linux_storage(context, *, meminfo=Path("/proc/meminfo")):
    values = {}
    for line in meminfo.read_text(encoding="ascii").splitlines():
        key, _, value = line.partition(":")
        if key in ("Dirty", "Writeback"):
            fields = value.split()
            if len(fields) != 2 or fields[1] != "kB":
                raise BenchmarkError(f"unexpected Linux {key} memory counter")
            values[f"host_cache_{key.lower()}_bytes"] = int(fields[0]) * 1024
    if len(values) != 2:
        raise BenchmarkError("Linux memory evidence requires both Dirty and Writeback counters")
    if context["scratch_device_stat_path"] is not None:
        fields = Path(context["scratch_device_stat_path"]).read_text(encoding="ascii").split()
        if len(fields) < 7:
            raise BenchmarkError("incomplete Linux scratch-device stat")
        # Linux block stat sectors are always 512 bytes, independent of the
        # device's logical/physical sector size. These are device-wide counters.
        values["scratch_device_read_bytes"] = int(fields[2]) * 512
        values["scratch_device_written_bytes"] = int(fields[6]) * 512
    return values


def measure(args, stack, scratch, output, env, brokers, command):
    log = output / "perftest.log"
    start = time.monotonic()
    storage_context = linux_storage_context(scratch) if platform.system() == "Linux" else None
    storage_samples = [(0, read_linux_storage(storage_context))] if storage_context else []
    client = spawn(stack, command, scratch, env, log)
    samples = []
    deadline = start + args.duration + 50
    while client.poll() is None:
        now = time.monotonic()
        if now >= deadline:
            raise BenchmarkError(f"PerfTest exceeded its wall-clock limit; see {log}")
        samples.append((now - start, read_rss(brokers)))
        if storage_context and now - start >= storage_samples[-1][0] + 1:
            storage_samples.append((now - start, read_linux_storage(storage_context)))
        try:
            client.wait(timeout=0.25)
        except subprocess.TimeoutExpired:
            pass
    write_json(output / "rss-samples.json", samples)
    if storage_context:
        storage_samples.append((time.monotonic() - start, read_linux_storage(storage_context)))
        write_json(output / "storage-samples.json", {"context": storage_context, "samples": storage_samples})
    if client.returncode:
        raise BenchmarkError(f"PerfTest exited {client.returncode}; see {log}")
    metrics = parse_perf_test(log.read_text(encoding="utf-8", errors="replace"), args.duration, args.warmup)
    storage_metrics = {}
    if storage_context:
        storage_metrics = {
            "peak_host_cache_dirty_bytes": max(row["host_cache_dirty_bytes"] for _, row in storage_samples),
            "peak_host_cache_writeback_bytes": max(row["host_cache_writeback_bytes"] for _, row in storage_samples),
            "storage_samples": len(storage_samples), "storage_context": storage_context,
        }
        for counter in ("scratch_device_read_bytes", "scratch_device_written_bytes"):
            if counter in storage_samples[0][1]:
                delta = storage_samples[-1][1][counter] - storage_samples[0][1][counter]
                if delta < 0:
                    raise BenchmarkError(f"Linux {counter} decreased during the timed trial")
                storage_metrics[f"{counter}_delta"] = delta
    return dict(metrics, **storage_metrics, peak_cluster_rss_bytes=peak_rss(samples, TOPOLOGIES[args.topology]),
                rss_samples=len(samples), topology=args.topology, broker_processes=len(brokers),
                strangeq_durability=args.strangeq_durability,
                rss_scope=f"simultaneous sum of {len(brokers)} broker processes; excludes client and EPMD")


def trial(args, case, rabbitmq, output, env):
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix="sqbench-", dir=args.scratch) as work, ExitStack() as stack:
        scratch = Path(work)
        if rabbitmq:
            brokers, ports, placement_context = launch_rabbitmq(args, stack, scratch, output, env)
        else:
            brokers, ports, placement_context = launch_strangeq(args, stack, scratch, output, env)
        prepare = perftest_args(args, case, ports, rabbitmq, prepare=True)
        prepared = run_logged(prepare, scratch, env, output / "prepare.log")
        if CLIENT_ERROR.search(prepared) or "test stopped (Reached time limit)" not in prepared:
            raise BenchmarkError(f"PerfTest topology setup failed; see {output / 'prepare.log'}")
        if rabbitmq:
            before = verify_rabbit_placement(args, case, placement_context, scratch, output, "before")
        else:
            before = wait_for_strangeq_placement(
                placement_context, case, ack_and_claim=args.strangeq_ack_and_claim,
                assignments=args.strangeq_assignments, body_compression=args.strangeq_journal_body_compression,
                dual_stream=args.strangeq_dual_stream,
                small_body_compression=args.strangeq_journal_small_body_compression,
                expected_shards=args.strangeq_shards, topology=args.topology,
            )
        write_json(output / "placement-before.json", before)
        if args.validate_only:
            return None
        command = perftest_args(args, case, ports, rabbitmq)
        write_json(output / "command.json", [os.fspath(arg) for arg in command])
        result = measure(args, stack, scratch, output, env, brokers, command)
        if rabbitmq:
            after = verify_rabbit_placement(args, case, placement_context, scratch, output, "after")
        else:
            after = verify_strangeq_placement(
                placement_context, case, ack_and_claim=args.strangeq_ack_and_claim,
                assignments=args.strangeq_assignments, body_compression=args.strangeq_journal_body_compression,
                dual_stream=args.strangeq_dual_stream,
                small_body_compression=args.strangeq_journal_small_body_compression,
                expected_shards=args.strangeq_shards, topology=args.topology,
            )
        write_json(output / "placement-after.json", after)
        if before != after:
            raise BenchmarkError("queue placement changed in the timed window")
    write_json(output / "result.json", result)
    return result


def stats(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def summarize(rows):
    result = {}
    for case in rows:
        result[case] = {}
        for engine, runs in rows[case].items():
            result[case][engine] = {
                "sent_per_second": stats([run["rates"]["sent"] for run in runs]),
                "confirms_per_second": stats([run["rates"]["confirmed"] for run in runs]),
                "deliveries_per_second": stats([run["rates"]["received"] for run in runs]),
                "peak_cluster_rss_bytes": stats([run["peak_cluster_rss_bytes"] for run in runs]),
                "confirm_p99_us": stats([run["p99_us"]["confirm"] for run in runs]),
                "consumer_p99_us": stats([run["p99_us"]["consumer"] for run in runs]),
            }
    return result


def executable(path):
    if not path.is_file() or not os.access(path, os.X_OK):
        raise BenchmarkError(f"missing executable: {path}")
    return path


def file_digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def probe_erlang(args, output, env):
    expression = (
        'Release = erlang:system_info(otp_release), '
        '{ok, Version} = file:read_file(filename:join([code:root_dir(), "releases", Release, "OTP_VERSION"])), '
        f'io:format("~s~n~s~n", [string:trim(binary_to_list(Version)), {ERLANG_BEAM_PATH}]), halt().'
    )
    probe_env = dict(env, PATH=f"{args.erlang_bin}{os.pathsep}{env['PATH']}")
    result = run_logged([args.erlang_bin / "erl", "+S", "1:1", "-noshell", "-noinput", "-eval", expression],
                        output, probe_env, output / "erlang-version.log")
    lines = result.splitlines()
    if len(lines) != 2 or lines[0] != OTP_VERSION:
        raise BenchmarkError(f"Erlang must report OTP {OTP_VERSION}; see {output / 'erlang-version.log'}")
    args.erlang_beam = executable(Path(lines[1]).resolve())
    return {"otp_version": lines[0], "erlang_beam": str(args.erlang_beam),
            "erlang_beam_sha256": file_digest(args.erlang_beam)}


def darwin_erlang_flush(args, output, env):
    otool = shutil.which("otool")
    if not otool or platform.machine() != "arm64":
        return "unknown"
    try:
        disassembly = run_logged([otool, "-tV", "-p", "_efile_sync", args.erlang_beam],
                                 output, env, output / "erlang-efile-sync.log", timeout=15)
    except BenchmarkError:
        return "unknown"
    _, _, function = disassembly.partition("\n_efile_sync:\n")
    function = function.split("\n_", 1)[0]
    call = re.search(r"\bmov\s+w1, #(0x55|0x33)\s*\n[0-9a-f]+\s+bl\s+[^\n]+: _fcntl\b", function)
    if not call:
        return "unknown"
    return {"0x55": "F_BARRIERFSYNC (85)", "0x33": "F_FULLFSYNC (51)"}[call[1]]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topology", choices=TOPOLOGIES, default="quorum3",
                        help="quorum3: three StrangeQ voters vs RabbitMQ quorum; singleton: local journal vs Classic")
    parser.add_argument("--payload-profile", choices=("perftest", "high-entropy"), default="perftest",
                        help="perftest: built-in time/sequence bodies; high-entropy: one reused deterministic binary per case")
    parser.add_argument("--strangeq-bin", type=Path, required=True)
    parser.add_argument("--strangeq-durability", choices=("barrier", "flush", "process"), default="barrier",
                        help="storage durability profile; process requires singleton and protects process crashes only")
    parser.add_argument("--strangeq-range-sync", action="store_true", help="enable full synchronous journal range writes where supported")
    parser.add_argument("--strangeq-dual-stream", action="store_true", help="use two journal streams on fresh StrangeQ brokers")
    parser.add_argument("--strangeq-ack-and-claim", action="store_true", help="enable combined ACK and refill on every fresh StrangeQ journal replica")
    parser.add_argument("--strangeq-assignments", action="store_true", help="enable committed consumer assignments on every fresh StrangeQ journal replica")
    parser.add_argument("--strangeq-replica-bodies", action="store_true", help="enable replica-local assignment bodies on the StrangeQ brokers")
    parser.add_argument("--strangeq-journal-body-compression", action="store_true", help="enable lossless journal body compression on fresh StrangeQ brokers")
    parser.add_argument("--strangeq-journal-small-body-compression", action="store_true",
                        help="also allow lossless packing of bodies from 1 KiB on fresh StrangeQ brokers")
    parser.add_argument("--strangeq-shards", type=int, help="configure shard count on fresh StrangeQ brokers")
    parser.add_argument("--rabbitmq-home", type=Path, required=True, help=f"extracted RabbitMQ {RABBITMQ_VERSION} release")
    parser.add_argument("--erlang-bin", type=Path, required=True, help=f"bin directory of Erlang/OTP {OTP_VERSION}")
    parser.add_argument("--perftest-jar", type=Path, required=True, help=f"RabbitMQ PerfTest {PERF_TEST_VERSION} shaded JAR")
    parser.add_argument("--java", default="java", help="Java executable name or path")
    parser.add_argument("--output", type=Path, required=True, help="directory in which to save an isolated result set")
    parser.add_argument("--scratch", type=Path, required=True, help="existing equivalent storage for both brokers")
    parser.add_argument("--case", choices=CASES, action="append", help="workload to repeat, default is all three")
    parser.add_argument("--duration", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--window", type=int, default=64)
    parser.add_argument("--prefetch", type=int, default=100)
    parser.add_argument("--rabbit-window", type=int, default=32)
    parser.add_argument("--rate-per-producer", type=int, help="matched offered load rather than saturation")
    parser.add_argument("--validate-only", action="store_true", help="verify topology without starting any publishers")
    parser.add_argument("--darwin-diagnostic", action="store_true", help="allow native macOS diagnostic runs with possibly different flush primitives")
    args = parser.parse_args()
    if args.strangeq_durability == "process" and args.topology != "singleton":
        parser.error("strangeq-durability process requires --topology singleton")
    if args.warmup < 0 or args.duration < args.warmup + 4:
        parser.error("the measured window must contain at least four complete seconds")
    if args.strangeq_shards is not None and not 1 <= args.strangeq_shards <= 1024:
        parser.error("strangeq-shards requires a value from 1 to 1024")
    if min(args.repeats, args.window, args.prefetch, args.rabbit_window) <= 0:
        parser.error("repeats, window, prefetch and rabbit-window must be positive")
    if args.rate_per_producer is not None and args.rate_per_producer <= 0:
        parser.error("rate-per-producer must be positive")
    if args.strangeq_dual_stream and not args.strangeq_range_sync:
        parser.error("strangeq-dual-stream requires strangeq-range-sync")
    if args.strangeq_assignments and args.strangeq_ack_and_claim:
        parser.error("strangeq-assignments requires its own journal mode")
    if args.strangeq_replica_bodies and not args.strangeq_assignments:
        parser.error("strangeq-replica-bodies requires strangeq-assignments")
    if args.strangeq_journal_body_compression and not args.strangeq_assignments:
        parser.error("strangeq-journal-body-compression requires strangeq-assignments")
    if args.strangeq_journal_small_body_compression and not args.strangeq_journal_body_compression:
        parser.error("strangeq-journal-small-body-compression requires strangeq-journal-body-compression")
    if args.case and len(args.case) != len(set(args.case)):
        parser.error("each workload can be selected only once")
    return args


def main():
    args = arguments()
    system = platform.system()
    if system != "Linux" and not (system == "Darwin" and args.darwin_diagnostic):
        raise BenchmarkError("run both brokers in the same Linux environment, or explicitly select macOS diagnostics")
    if not args.scratch.is_dir():
        raise BenchmarkError("--scratch must name an existing directory on the volume under test")
    for field in ("strangeq_bin", "rabbitmq_home", "erlang_bin", "perftest_jar", "scratch", "output"):
        setattr(args, field, getattr(args, field).resolve())
    executable(args.strangeq_bin)
    for program in ("rabbitmq-server", "rabbitmqctl"):
        executable(args.rabbitmq_home / "sbin" / program)
    for program in ("erl", "epmd"):
        executable(args.erlang_bin / program)
    java = shutil.which(args.java)
    if not java or not args.perftest_jar.is_file():
        raise BenchmarkError("Java and the pinned PerfTest JAR are required")
    args.perftest = [Path(java).resolve(), "-XX:ActiveProcessorCount=4", "-Duser.language=en", "-Duser.country=US",
                    "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8", "-jar", args.perftest_jar]
    # Strip unrelated tuning and broker-specific environment overrides from the shell.
    env = {key: os.environ[key] for key in ("HOME", "USER", "LOGNAME") if key in os.environ}
    locale = "C.UTF-8" if system == "Linux" else "C"
    env.update(PATH=os.defpath, LC_ALL=locale, LANG=locale)
    args.output.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="cluster-compare-", dir=args.output))
    print(f"Evidence directory {output}", flush=True)
    cases = [CASES[name] for name in (args.case or CASES)]
    args.payloads = prepare_payloads(args.payload_profile, cases, output)
    version = run_logged([*args.perftest, "--version"], output, env, output / "perftest-version.log")
    if not re.search(rf"\b{re.escape(PERF_TEST_VERSION)}\b", version):
        raise BenchmarkError(f"PerfTest must report version {PERF_TEST_VERSION}")
    otp = probe_erlang(args, output, env)
    if system == "Darwin":
        otp["erlang_efile_sync_static"] = darwin_erlang_flush(args, output, env)
    write_json(output / "environment.json", {
        "platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count(),
        "scratch": str(args.scratch), "locale": locale,
        "benchmark_mode": "same-host Linux" if system == "Linux" else "Darwin diagnostic only",
        "payload_profile": args.payload_profile, "payloads": args.payloads,
        "topology": args.topology, "broker_processes_per_engine": TOPOLOGIES[args.topology],
        "rabbitmq_queue_type": "classic" if args.topology == "singleton" else "quorum",
        "strangeq_peer_transport": args.topology != "singleton",
        "rss_scope": f"simultaneous sum of {TOPOLOGIES[args.topology]} broker processes per engine; excludes client and EPMD",
        "strangeq_sha256": file_digest(args.strangeq_bin), "strangeq_binary": str(args.strangeq_bin),
        "strangeq_storage": "segmented_journal",
        "strangeq_durability": args.strangeq_durability,
        "strangeq_range_sync": args.strangeq_range_sync,
        "strangeq_dual_stream": args.strangeq_dual_stream,
        "strangeq_ack_and_claim": args.strangeq_ack_and_claim,
        "strangeq_assignments": args.strangeq_assignments,
        "strangeq_replica_bodies": args.strangeq_replica_bodies,
        "strangeq_journal_body_compression": args.strangeq_journal_body_compression,
        "strangeq_journal_small_body_compression": args.strangeq_journal_small_body_compression,
        "strangeq_shards": args.strangeq_shards,
        "rabbitmq_home": str(args.rabbitmq_home), "rabbitmq_version": RABBITMQ_VERSION,
        "erlang_bin": str(args.erlang_bin), **otp,
        "perftest_sha256": file_digest(args.perftest_jar), "perftest_reported_version": version.strip(),
        "java": java, "window": args.window, "prefetch": args.prefetch, "rabbit_window": args.rabbit_window,
        "duration": args.duration, "warmup": args.warmup, "repeats": args.repeats,
        "rate_per_producer": args.rate_per_producer, "validate_only": args.validate_only,
    })
    rows = {case.name: {"strangeq": [], "rabbitmq": []} for case in cases}
    rejected = []
    for repeat in range(1, args.repeats + 1):
        for case_index, case in enumerate(cases):
            engines = ("strangeq", "rabbitmq") if (repeat + case_index) % 2 else ("rabbitmq", "strangeq")
            for engine in engines:
                name = f"{repeat:02d}-{case.name}-{engine}"
                print(f"Running {name}", flush=True)
                try:
                    result = trial(args, case, engine == "rabbitmq", output / name, env)
                except RejectedWorkload as error:
                    rejected.append({"trial": name, "error": str(error)})
                    write_json(output / "failures.json", rejected)
                    print(f"Rejected {name} ({error})", flush=True)
                    continue
                rows[case.name][engine].append(result)
    if rejected:
        raise BenchmarkError(f"{len(rejected)} rejected workload trials. Evidence directory {output}")
    if args.validate_only:
        print(f"Topology verified. Evidence directory {output}", flush=True)
        return
    write_json(output / "summary.json", summarize(rows))
    print(f"Completed {output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        main()
    except (BenchmarkError, KeyboardInterrupt, OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
        print(f"Cluster comparison stopped: {error or type(error).__name__}", file=sys.stderr)
        sys.exit(1)
