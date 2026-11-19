import copy
import hashlib
import io
import json
import sys
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import cluster_compare
from cluster_compare import (
    CASES,
    BenchmarkError,
    parse_perf_test,
    parse_rabbit_queues,
    peak_rss,
    summarize,
    verify_strangeq_placement,
)


def perf_output(*, nacks=0, returns=0, stop="Reached time limit", omit_last=False):
    samples = [
        f"id: test, time {second}.004 s, sent: {second * 100} msg/s, "
        f"returned: {returns} msg/s, confirmed: {second * 90} msg/s, "
        f"nacked: {nacks} msg/s, received: {second * 85} msg/s"
        for second in range(1, 6 if omit_last else 7)
    ]
    return "\n".join(samples + [
        f"test stopped ({stop})",
        "id: test, consumer latency min/median/75th/95th/99th/max 1/2/3/4/5/6 µs",
        "id: test, confirm latency min/median/75th/95th/99th/max 2/3/4/5/6/7 µs",
    ])


class PairedWorkloadTests(unittest.TestCase):
    def test_rejected_workload_still_collects_counterpart_without_valid_summary(self):
        self.check_rejected_pair("quorum3")

    def test_singleton_metadata_and_rejected_pair_use_the_same_trial_loop(self):
        self.check_rejected_pair("singleton")

    def test_high_entropy_metadata_records_selected_fixture_for_both_engines(self):
        self.check_rejected_pair("singleton", payload_profile="high-entropy")

    def check_rejected_pair(self, topology, payload_profile="perftest"):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            jar = root / "perftest.jar"
            jar.touch()
            report = root / "results"
            args = [
                "cluster_compare.py", "--strangeq-bin", str(root / "broker"),
                "--rabbitmq-home", str(root), "--erlang-bin", str(root),
                "--perftest-jar", str(jar), "--scratch", str(root), "--output", str(report),
                "--case", "1024-1x1-1q", "--duration", "6", "--warmup", "2", "--repeats", "1",
                "--topology", topology,
                "--payload-profile", payload_profile,
            ]
            observed = []

            def trial(args, case, rabbitmq, evidence, env):
                observed.append(rabbitmq)
                evidence.mkdir()
                metrics = parse_perf_test(perf_output(nacks=0 if rabbitmq else 1), args.duration, args.warmup)
                return dict(metrics, peak_cluster_rss_bytes=1)

            with (
                mock.patch.object(sys, "argv", args),
                mock.patch("cluster_compare.platform.system", return_value="Linux"),
                mock.patch("cluster_compare.shutil.which", return_value="/synthetic/java"),
                mock.patch.multiple(cluster_compare, executable=mock.DEFAULT, run_logged=mock.DEFAULT,
                                    probe_erlang=mock.DEFAULT, file_digest=mock.DEFAULT) as fakes,
                mock.patch.object(cluster_compare, "trial", side_effect=trial),
            ):
                fakes["run_logged"].return_value = cluster_compare.PERF_TEST_VERSION
                fakes["probe_erlang"].return_value = {}
                fakes["file_digest"].return_value = "fixture"
                with self.assertRaises(BenchmarkError):
                    cluster_compare.main()
            self.assertEqual(observed, [False, True], "a negative confirm must still record the paired broker")
            evidence = next(report.iterdir())
            environment = json.loads((evidence / "environment.json").read_text())
            self.assertEqual(environment["strangeq_storage"], "segmented_journal")
            self.assertEqual(environment["payload_profile"], payload_profile)
            self.assertEqual(set(environment["payloads"]), {"1024-1x1-1q"})
            payload = environment["payloads"]["1024-1x1-1q"]
            self.assertEqual(payload["size_bytes"], 1024)
            if payload_profile == "high-entropy":
                body = Path(payload["path"]).read_bytes()
                self.assertEqual(len(body), 1024)
                self.assertEqual(payload["sha256"], hashlib.sha256(body).hexdigest())
            else:
                self.assertIsNone(payload["sha256"])
                self.assertIsNone(payload["path"])
            self.assertEqual(environment["topology"], topology)
            self.assertEqual(environment["broker_processes_per_engine"], 1 if topology == "singleton" else 3)
            self.assertEqual(environment["rabbitmq_queue_type"], "classic" if topology == "singleton" else "quorum")
            self.assertEqual(environment["strangeq_peer_transport"], topology != "singleton")
            for option in ("range_sync", "dual_stream", "ack_and_claim", "assignments", "replica_bodies",
                           "journal_body_compression", "journal_small_body_compression"):
                self.assertIs(environment[f"strangeq_{option}"], False)
            self.assertFalse((evidence / "summary.json").exists())
            failures = json.loads((evidence / "failures.json").read_text())
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["trial"], "01-1024-1x1-1q-strangeq")
            self.assertIn("negative confirm", failures[0]["error"])


class JournalDefaultTests(unittest.TestCase):
    def arguments(self, root, *options):
        argv = ["cluster_compare.py", "--strangeq-bin", str(root / "broker"),
                "--rabbitmq-home", str(root), "--erlang-bin", str(root),
                "--perftest-jar", str(root / "perftest.jar"), "--scratch", str(root),
                "--output", str(root / "results"), *options]
        with mock.patch.object(sys, "argv", argv):
            return cluster_compare.arguments()

    def test_launch_uses_default_journal_without_retired_config_setting(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            args = self.arguments(root, "--strangeq-range-sync", "--strangeq-assignments")
            with mock.patch.multiple(cluster_compare, run_logged=mock.DEFAULT, spawn=mock.DEFAULT,
                                     wait_until_ready=mock.DEFAULT, free_ports=mock.DEFAULT) as fakes:
                fakes["free_ports"].return_value = list(range(16000, 16009))
                cluster_compare.launch_strangeq(args, mock.Mock(), root, root, {})
            for number in range(1, 4):
                config = json.loads((root / f"node-{number}.yaml").read_text())["cluster"]
                self.assertNotIn("experimental_raft_journal", config)
                self.assertIs(config["experimental_raft_range_sync"], True)
                self.assertIs(config["experimental_assignments"], True)

    def test_ack_and_claim_does_not_require_a_backend_selector(self):
        self.assertTrue(self.arguments(Path("/fixture"), "--strangeq-ack-and-claim").strangeq_ack_and_claim)

    def test_payload_profile_defaults_preserve_perftest_bodies_without_files(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            args = self.arguments(root)
            self.assertEqual(args.payload_profile, "perftest")
            cases = [CASES["1024-1x1-1q"]]
            args.payloads = cluster_compare.prepare_payloads(args.payload_profile, cases, root)
            args.perftest = ["java", "perftest.jar"]
            self.assertEqual(list(root.iterdir()), [])
            command = cluster_compare.perftest_args(args, cases[0], [16000], False)
            self.assertNotIn("--body", command)
            self.assertEqual(command[command.index("--size") + 1], "1024")
            self.assertIsNone(args.payloads[cases[0].name]["sha256"])

    def test_high_entropy_selection_is_deterministic_and_reused_across_trials(self):
        expected = {"1024-1x1-1q": "839acc6bfc1d5ee5b70027f5d8227ed4bf5e23419fed311fff31e97a2e2c7635",
                    "65536-10x10-10q": "f779634882a1b1c058d0d4467b87c6f427178b5a7e527a5cad793824c4cf7dc5"}
        cases = [CASES[name] for name in expected]
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            args = self.arguments(root, "--payload-profile", "high-entropy")
            args.payloads = cluster_compare.prepare_payloads(args.payload_profile, cases, root)
            args.perftest = ["java", "perftest.jar"]
            self.assertEqual({path.name for path in root.iterdir()}, {f"payload-{case.name}.bin" for case in cases})
            self.assertEqual(set(args.payloads), set(expected))
            for case in cases:
                payload = args.payloads[case.name]
                body = Path(payload["path"]).read_bytes()
                self.assertEqual(len(body), case.size)
                self.assertEqual(payload["size_bytes"], case.size)
                self.assertEqual(payload["sha256"], expected[case.name])
                self.assertEqual(hashlib.sha256(body).hexdigest(), expected[case.name])
                for _ in range(2):
                    for rabbitmq in (False, True):
                        for prepare in (False, True):
                            command = cluster_compare.perftest_args(args, case, [16000], rabbitmq, prepare)
                            self.assertEqual(command[command.index("--body") + 1], payload["path"])
                self.assertEqual(Path(payload["path"]).read_bytes(), body)
            other = root / "another-result"
            other.mkdir()
            replay = cluster_compare.prepare_payloads("high-entropy", cases, other)
            self.assertEqual({name: row["sha256"] for name, row in replay.items()}, expected)

    def test_singleton_launch_opens_local_journal_without_provisioning_or_peer_transport(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            args = self.arguments(root, "--topology", "singleton", "--strangeq-shards", "4")
            with mock.patch.multiple(cluster_compare, run_logged=mock.DEFAULT, spawn=mock.DEFAULT,
                                     wait_until_ready=mock.DEFAULT, free_ports=mock.DEFAULT) as fakes:
                fakes["free_ports"].return_value = [16000, 16001]
                brokers, ports, context = cluster_compare.launch_strangeq(args, mock.Mock(), root, root, {})
            config = json.loads((root / "node-1.yaml").read_text())
            self.assertEqual(config["cluster"], {"local": True, "shards": 4})
            self.assertEqual(len(brokers), 1)
            self.assertEqual(ports, [16000])
            self.assertEqual(context, ([root / "node-1"], None))
            self.assertFalse((root / "peer.token").exists())
            fakes["run_logged"].assert_not_called()
            fakes["free_ports"].assert_called_once_with(2)
            fakes["spawn"].assert_called_once()

    def test_singleton_rabbitmq_launch_has_one_classic_broker_and_no_join(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            args = self.arguments(root, "--topology", "singleton")
            args.erlang_beam = (root / "beam.smp").resolve()
            broker = mock.Mock(pid=42)
            with mock.patch.multiple(cluster_compare, rabbit_cmd=mock.DEFAULT, spawn=mock.DEFAULT,
                                     wait_until_ready=mock.DEFAULT, free_ports=mock.DEFAULT) as fakes:
                fakes["free_ports"].return_value = [16000, 16001, 16002]
                fakes["spawn"].return_value = broker
                fakes["rabbit_cmd"].side_effect = ['"42"', f'"{args.erlang_beam}"']
                brokers, ports, environments = cluster_compare.launch_rabbitmq(args, mock.Mock(), root, root, {"PATH": "/bin"})
            self.assertEqual((brokers, ports, len(environments)), ([broker], [16000], 1))
            self.assertIn("default_queue_type = classic", (root / "node-1/rabbitmq.conf").read_text())
            self.assertNotIn("quorum_queue", (root / "node-1/rabbitmq.conf").read_text())
            self.assertEqual(fakes["rabbit_cmd"].call_count, 2, "singleton must not execute join_cluster")

    def test_workload_and_caps_are_identical_between_engines_in_each_topology(self):
        for topology in ("quorum3", "singleton"):
            args = self.arguments(Path("/fixture"), "--topology", topology,
                                  "--rate-per-producer", "500", "--window", "80", "--prefetch", "120")
            args.perftest = ["java", "perftest.jar"]
            for case in CASES.values():
                for prepare in (False, True):
                    with self.subTest(topology=topology, case=case.name, prepare=prepare):
                        ports = [16000] if topology == "singleton" else [16000, 16001, 16002]
                        sq = cluster_compare.perftest_args(args, case, ports, False, prepare)
                        rabbit = cluster_compare.perftest_args(args, case, ports, True, prepare)
                        self.assertEqual([arg for arg in rabbit if arg != "--quorum-queue"], sq)
                        self.assertEqual("--quorum-queue" in rabbit, topology == "quorum3")
                        self.assertEqual("--rate" in sq, not prepare)
                        self.assertEqual(sq[sq.index("--confirm") + 1], "80")
                        self.assertEqual(sq[sq.index("--qos") + 1], "120")

    def test_retired_selector_and_incompatible_active_options_are_rejected(self):
        for options in (("--strangeq-journal",), ("--strangeq-dual-stream",),
                        ("--strangeq-assignments", "--strangeq-ack-and-claim"),
                        ("--strangeq-replica-bodies",), ("--strangeq-journal-body-compression",),
                        ("--strangeq-journal-small-body-compression",)):
            with self.subTest(options=options), mock.patch("sys.stderr", new_callable=io.StringIO), \
                    self.assertRaises(SystemExit) as raised:
                self.arguments(Path("/fixture"), *options)
            self.assertEqual(raised.exception.code, 2)


class PerfTestOutputTests(unittest.TestCase):
    def test_rates_use_only_complete_steady_intervals(self):
        result = parse_perf_test(perf_output(), duration=6, warmup=2)
        self.assertEqual(result["rates"], {"sent": 450, "confirmed": 405, "received": 382.5})
        self.assertEqual(result["p99_us"], {"consumer": 5, "confirm": 6})
        self.assertEqual(result["missing_warmup_intervals"], 0)

    def test_accepts_missing_warmup_and_records_it(self):
        first, _, remaining = perf_output().partition("\n")
        result = parse_perf_test(remaining, duration=6, warmup=2)
        self.assertEqual(result["missing_warmup_intervals"], 1)
        self.assertEqual(result["steady_intervals"], 4)
        self.assertEqual(result["rates"], {"sent": 450, "confirmed": 405, "received": 382.5})
        with self.assertRaisesRegex(BenchmarkError, "interval"):
            parse_perf_test(remaining, duration=6, warmup=0)
        for field in ("nacked", "returned"):
            with self.subTest(field=field), self.assertRaisesRegex(BenchmarkError, "negative confirm or mandatory return"):
                bad_warmup = first.replace(f"{field}: 0", f"{field}: 0.1")
                parse_perf_test(f"{bad_warmup}\n{remaining}", duration=6, warmup=2)

    def test_parses_low_rates_printed_with_fractional_precision(self):
        output = perf_output().replace("received: 510 msg/s", "received: 9.9 msg/s")
        result = parse_perf_test(output, duration=6, warmup=2)
        self.assertAlmostEqual(result["rates"]["received"], 257.475)

    def test_parses_complete_final_sample_and_latency_trailer(self):
        output = perf_output().replace(
            "id: test, time 6.004 s, sent: 600 msg/s, returned: 0 msg/s, confirmed: 540 msg/s, "
            "nacked: 0 msg/s, received: 510 msg/s",
            "id: test-155630-423, time 6.006 s, sent: 33890 msg/s, returned: 0 msg/s, "
            "confirmed: 33890 msg/s, nacked: 0 msg/s, received: 33890 msg/s, "
            "min/median/75th/95th/99th/max consumer latency: 10187/18294/21336/26367/28815/34297 µs, "
            "confirm latency: 10387/17841/20998/26022/29617/34173 µs",
        )
        result = parse_perf_test(output, duration=6, warmup=2)
        self.assertEqual(result["rates"]["received"], 8727.5)
        self.assertEqual(result["steady_intervals"], 4)

    def test_refuses_rejected_and_incomplete_workloads(self):
        for output in (
            perf_output(nacks=1),
            perf_output(nacks=0.1),
            perf_output(returns=1),
            perf_output(stop="Connection failed"),
            perf_output(omit_last=True),
            perf_output() + "\njava.io.IOException: connection closed",
        ):
            with self.subTest(output=output), self.assertRaises(BenchmarkError):
                parse_perf_test(output, duration=6, warmup=2)

    def test_rss_peak_uses_same_time_samples(self):
        samples = [(0.1, [99, 1, 1]), (0.5, [1, 99, 1]), (0.9, [2, 3, 99])]
        self.assertEqual(peak_rss(samples), 104)
        with self.assertRaises(BenchmarkError):
            peak_rss([(0.1, [99, 1])])
        self.assertEqual(peak_rss([(0.1, [99]), (0.5, [103])], broker_count=1), 103)
        for bad_samples in ([], [(0.1, [0])], [(0.1, [99, 1, 1])], [(0.1, [99]), (0.2, [])]):
            with self.subTest(samples=bad_samples), self.assertRaises(BenchmarkError):
                peak_rss(bad_samples, broker_count=1)

    def test_summary_shows_actual_steady_offer_alongside_confirm_and_delivery(self):
        first = {"rates": {"sent": 120, "confirmed": 105, "received": 85},
                 "peak_cluster_rss_bytes": 512, "p99_us": {"confirm": 2, "consumer": 3}}
        second = {"rates": {"sent": 140, "confirmed": 115, "received": 95},
                  "peak_cluster_rss_bytes": 768, "p99_us": {"confirm": 4, "consumer": 5}}
        engine = summarize({"case": {"strangeq": [first, second]}})["case"]["strangeq"]
        self.assertEqual(engine["sent_per_second"], {"median": 130, "min": 120, "max": 140})
        self.assertEqual(engine["confirms_per_second"]["median"], 110)
        self.assertEqual(engine["deliveries_per_second"]["median"], 90)


class PlacementTests(unittest.TestCase):
    def test_singleton_requires_a_durable_running_classic_queue(self):
        case = CASES["1024-1x1-1q"]
        nodes = {"one@127.0.0.1"}
        row = f"{case.queue_names[0]}\tclassic\ttrue\trunning"
        placement = parse_rabbit_queues(row, case, nodes, topology="singleton")
        self.assertEqual(placement[case.queue_names[0]],
                         {"leader": next(iter(nodes)), "type": "classic", "durable": True})
        for bad_row in (row.replace("classic", "quorum"), row.replace("true", "false"),
                        row.replace("running", "down"), row + "\n" + row, row + "\textra", ""):
            with self.subTest(row=bad_row), self.assertRaises(BenchmarkError):
                parse_rabbit_queues(bad_row, case, nodes, topology="singleton")
        with self.assertRaises(BenchmarkError):
            parse_rabbit_queues(row, case, nodes | {"two@127.0.0.1"}, topology="singleton")

    def test_requires_durable_quorum_queue_with_all_three_online_members(self):
        case = CASES["1024-1x1-1q"]
        nodes = {"one@127.0.0.1", "two@127.0.0.1", "three@127.0.0.1"}
        members = "[one@127.0.0.1, two@127.0.0.1, three@127.0.0.1]"
        row = f"{case.queue_names[0]}\tquorum\ttrue\tone@127.0.0.1\t{members}\t{members}"
        placement = parse_rabbit_queues(row, case, nodes)
        self.assertEqual(placement[case.queue_names[0]]["leader"], "one@127.0.0.1")
        for bad_row in (
            row.replace("\tquorum\t", "\tclassic\t"),
            row.replace("\ttrue\t", "\tfalse\t"),
            row.replace(members, "[one@127.0.0.1, two@127.0.0.1]"),
            row + "\n" + row,
        ):
            with self.subTest(row=bad_row), self.assertRaises(BenchmarkError):
                parse_rabbit_queues(bad_row, case, nodes)

    def make_strangeq_cluster(self, root, *, local=False):
        seeds = ([{"ID": (1 << 62) + 17, "Address": ""}] if local else
                 [{"ID": i, "Address": f"127.0.0.1:{16000 + i}"} for i in range(1, 4)])
        voters = [peer["ID"] for peer in seeds]
        voter_set = {"Voters": voters, "Learners": None}
        paths = [root / f"node-{i}" for i in range(1, len(seeds) + 1)]
        lineage = "a" * 32 if local else root.name
        view = {
            "Cluster": lineage, "Shards": 1, "Nodes": seeds, "Nonces": None,
            "Catalog": {"Group": cluster_compare.CATALOG_GROUP, "Index": 1, **voter_set}, "CatalogTarget": voters,
            "Placements": [{"Group": 1, "Index": 1, **voter_set}],
        }
        for path, peer in zip(paths, seeds):
            cluster = path / "cluster"
            identity = {"Peer": peer, "Cluster": lineage, "Shards": 1, "Seeds": seeds, "Nonce": ""}
            if local:
                identity["Local"] = True
            (cluster / "raft-journal").mkdir(parents=True)
            (cluster / "raft-journal" / "extent-0000000000000001.raft-journal").write_bytes(b"frame")
            (cluster / "raft-journal" / "segments.manifest").write_bytes(b"manifest")
            (cluster / "journal-provisioned-v1").write_bytes(b"\x01")
            (cluster / "raft-storage-mode-v1").write_text("segmented-journal-dead-letter-v2")
            (cluster / "opened-18446744073709551615").write_bytes(b"\x01")
            (cluster / "opened-1").write_bytes(b"\x01")
            (cluster / "journal-identity-v1.json").write_text(json.dumps(identity))
            if local:
                (cluster / "journal-identity-v1.json").chmod(0o600)
            (cluster / "catalog-cache").write_text(json.dumps(view))
        return (paths, None if local else seeds), view

    def test_local_journal_requires_private_identity_and_sole_committed_voters(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, view = self.make_strangeq_cluster(Path(scratch), local=True)
            placement = verify_strangeq_placement(context, case, topology="singleton", expected_shards=1)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [view["Nodes"][0]["ID"]])
            self.assertTrue(placement[case.queue_names[0]]["local"])
            cluster = context[0][0] / "cluster"
            identity_path = cluster / "journal-identity-v1.json"
            identity = json.loads(identity_path.read_text())
            invalid_identities = [dict(identity, Local=False), dict(identity, Cluster=""),
                                  dict(identity, Peer={"ID": 0, "Address": ""}),
                                  dict(identity, Peer={"ID": identity["Peer"]["ID"], "Address": "127.0.0.1:1"}),
                                  dict(identity, Nonce="enrolled"), dict(identity, Seeds=[])]
            for invalid in invalid_identities:
                identity_path.write_text(json.dumps(invalid))
                with self.subTest(identity=invalid), self.assertRaises(BenchmarkError):
                    verify_strangeq_placement(context, case, topology="singleton")
            identity_path.write_text(json.dumps(identity))
            identity_path.chmod(0o644)
            with self.assertRaisesRegex(BenchmarkError, "private identity"):
                verify_strangeq_placement(context, case, topology="singleton")
            identity_path.chmod(0o600)
            activation = cluster / "peer-activation-v1.json"
            activation.touch()
            with self.assertRaisesRegex(BenchmarkError, "peer activation"):
                verify_strangeq_placement(context, case, topology="singleton")
            activation.unlink()
            other_identity = cluster / "journal-small-body-identity-v1.json"
            other_identity.touch()
            with self.assertRaisesRegex(BenchmarkError, "identities coexist"):
                verify_strangeq_placement(context, case, topology="singleton")
            other_identity.unlink()
            for mutate in (
                lambda v: v.update(Nodes=[]),
                lambda v: v["Catalog"].update(Voters=[1, 2, 3]),
                lambda v: v["Catalog"].update(Index=0),
                lambda v: v["Placements"][0].update(Learners=[1]),
                lambda v: v["Placements"][0].update(Index=0),
                lambda v: v["Placements"].append(v["Placements"][0]),
            ):
                invalid = copy.deepcopy(view)
                mutate(invalid)
                (cluster / "catalog-cache").write_text(json.dumps(invalid))
                with self.subTest(view=invalid), self.assertRaises(BenchmarkError):
                    verify_strangeq_placement(context, case, topology="singleton")
            (cluster / "catalog-cache").write_text(json.dumps(view))
            with self.assertRaisesRegex(BenchmarkError, "64 shards"):
                verify_strangeq_placement(context, case, topology="singleton", expected_shards=64)
            (cluster / "raft-journal/extent-0000000000000001.raft-journal").unlink()
            with self.assertRaisesRegex(BenchmarkError, "segmented journal"):
                verify_strangeq_placement(context, case, topology="singleton")

    def test_initial_readiness_waits_for_committed_catalog_and_workload_reports(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, ready = self.make_strangeq_cluster(Path(scratch), local=True)
            pending = copy.deepcopy(ready)
            pending["Catalog"]["Index"] = pending["Placements"][0]["Index"] = 0
            cache = context[0][0] / "cluster/catalog-cache"
            cache.write_text(json.dumps(pending))
            with self.assertRaises(cluster_compare.PendingMembershipReport):
                verify_strangeq_placement(context, case, topology="singleton")
            with mock.patch("cluster_compare.time.sleep", side_effect=lambda _: cache.write_text(json.dumps(ready))) as sleep:
                placement = cluster_compare.wait_for_strangeq_placement(context, case, topology="singleton")
            sleep.assert_called_once()
            self.assertEqual(placement[case.queue_names[0]]["voters"], [ready["Nodes"][0]["ID"]])

    def test_initial_readiness_timeout_and_hard_failures_never_relax_validation(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, pending = self.make_strangeq_cluster(Path(scratch), local=True)
            pending["Catalog"]["Index"] = pending["Placements"][0]["Index"] = 0
            cluster = context[0][0] / "cluster"
            cache = cluster / "catalog-cache"
            cache.write_text(json.dumps(pending))
            with mock.patch("cluster_compare.time.monotonic", side_effect=[0, 0, 10]), \
                    mock.patch("cluster_compare.time.sleep") as sleep, \
                    self.assertRaisesRegex(BenchmarkError, "readiness timed out after 10s"):
                cluster_compare.wait_for_strangeq_placement(context, case, topology="singleton")
            sleep.assert_called_once()
            for mutate in (
                lambda v: v["Placements"][0].update(Voters=[1, 2, 3]),
                lambda v: v["Placements"].append(v["Placements"][0]),
                lambda v: v["Placements"][0].update(Index=-1),
                lambda v: v["Placements"][0].pop("Index"),
            ):
                invalid = copy.deepcopy(pending)
                mutate(invalid)
                cache.write_text(json.dumps(invalid))
                with self.subTest(view=invalid), mock.patch("cluster_compare.time.sleep") as sleep, \
                        self.assertRaises(BenchmarkError) as error:
                    cluster_compare.wait_for_strangeq_placement(context, case, topology="singleton")
                self.assertNotIsInstance(error.exception, cluster_compare.PendingMembershipReport)
                sleep.assert_not_called()
            cache.write_text(json.dumps(pending))
            for name, contents in (("opened-1", b"\x00"), ("raft-storage-mode-v1", b"bolt"),
                                   ("journal-identity-v1.json", b"{}")):
                path = cluster / name
                previous = path.read_bytes()
                path.write_bytes(contents)
                with self.subTest(name=name), mock.patch("cluster_compare.time.sleep") as sleep, \
                        self.assertRaises(BenchmarkError):
                    cluster_compare.wait_for_strangeq_placement(context, case, topology="singleton")
                sleep.assert_not_called()
                path.write_bytes(previous)

    def test_post_timing_membership_report_failure_is_not_polled(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            context, pending = self.make_strangeq_cluster(root, local=True)
            pending["Placements"][0]["Index"] = 0
            args = mock.Mock(scratch=root, topology="singleton", validate_only=False, strangeq_shards=1,
                             strangeq_ack_and_claim=False, strangeq_assignments=False,
                             strangeq_journal_body_compression=False, strangeq_dual_stream=False,
                             strangeq_journal_small_body_compression=False)

            def finish_measurement(*_):
                (context[0][0] / "cluster/catalog-cache").write_text(json.dumps(pending))
                return {}

            with mock.patch("cluster_compare.launch_strangeq", return_value=([mock.Mock()], [16000], context)), \
                    mock.patch("cluster_compare.perftest_args", return_value=[]), \
                    mock.patch("cluster_compare.run_logged", return_value="test stopped (Reached time limit)"), \
                    mock.patch("cluster_compare.measure", side_effect=finish_measurement), \
                    mock.patch("cluster_compare.time.sleep") as sleep, \
                    self.assertRaises(cluster_compare.PendingMembershipReport):
                cluster_compare.trial(args, case, False, root / "trial", {})
            sleep.assert_not_called()

    def test_seeded_catalog_accepts_null_nonces_only_for_the_three_provisioned_voters(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            (paths, seeds), view = self.make_strangeq_cluster(Path(scratch))
            placement = verify_strangeq_placement((paths, seeds), case)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [1, 2, 3])
            last_catalog = paths[-1] / "cluster" / "catalog-cache"
            invalid_views = (
                dict(view, Nodes=seeds[:2]),
                dict(view, Nonces={"3": "new-peer-nonce"}),
                dict(view, Placements=[{"Group": 1, "Voters": [1, 2], "Learners": [3]}]),
            )
            for invalid in invalid_views:
                last_catalog.write_text(json.dumps(invalid))
                with self.subTest(view=invalid), self.assertRaises(BenchmarkError):
                    verify_strangeq_placement((paths, seeds), case)

    def test_journal_placement_requires_a_single_shared_local_store(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, view = self.make_strangeq_cluster(Path(scratch))
            placement = verify_strangeq_placement(context, case)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [1, 2, 3])
            cluster = context[0][-1] / "cluster"
            invalid_files = {
                "journal-provisioned-v1": b"\x00",
                "raft-storage-mode-v1": b"bolt",
                "raft-journal/active.raft-journal": b"",
                "1/active.raft-journal": b"extra",
                "1/raft.db": b"legacy",
                "identity.json": b"legacy",
                "retired-1": b"\x01",
            }
            for name, contents in invalid_files.items():
                path = cluster / name
                path.parent.mkdir(exist_ok=True)
                previous = path.read_bytes() if path.exists() else None
                path.write_bytes(contents)
                with self.subTest(name=name), self.assertRaises(BenchmarkError):
                    verify_strangeq_placement(context, case)
                if previous is None:
                    path.unlink()
                else:
                    path.write_bytes(previous)
            for name in ("opened-1", "opened-18446744073709551615", "journal-identity-v1.json"):
                path = cluster / name
                previous = path.read_bytes()
                path.unlink()
                with self.subTest(name=name), self.assertRaises(BenchmarkError):
                    verify_strangeq_placement(context, case)
                path.write_bytes(previous)
            (cluster / "catalog-cache").write_text(json.dumps(dict(view, Nonces={"3": "rejoined"})))
            with self.assertRaisesRegex(BenchmarkError, "catalog or local identity"):
                verify_strangeq_placement(context, case)

    def test_placement_requires_the_explicit_shard_count(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, _ = self.make_strangeq_cluster(Path(scratch))
            with self.assertRaisesRegex(BenchmarkError, "64 shards"):
                verify_strangeq_placement(context, case, expected_shards=64)
            placement = verify_strangeq_placement(context, case, expected_shards=1)
            self.assertEqual(placement[case.queue_names[0]]["shard"], 1)

    def test_ack_and_claim_mode_requires_all_journal_replicas_to_opt_in(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, _ = self.make_strangeq_cluster(Path(scratch))
            for node in context[0][:-1]:
                (node / "cluster/raft-storage-mode-v1").write_bytes(b"segmented-journal-ack-and-claim-dead-letter-v2")
            with self.assertRaisesRegex(BenchmarkError, "journal mode"):
                verify_strangeq_placement(context, case, ack_and_claim=True)
            (context[0][-1] / "cluster/raft-storage-mode-v1").write_bytes(b"segmented-journal-ack-and-claim-dead-letter-v2")
            placement = verify_strangeq_placement(context, case, ack_and_claim=True)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [1, 2, 3])
            with self.assertRaisesRegex(BenchmarkError, "journal mode"):
                verify_strangeq_placement(context, case)

    def test_small_body_mode_requires_its_identity_on_all_voters(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, _ = self.make_strangeq_cluster(Path(scratch))
            for node in context[0]:
                cluster = node / "cluster"
                (cluster / "raft-storage-mode-v1").write_bytes(b"segmented-journal-assignments-small-compressed-bodies-dead-letter-v2")
                (cluster / "journal-identity-v1.json").rename(cluster / "journal-small-body-identity-v1.json")
            options = dict(assignments=True, body_compression=True, small_body_compression=True)
            placement = verify_strangeq_placement(context, case, **options)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [1, 2, 3])
            (context[0][-1] / "cluster/journal-identity-v1.json").touch()
            with self.assertRaisesRegex(BenchmarkError, "legacy identities coexist"):
                verify_strangeq_placement(context, case, **options)


if __name__ == "__main__":
    unittest.main()
