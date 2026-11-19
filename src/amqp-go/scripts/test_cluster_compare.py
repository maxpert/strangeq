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
            self.assertFalse((evidence / "summary.json").exists())
            failures = json.loads((evidence / "failures.json").read_text())
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["trial"], "01-1024-1x1-1q-strangeq")
            self.assertIn("negative confirm", failures[0]["error"])


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

    def make_strangeq_cluster(self, root, *, journal=False):
        seeds = [{"ID": i, "Address": f"127.0.0.1:{16000 + i}"} for i in range(1, 4)]
        voter_set = {"Voters": [1, 2, 3], "Learners": None}
        paths = [root / f"node-{i}" for i in range(1, 4)]
        view = {
            "Cluster": root.name, "Shards": 1, "Nodes": seeds, "Nonces": None,
            "Catalog": voter_set, "CatalogTarget": [1, 2, 3],
            "Placements": [{"Group": 1, **voter_set}],
        }
        for path, peer in zip(paths, seeds):
            cluster = path / "cluster"
            identity = {"Peer": peer, "Cluster": root.name, "Shards": 1, "Seeds": seeds, "Nonce": ""}
            if journal:
                (cluster / "raft-journal").mkdir(parents=True)
                (cluster / "raft-journal" / "active.raft-journal").write_bytes(b"frame")
                (cluster / "journal-provisioned-v1").write_bytes(b"\x01")
                (cluster / "raft-storage-mode-v1").write_text("journal")
                (cluster / "opened-18446744073709551615").write_bytes(b"\x01")
                (cluster / "opened-1").write_bytes(b"\x01")
                (cluster / "journal-identity-v1.json").write_text(json.dumps(identity))
            else:
                (cluster / "1").mkdir(parents=True)
                (cluster / "identity.json").write_text(json.dumps(identity))
            (cluster / "catalog-cache").write_text(json.dumps(view))
        return (paths, seeds), view

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
            context, view = self.make_strangeq_cluster(Path(scratch), journal=True)
            placement = verify_strangeq_placement(context, case, journal=True)
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
                    verify_strangeq_placement(context, case, journal=True)
                if previous is None:
                    path.unlink()
                else:
                    path.write_bytes(previous)
            for name in ("opened-1", "opened-18446744073709551615", "journal-identity-v1.json"):
                path = cluster / name
                previous = path.read_bytes()
                path.unlink()
                with self.subTest(name=name), self.assertRaises(BenchmarkError):
                    verify_strangeq_placement(context, case, journal=True)
                path.write_bytes(previous)
            (cluster / "catalog-cache").write_text(json.dumps(dict(view, Nonces={"3": "rejoined"})))
            with self.assertRaisesRegex(BenchmarkError, "catalog or local identity"):
                verify_strangeq_placement(context, case, journal=True)

    def test_placement_requires_the_explicit_shard_count(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, _ = self.make_strangeq_cluster(Path(scratch), journal=True)
            with self.assertRaisesRegex(BenchmarkError, "64 shards"):
                verify_strangeq_placement(context, case, journal=True, expected_shards=64)
            placement = verify_strangeq_placement(context, case, journal=True, expected_shards=1)
            self.assertEqual(placement[case.queue_names[0]]["shard"], 1)

    def test_ack_and_claim_mode_requires_all_journal_replicas_to_opt_in(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, _ = self.make_strangeq_cluster(Path(scratch), journal=True)
            for node in context[0][:-1]:
                (node / "cluster/raft-storage-mode-v1").write_bytes(b"journal-ack-and-claim")
            with self.assertRaisesRegex(BenchmarkError, "journal mode"):
                verify_strangeq_placement(context, case, journal=True, ack_and_claim=True)
            (context[0][-1] / "cluster/raft-storage-mode-v1").write_bytes(b"journal-ack-and-claim")
            placement = verify_strangeq_placement(context, case, journal=True, ack_and_claim=True)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [1, 2, 3])
            with self.assertRaisesRegex(BenchmarkError, "journal mode"):
                verify_strangeq_placement(context, case, journal=True)

    def test_small_body_mode_requires_its_identity_on_all_voters(self):
        case = CASES["1024-1x1-1q"]
        with tempfile.TemporaryDirectory() as scratch:
            context, _ = self.make_strangeq_cluster(Path(scratch), journal=True)
            for node in context[0]:
                cluster = node / "cluster"
                (cluster / "raft-storage-mode-v1").write_bytes(b"journal-assignments-small-compressed-bodies")
                (cluster / "journal-identity-v1.json").rename(cluster / "journal-small-body-identity-v1.json")
            options = dict(journal=True, assignments=True, body_compression=True, small_body_compression=True)
            placement = verify_strangeq_placement(context, case, **options)
            self.assertEqual(placement[case.queue_names[0]]["voters"], [1, 2, 3])
            (context[0][-1] / "cluster/journal-identity-v1.json").touch()
            with self.assertRaisesRegex(BenchmarkError, "legacy identities coexist"):
                verify_strangeq_placement(context, case, **options)


if __name__ == "__main__":
    unittest.main()
