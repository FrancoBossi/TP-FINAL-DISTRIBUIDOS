import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_benchmark import (
    count_injections,
    multiset_fingerprint,
    parse_io_bytes,
    parse_memory_bytes,
    queue_cleanup_findings,
    read_first_result_seconds,
    read_journal_bytes,
    resolve_dataset,
    summarize_metrics,
    summarize_runs,
    summarize_stack_metrics,
    verify_cleanup,
)


class MultisetFingerprintTest(unittest.TestCase):
    def write_jsonl(self, path, records):
        path.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    def test_fingerprint_ignores_record_order(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.jsonl"
            second = Path(directory) / "second.jsonl"
            self.write_jsonl(first, [{"value": 1}, {"value": 2}])
            self.write_jsonl(second, [{"value": 2}, {"value": 1}])

            first_fingerprint = multiset_fingerprint(first)
            second_fingerprint = multiset_fingerprint(second)
            first_fingerprint.pop("bytes")
            second_fingerprint.pop("bytes")

            self.assertEqual(first_fingerprint, second_fingerprint)

    def test_fingerprint_preserves_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.jsonl"
            second = Path(directory) / "second.jsonl"
            self.write_jsonl(first, [{"value": 1}])
            self.write_jsonl(second, [{"value": 1}, {"value": 1}])

            self.assertNotEqual(
                multiset_fingerprint(first), multiset_fingerprint(second)
            )


class BenchmarkHelpersTest(unittest.TestCase):
    def test_stack_peaks_use_simultaneous_samples(self):
        samples = [
            ("t1", "a", "110%", "30MiB"),
            ("t1", "b", "10%", "10MiB"),
            ("t2", "a", "0%", "10MiB"),
            ("t2", "b", "70%", "50MiB"),
            ("incomplete", "a", "300%", "500MiB"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            path.write_text(
                "".join(
                    json.dumps(
                        {
                            "captured_at": timestamp,
                            "expected_containers": 2,
                            "Name": name,
                            "CPUPerc": cpu,
                            "MemUsage": f"{memory} / 1GiB",
                        }
                    )
                    + "\n"
                    for timestamp, name, cpu, memory in samples
                ),
                encoding="utf-8",
            )
            stack = summarize_stack_metrics(path)

        self.assertEqual(stack["samples"], 2)
        self.assertEqual(stack["cpu_percent_mean"], 95)
        self.assertEqual(stack["cpu_percent_peak"], 120)
        self.assertEqual(stack["memory_bytes_peak"], 60 * 1024**2)
        aggregate = summarize_runs([{"status": "success", "stack_metrics": stack}])
        self.assertEqual(aggregate["memory_bytes_peak"]["median"], 60 * 1024**2)

    def test_network_summary_accumulates_observed_counter_resets(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            samples = [
                {
                    "Name": "worker",
                    "CPUPerc": "10%",
                    "MemUsage": "1MiB / 2MiB",
                    "NetIO": net,
                }
                for net in ("100B / 50B", "140B / 90B", "20B / 10B", "70B / 40B")
            ]
            path.write_text(
                "".join(json.dumps(sample) + "\n" for sample in samples),
                encoding="utf-8",
            )
            metrics = summarize_metrics(path)

        worker = metrics["worker"]
        self.assertEqual(worker["network_receive_bytes_final"], 70)
        self.assertEqual(worker["network_receive_bytes_sampled"], 210)
        self.assertEqual(worker["network_transmit_bytes_sampled"], 130)
        report = summarize_runs(
            [{"status": "success", "chaos": True, "metrics": metrics}]
        )
        self.assertEqual(report["network_receive_bytes"]["median"], 210)

    def test_legacy_final_network_counter_is_used_only_for_fault_free_runs(self):
        run = {
            "status": "success",
            "metrics": {"worker": {"network_receive_bytes_final": 70}},
        }
        self.assertEqual(summarize_runs([run])["network_receive_bytes"]["median"], 70)
        run["chaos"] = True
        self.assertIsNone(summarize_runs([run])["network_receive_bytes"]["median"])

    def test_journal_bytes_count_each_generation_once(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "compose.log"
            log.write_text("No journal telemetry\n", encoding="utf-8")
            self.assertIsNone(read_journal_bytes(log))
            samples = [
                ("first", 0, 0, 12),
                ("first", 0, 0, 30),
                ("first", 0, 0, 30),
                ("first", 0, 1, 40),
                ("second", 1, 0, 50),
            ]
            log.write_text(
                "".join(
                    f"Journal bytes execution={execution} partition={partition} "
                    f"file=pending-{generation}.jsonl bytes={size}\n"
                    for execution, partition, generation, size in samples
                ),
                encoding="utf-8",
            )
            self.assertEqual(read_journal_bytes(log), 120)

    def test_first_result_metric_is_optional_and_parsed_from_client_log(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "client.log"
            log.write_text("All queries complete: 0 results\n", encoding="utf-8")
            self.assertIsNone(read_first_result_seconds(log))
            log.write_text(
                "2026-11-22 18:00:00 INFO First result persisted after 1.234567 seconds\n",
                encoding="utf-8",
            )
            self.assertAlmostEqual(read_first_result_seconds(log), 1.234567)

    def test_aggregate_excludes_failed_and_inconsistent_runs(self):
        runs = [
            {"status": "success", "duration_seconds": 10, "first_result_seconds": 2},
            {"status": "success", "duration_seconds": 14},
            {"status": "failed", "duration_seconds": 1, "first_result_seconds": 1},
            {
                "status": "success",
                "duration_seconds": 1,
                "matches_first_success": False,
            },
        ]

        summary = summarize_runs(runs)

        self.assertEqual(summary["duration_seconds"]["median"], 12)
        self.assertAlmostEqual(summary["duration_seconds"]["sample_stddev"], 8**0.5)
        self.assertEqual(summary["first_result_seconds"]["samples"], 1)
        self.assertIsNone(summary["first_result_seconds"]["sample_stddev"])

    @patch("scripts.run_benchmark.subprocess.run")
    def test_cleanup_requires_queue_and_persistent_state_to_disappear(self, run):
        execution_id = "e1-123-" + "a" * 32
        config = {
            "services": {"result-sink": {"environment": {"STATE_ROOT": "/state"}}}
        }

        def compose_query(command, **kwargs):
            if "config" in command:
                output = json.dumps(config)
            elif "rabbitmqctl" in command:
                output = json.dumps(
                    [
                        {
                            "name": f"results.{execution_id}",
                            "messages_ready": 0,
                            "messages_unacknowledged": 0,
                        },
                        {
                            "name": "pipeline.input",
                            "messages_ready": 2,
                            "messages_unacknowledged": 1,
                        },
                    ]
                )
            else:
                output = f"/state/result_sink/partition-0/active/{execution_id}.json\n"
            return subprocess.CompletedProcess(command, 0, output, "")

        run.side_effect = compose_query
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "compose.log"
            log.write_text(
                f"gateway-1 | Execution {execution_id}: starting upload\n",
                encoding="utf-8",
            )
            verdict = verify_cleanup(["docker", "compose"], Path(directory), {}, log, 0)

        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["execution_id"], execution_id)
        self.assertEqual(
            verdict["remaining"]["rabbitmq"],
            [
                f"temporary queue still exists: results.{execution_id}",
                "pipeline.input: ready=2, unacked=1",
            ],
        )
        self.assertIn("result-sink", verdict["remaining"])

    def test_queue_cleanup_requires_every_queue_to_be_drained(self):
        execution_id = "e1-123-" + "b" * 32
        drained = json.dumps(
            [
                {
                    "name": "pipeline.input",
                    "messages_ready": 0,
                    "messages_unacknowledged": 0,
                }
            ]
        )
        pending = json.dumps(
            [
                {
                    "name": "pipeline.input",
                    "messages_ready": 1,
                    "messages_unacknowledged": 0,
                }
            ]
        )

        self.assertEqual(queue_cleanup_findings(drained, execution_id), [])
        self.assertEqual(
            queue_cleanup_findings(pending, execution_id),
            ["pipeline.input: ready=1, unacked=0"],
        )

    @patch("scripts.run_benchmark.subprocess.run")
    def test_cleanup_requires_only_the_selected_input_batcher_tombstone(self, run):
        execution_id = "e1-123-" + "c" * 32
        config = {
            "services": {
                "input-batcher-0": {
                    "environment": {
                        "STATE_ROOT": "/state",
                        "INPUT_BATCHER_PARTITION_ID": "0",
                    }
                },
                "input-batcher-1": {
                    "environment": {
                        "STATE_ROOT": "/state",
                        "INPUT_BATCHER_PARTITION_ID": "1",
                    }
                },
                "result-sink": {"environment": {"STATE_ROOT": "/state"}},
            }
        }
        inspections = []

        def compose_query(command, **kwargs):
            if "config" in command:
                output = json.dumps(config)
            elif "rabbitmqctl" in command:
                output = json.dumps(
                    [
                        {
                            "name": "pipeline.input",
                            "messages_ready": 0,
                            "messages_unacknowledged": 0,
                        }
                    ]
                )
            else:
                inspections.append((command[4], command[-1]))
                output = ""
            return subprocess.CompletedProcess(command, 0, output, "")

        run.side_effect = compose_query
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "compose.log"
            log.write_text(
                f"gateway-1 | Execution {execution_id}: starting upload\n",
                encoding="utf-8",
            )
            verdict = verify_cleanup(["docker", "compose"], Path(directory), {}, log, 0)

        selected_partition = (
            int.from_bytes(hashlib.sha256(execution_id.encode()).digest(), "big") % 2
        )
        expected = {
            "input-batcher-0": "yes" if selected_partition == 0 else "no",
            "input-batcher-1": "yes" if selected_partition == 1 else "no",
            "result-sink": "yes",
        }
        self.assertTrue(verdict["passed"])
        self.assertEqual(dict(inspections), expected)

    def test_only_confirmed_injections_count(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "chaos.jsonl"
            self.assertEqual(count_injections(log), 0)
            events = [
                {"event": "injection_started", "targets": ["worker"]},
                {"event": "injection_finished", "returncode": 1, "targets": ["worker"]},
                {
                    "event": "injection_finished",
                    "returncode": 0,
                    "targets": ["worker", "sink"],
                },
            ]
            log.write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )

            self.assertEqual(count_injections(log), 2)

    def test_parse_binary_and_decimal_memory_units(self):
        self.assertEqual(parse_memory_bytes("1.5MiB / 2GiB"), 1_572_864)
        self.assertEqual(parse_memory_bytes("2.5MB / 1GB"), 2_500_000)
        self.assertEqual(parse_io_bytes("1.5kB / 2MiB"), (1_500, 2_097_152))

    def test_resolve_dataset_builds_container_path(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)
            dataset = repository / "datasets" / "sample.parquet"
            dataset.parent.mkdir()
            dataset.touch()

            path, container_path = resolve_dataset(
                repository, Path("datasets/sample.parquet")
            )

            self.assertEqual(path, dataset.resolve())
            self.assertEqual(container_path, "/datasets/sample.parquet")

    def test_resolve_dataset_rejects_file_outside_dataset_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)
            (repository / "datasets").mkdir()
            dataset = repository / "sample.parquet"
            dataset.touch()

            with self.assertRaises(ValueError):
                resolve_dataset(repository, dataset)


if __name__ == "__main__":
    unittest.main()
