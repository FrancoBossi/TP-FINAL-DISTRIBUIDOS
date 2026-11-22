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
    resolve_dataset,
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
                output = f"results.{execution_id}\n"
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
        self.assertEqual(verdict["remaining"]["rabbitmq"], [f"results.{execution_id}"])
        self.assertIn("result-sink", verdict["remaining"])

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
