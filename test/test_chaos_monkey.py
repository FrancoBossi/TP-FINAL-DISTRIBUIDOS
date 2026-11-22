import io
import json
import subprocess
import unittest
from threading import Event
from unittest.mock import patch

from scripts.chaos_monkey import eligible_targets, failure_batches, run_chaos


class ChaosMonkeyTest(unittest.TestCase):
    def test_schedule_is_seeded_and_respects_failure_budget(self):
        targets = ["worker-0", "worker-1", "worker-2"]
        batches = list(failure_batches(targets, 27, 5, 2))

        self.assertEqual(batches, list(failure_batches(reversed(targets), 27, 5, 2)))
        self.assertEqual([len(set(batch)) for batch in batches], [2, 2, 1])

    def test_batch_preserves_last_running_failure_manager(self):
        selected, skipped = eligible_targets(
            ["failure-manager-0", "failure-manager-1", "failure-manager-2", "worker"],
            ["failure-manager-0", "failure-manager-2", "worker"],
        )

        self.assertEqual(selected, ["failure-manager-0", "worker"])
        self.assertEqual(
            skipped,
            {
                "failure-manager-1": "not_running",
                "failure-manager-2": "last_failure_manager",
            },
        )

    @patch("scripts.chaos_monkey.time.sleep")
    @patch("scripts.chaos_monkey.subprocess.run")
    def test_kill_is_scoped_to_project_and_logged(self, run, sleep):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, "worker\n", ""),
            subprocess.CompletedProcess([], 0, "worker\n", ""),
            subprocess.CompletedProcess([], 0, "", ""),
        ]
        log = io.StringIO()
        compose = ["docker", "compose", "--project-name", "scenario"]

        run_chaos(
            compose,
            ["worker"],
            seed=7,
            interval=5,
            max_failures=1,
            batch_size=1,
            log=log,
        )

        run.assert_called_with(
            [*compose, "kill", "--signal", "SIGKILL", "worker"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        sleep.assert_called_once()
        events = [json.loads(line) for line in log.getvalue().splitlines()]
        self.assertEqual(
            events[0]["batches"], [{"offset_seconds": 5, "targets": ["worker"]}]
        )
        self.assertEqual(events[-1]["event"], "injection_finished")
        self.assertEqual(events[-1]["returncode"], 0)

    @patch("scripts.chaos_monkey.subprocess.run")
    def test_cancelled_run_does_not_inject_future_failures(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "worker\n", "")
        stop = Event()
        stop.set()

        run_chaos(
            ["docker", "compose"],
            ["worker"],
            seed=7,
            interval=5,
            max_failures=1,
            batch_size=1,
            log=io.StringIO(),
            stop=stop,
        )

        run.assert_called_once()

    @patch("scripts.chaos_monkey.subprocess.run")
    def test_rejects_protected_service_before_trying_to_kill(self, run):
        run.return_value = subprocess.CompletedProcess([], 0, "rabbitmq\n", "")

        with self.assertRaisesRegex(ValueError, "rabbitmq"):
            run_chaos(
                ["docker", "compose"],
                ["rabbitmq"],
                seed=7,
                interval=5,
                max_failures=1,
                batch_size=1,
                log=io.StringIO(),
            )

        run.assert_called_once()
