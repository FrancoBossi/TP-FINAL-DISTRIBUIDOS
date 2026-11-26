import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.chaos import main


class ChaosIntervalSweepTest(unittest.TestCase):
    def test_sweep_keeps_failed_attempt_and_reports_smallest_passing_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "sweep"
            args = [
                "chaos",
                "sweep",
                "--dataset",
                "datasets/sample.parquet",
                "--expected-output",
                "output/reference",
                "--output",
                str(output),
                "--target",
                "worker",
                "--timeout-seconds",
                "60",
            ]

            def run_benchmark(command, **kwargs):
                interval = float(command[command.index("--chaos-interval") + 1])
                case = Path(command[command.index("--results-root") + 1]) / "trial"
                case.mkdir()
                runs = []
                for index in range(1, 4):
                    run = case / f"run-{index:02d}"
                    run.mkdir()
                    events = [
                        {"event": "injection_finished", "returncode": 0, "cycle": 1},
                        {
                            "event": "injection_finished",
                            "returncode": 0,
                            "cycle": 1 if interval == 1 else 2,
                        },
                    ]
                    (run / "chaos.jsonl").write_text(
                        "".join(json.dumps(event) + "\n" for event in events),
                        encoding="utf-8",
                    )
                    runs.append(
                        {
                            "run": index,
                            "status": "success",
                            "matches_expected": True,
                            "cleanup": {"passed": True},
                        }
                    )
                (case / "summary.json").write_text(
                    json.dumps({"runs": runs}), encoding="utf-8"
                )
                return subprocess.CompletedProcess(command, 0)

            with (
                patch("sys.argv", args),
                patch(
                    "scripts.chaos_benchmark.subprocess.run",
                    side_effect=run_benchmark,
                ),
                patch("builtins.print"),
            ):
                self.assertEqual(main(), 0)

            report = json.loads((output / "sweep.json").read_text(encoding="utf-8"))
            self.assertEqual(report["minimum_passing_interval_seconds"], 2.5)
            self.assertEqual(
                [attempt["passed"] for attempt in report["attempts"]],
                [True, True, False],
            )
            self.assertEqual(report["attempts"][-1]["runs"][0]["confirmed_cycles"], 1)
