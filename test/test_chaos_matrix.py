import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_chaos_matrix import matrix_cases
from scripts.sweep_chaos_intervals import assess_interval, confirmed_cycles


class ChaosMatrixTest(unittest.TestCase):
    def test_covers_each_type_and_excludes_protected_services(self):
        cases, pending = matrix_cases(
            [
                "rabbitmq",
                "gateway",
                "client",
                "grafana",
                "preprocessor-1",
                "preprocessor-0",
                "query-router-0",
                "result-sink",
            ],
            "all",
        )
        self.assertEqual(
            cases,
            {
                "preprocessor": ["preprocessor-0"],
                "query-router": ["query-router-0"],
                "result-sink": ["result-sink"],
                "simultaneous-workers": ["preprocessor-0", "query-router-0"],
            },
        )
        self.assertIn("failure-manager", pending)

    def test_simultaneous_case_requires_both_targets_in_a_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "chaos.jsonl"
            log.write_text(
                "".join(
                    json.dumps(
                        {
                            "event": "injection_finished",
                            "returncode": 0,
                            "cycle": cycle,
                            "targets": targets,
                        }
                    )
                    + "\n"
                    for cycle, targets in enumerate(
                        [["worker"], ["worker", "router"]], 1
                    )
                ),
                encoding="utf-8",
            )
            self.assertEqual(confirmed_cycles(log, {"worker", "router"}), 1)

    def test_matrix_assessment_can_require_one_confirmed_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "evidence"
            run = evidence / "run-01"
            run.mkdir(parents=True)
            (evidence / "summary.json").write_text(
                json.dumps(
                    {
                        "runs": [
                            {
                                "run": 1,
                                "status": "success",
                                "matches_expected": True,
                                "cleanup": {"passed": True},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (run / "chaos.jsonl").write_text(
                json.dumps(
                    {
                        "event": "injection_finished",
                        "returncode": 0,
                        "cycle": 1,
                        "targets": ["worker", "router"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            result = assess_interval(
                root,
                0,
                1,
                {"worker", "router"},
                minimum_confirmed_cycles=1,
            )

        self.assertTrue(result["passed"])
