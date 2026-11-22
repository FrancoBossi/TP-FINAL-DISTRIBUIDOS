import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_chaos_matrix import matrix_cases
from scripts.sweep_chaos_intervals import confirmed_cycles


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
