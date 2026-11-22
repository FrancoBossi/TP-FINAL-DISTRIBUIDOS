import unittest
from copy import deepcopy

from scripts.compare_benchmarks import compare_benchmarks
from scripts.run_benchmark import QUERY_FILES


def summary(duration, *, verified=False):
    return {
        "commit": "test-commit",
        "dataset": {"sha256": "fixture-sha256", "rows": 24},
        "runs": [
            {
                "run": index,
                "status": "success",
                "duration_seconds": duration,
                "articles_per_second": 24 / duration,
                "matches_expected": True if verified else None,
                "fingerprints": {
                    query: {"records": 3, "sum": "a", "sum_squares": "b", "xor": "c"}
                    for query in QUERY_FILES
                },
            }
            for index in range(1, 4)
        ],
    }


class CompareBenchmarksTest(unittest.TestCase):
    def test_reports_steady_overhead_and_fault_impact_separately(self):
        steady = summary(12, verified=True)
        faults = summary(18)
        faults["chaos"] = {"seed": 42}
        faults["runs"].append({"run": 4, "status": "failed"})

        report = compare_benchmarks(summary(8), steady, faults)

        self.assertEqual(
            report["median_change_percent"]["steady_vs_baseline"]["duration_seconds"],
            50,
        )
        self.assertEqual(
            report["median_change_percent"]["faults_vs_steady"]["duration_seconds"],
            50,
        )
        self.assertIsNone(
            report["median_change_percent"]["steady_vs_baseline"][
                "first_result_seconds"
            ]
        )
        self.assertEqual(report["cases"]["faults"]["failed_runs"], [4])

    def test_rejects_a_successful_run_with_different_outputs(self):
        steady = summary(12, verified=True)
        baseline = deepcopy(steady)
        baseline["runs"][2]["fingerprints"]["query5.jsonl"]["records"] = 4

        with self.assertRaisesRegex(ValueError, "baseline run 3"):
            compare_benchmarks(baseline, steady)

    def test_rejects_different_datasets(self):
        steady = summary(12, verified=True)
        steady["dataset"]["sha256"] = "other-fixture"

        with self.assertRaisesRegex(ValueError, "steady: dataset differs"):
            compare_benchmarks(summary(8), steady)

    def test_requires_serial_validation_and_three_successful_runs(self):
        steady = summary(12)
        with self.assertRaisesRegex(ValueError, "serial reference"):
            compare_benchmarks(summary(8), steady)

        steady["runs"][0]["matches_expected"] = True
        steady["runs"][2]["status"] = "failed"
        with self.assertRaisesRegex(ValueError, "three valid runs"):
            compare_benchmarks(summary(8), steady)
