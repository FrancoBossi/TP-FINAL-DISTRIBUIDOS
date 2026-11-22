"""Compare block 0, steady-state tolerance, and optional fault-run summaries."""

import argparse
import json
from pathlib import Path
from typing import Any

from scripts.run_benchmark import QUERY_FILES, summarize_runs


def output_identity(run: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    fingerprints = run["fingerprints"]
    return tuple(
        tuple(
            fingerprints[query][field]
            for field in ("records", "sum", "sum_squares", "xor")
        )
        for query in QUERY_FILES
    )


def compare_benchmarks(
    baseline: dict[str, Any],
    steady: dict[str, Any],
    faults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cases = {"baseline": baseline, "steady": steady}
    if faults is not None:
        cases["faults"] = faults

    dataset_hash = baseline["dataset"].get("sha256")
    if not dataset_hash:
        raise ValueError("Baseline summary needs a dataset SHA-256")
    if (
        baseline.get("chaos")
        or steady.get("chaos")
        or (faults is not None and not faults.get("chaos"))
    ):
        raise ValueError(
            "Supply fault-free baseline and steady-state summaries, then an optional chaos summary"
        )

    reference_run = next(
        (
            run
            for run in steady["runs"]
            if run["status"] == "success" and run.get("matches_expected") is True
        ),
        None,
    )
    if reference_run is None:
        raise ValueError(
            "Steady-state summary needs results verified against the serial reference"
        )
    reference = output_identity(reference_run)

    report = {}
    for label, summary in cases.items():
        dataset = summary["dataset"]
        if (
            dataset.get("sha256") != dataset_hash
            or dataset["rows"] != baseline["dataset"]["rows"]
        ):
            raise ValueError(f"{label}: dataset differs from baseline")
        valid = []
        failed = []
        for run in summary["runs"]:
            if run["status"] != "success":
                failed.append(run["run"])
                continue
            if (
                run.get("matches_expected") is False
                or run.get("matches_first_success") is False
                or output_identity(run) != reference
            ):
                raise ValueError(
                    f"{label} run {run['run']}: Q1–Q5 differ from reference"
                )
            valid.append(run)
        if len(valid) < 3:
            raise ValueError(
                f"{label}: need at least three valid runs, found {len(valid)}"
            )
        report[label] = {
            "commit": summary["commit"],
            "dirty": summary.get("dirty"),
            "valid_runs": len(valid),
            "failed_runs": failed,
            "metrics": summarize_runs(valid),
        }

    comparisons = {}
    pairs = [("steady_vs_baseline", "steady", "baseline")]
    if faults is not None:
        pairs.append(("faults_vs_steady", "faults", "steady"))
    for name, current, prior in pairs:
        deltas = {}
        for metric, stats in report[current]["metrics"].items():
            value = stats["median"]
            base = report[prior]["metrics"][metric]["median"]
            deltas[metric] = (
                None if value is None or not base else (value / base - 1) * 100
            )
        comparisons[name] = deltas

    return {
        "dataset_sha256": dataset_hash,
        "cases": report,
        "median_change_percent": comparisons,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("steady", type=Path)
    parser.add_argument("--faults", type=Path)
    args = parser.parse_args()
    try:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        steady = json.loads(args.steady.read_text(encoding="utf-8"))
        faults = (
            json.loads(args.faults.read_text(encoding="utf-8")) if args.faults else None
        )
        comparison = compare_benchmarks(baseline, steady, faults)
    except (OSError, KeyError, TypeError, ValueError) as error:
        parser.exit(1, f"Benchmark comparison: {error}\n")
    print(json.dumps(comparison, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
