"""Measure which fault intervals complete reproducibly in Compose."""

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any


def confirmed_cycles(log: Path) -> int:
    if not log.is_file():
        return 0
    with log.open(encoding="utf-8") as lines:
        events = (json.loads(line) for line in lines if line.endswith("\n"))
        return len(
            {
                event["cycle"]
                for event in events
                if event["event"] == "injection_finished" and event["returncode"] == 0
            }
        )


def assess_interval(
    directory: Path, exit_code: int, expected_runs: int
) -> dict[str, Any]:
    summary_path = next(directory.glob("*/summary.json"), None)
    runs = []
    if summary_path is not None:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        for run in summary["runs"]:
            cycles = confirmed_cycles(
                summary_path.parent / f"run-{run['run']:02d}" / "chaos.jsonl"
            )
            runs.append(
                {
                    "run": run["run"],
                    "status": run["status"],
                    "matches_expected": run.get("matches_expected"),
                    "cleanup_passed": run.get("cleanup", {}).get("passed"),
                    "confirmed_cycles": cycles,
                }
            )

    return {
        "exit_code": exit_code,
        "summary": str(summary_path) if summary_path is not None else None,
        "runs": runs,
        "passed": exit_code == 0
        and len(runs) == expected_runs
        and all(
            run["status"] == "success"
            and run["matches_expected"] is True
            and run["cleanup_passed"] is True
            and run["confirmed_cycles"] >= 2
            for run in runs
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--expected-output", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, required=True, help="New evidence directory"
    )
    parser.add_argument("--target", action="append", required=True)
    parser.add_argument(
        "--interval",
        type=float,
        action="append",
        help="Repeat in descending order (default: 5, 2.5, 1)",
    )
    parser.add_argument("--project-name", default="wikipedia-chaos-sweep")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-failures", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    intervals = args.interval or [5.0, 2.5, 1.0]
    if any(
        not math.isfinite(interval) or interval <= 0 for interval in intervals
    ) or any(left <= right for left, right in zip(intervals, intervals[1:])):
        parser.error(
            "--interval values must be finite, positive and strictly descending"
        )
    if args.runs < 3:
        parser.error("--runs must be at least 3")
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")
    if args.batch_size < 1 or args.max_failures <= min(
        args.batch_size, len(set(args.target))
    ):
        parser.error("The fault budget must allow at least two injection cycles")

    repo = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    command = [
        sys.executable,
        str(repo / "scripts" / "run_benchmark.py"),
        "--dataset",
        str(args.dataset.resolve()),
        "--expected-output",
        str(args.expected_output.resolve()),
        "--project-name",
        args.project_name,
        "--runs",
        str(args.runs),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--chaos-seed",
        str(args.seed),
        "--chaos-max-failures",
        str(args.max_failures),
        "--chaos-batch-size",
        str(args.batch_size),
    ]
    for target in args.target:
        command.extend(["--chaos-target", target])
    if args.skip_build:
        command.append("--skip-build")

    report: dict[str, Any] = {
        "benchmark_command": command,
        "planned_intervals_seconds": intervals,
        "minimum_passing_interval_seconds": None,
        "attempts": [],
    }
    try:
        output.mkdir(parents=True)
        for index, interval in enumerate(intervals, 1):
            directory = output / f"{index:02d}-{interval:g}s"
            directory.mkdir()
            log = directory / "benchmark.log"
            print(f"Testing {interval:g}s. Logs: {log}", flush=True)
            with log.open("wb") as capture:
                result = subprocess.run(
                    [
                        *command,
                        "--chaos-interval",
                        str(interval),
                        "--results-root",
                        str(directory),
                    ],
                    cwd=repo,
                    stdout=capture,
                    stderr=subprocess.STDOUT,
                )
            assessment = assess_interval(directory, result.returncode, args.runs)
            report["attempts"].append(
                {"interval_seconds": interval, "log": str(log), **assessment}
            )
            if assessment["passed"]:
                report["minimum_passing_interval_seconds"] = interval
            report_path = output / "sweep.json"
            report_path.write_text(
                json.dumps(report, indent=2) + "\n", encoding="utf-8"
            )
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"Chaos interval sweep: {error}\n")

    minimum = report["minimum_passing_interval_seconds"]
    if minimum is None:
        print(f"No tested interval passed. Report: {report_path}", file=sys.stderr)
        return 1
    print(f"Smallest passing tested interval: {minimum:g}s. Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
