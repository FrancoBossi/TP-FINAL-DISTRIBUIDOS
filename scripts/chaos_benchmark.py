"""Shared benchmark invocation for chaos scenarios."""

import argparse
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def add_benchmark_arguments(
    parser: argparse.ArgumentParser, *, required: bool, project_name: str
) -> None:
    parser.add_argument("--dataset", type=Path, required=required)
    parser.add_argument("--expected-output", type=Path, required=required)
    parser.add_argument(
        "--output", type=Path, required=required, help="New evidence directory"
    )
    parser.add_argument("--timeout-seconds", type=float, required=required)
    parser.add_argument("--project-name", default=project_name)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-build", action="store_true")


def benchmark_command(args: argparse.Namespace, runs: int) -> list[str]:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "run_benchmark.py"),
        "--dataset",
        str(args.dataset.resolve()),
        "--expected-output",
        str(args.expected_output.resolve()),
        "--project-name",
        args.project_name,
        "--runs",
        str(runs),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--chaos-seed",
        str(args.seed),
    ]
    if args.skip_build:
        command.append("--skip-build")
    return command


def target_arguments(targets: Sequence[str], batch_size: int) -> list[str]:
    options = ["--chaos-batch-size", str(batch_size)]
    for target in targets:
        options.extend(["--chaos-target", target])
    return options


def run_benchmark(command: list[str], directory: Path, label: str) -> tuple[int, Path]:
    directory.mkdir()
    log = directory / "benchmark.log"
    print(f"Testing {label}. Logs: {log}", flush=True)
    with log.open("wb") as capture:
        result = subprocess.run(
            [*command, "--results-root", str(directory)],
            cwd=REPO_ROOT,
            stdout=capture,
            stderr=subprocess.STDOUT,
        )
    return result.returncode, log
