"""Run one Compose fault scenario per service type, plus a simultaneous failure."""

import argparse
import json
import math
import re
import subprocess
from typing import Any

from scripts.chaos_benchmark import (
    REPO_ROOT,
    add_benchmark_arguments,
    benchmark_command,
    run_benchmark,
    target_arguments,
)
from scripts.chaos_monkey import EXCLUDED_SERVICES, is_failure_manager
from scripts.sweep_chaos_intervals import assess_interval

REPRESENTATIVE_TYPES = (
    "preprocessor",
    "input-batcher",
    "query-router",
    "q3-word-aggregator",
    "q4-top20-reducer",
    "q5-comparator",
    "result-sink",
)


def matrix_cases(
    services: list[str], scope: str
) -> tuple[dict[str, list[str]], dict[str, str]]:
    by_type: dict[str, list[str]] = {}
    managers = []
    for service in sorted(set(services)):
        if service in EXCLUDED_SERVICES:
            continue
        if is_failure_manager(service):
            managers.append(service)
        else:
            service_type = re.sub(r"-[0-9]+$", "", service)
            by_type.setdefault(service_type, []).append(service)

    types = REPRESENTATIVE_TYPES if scope == "representative" else sorted(by_type)
    cases = {name: by_type[name][:1] for name in types if name in by_type}
    pending = {
        name: "Service type absent from Compose"
        for name in types
        if name not in by_type
    }

    simultaneous = [
        by_type[name][0] for name in ("preprocessor", "query-router") if name in by_type
    ]
    if len(simultaneous) == 2:
        cases["simultaneous-workers"] = simultaneous
    else:
        pending["simultaneous-workers"] = "Requires Preprocessor and QueryRouter"
    if len(managers) >= 2:
        cases["failure-manager"] = managers[:1]
    else:
        pending["failure-manager"] = "Requires at least two FailureManagers"
    return cases, pending


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--scope", choices=("representative", "all"), default="representative"
    )
    parser.add_argument(
        "--list", action="store_true", help="Print the plan without starting Compose"
    )
    add_benchmark_arguments(
        parser, required=False, project_name="wikipedia-chaos-matrix"
    )
    parser.add_argument("--interval", type=float, default=5)


def run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if not args.list:
        if None in (
            args.dataset,
            args.expected_output,
            args.output,
            args.timeout_seconds,
        ):
            parser.error(
                "Runs require --dataset, --expected-output, --output and --timeout-seconds"
            )
        if any(
            not math.isfinite(value) or value <= 0
            for value in (args.interval, args.timeout_seconds)
        ):
            parser.error("Interval and timeout must be finite and positive")

    compose = [
        "docker",
        "compose",
        "--project-name",
        args.project_name,
        "--file",
        str(REPO_ROOT / "docker-compose.yaml"),
    ]
    try:
        services = subprocess.run(
            [*compose, "config", "--services"],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.splitlines()
        cases, pending = matrix_cases(services, args.scope)
        report: dict[str, Any] = {"cases": cases, "pending": pending, "results": {}}
        if args.list:
            print(json.dumps(report, indent=2))
            return 0

        output = args.output.resolve()
        output.mkdir(parents=True)
        report_path = output / "matrix.json"
        report["interval_seconds"] = args.interval
        report["seed"] = args.seed
        command = [*benchmark_command(args, 3), "--chaos-interval", str(args.interval)]
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        for case, targets in cases.items():
            directory = output / case
            exit_code, log = run_benchmark(
                [*command, *target_arguments(targets, len(targets))], directory, case
            )
            report["results"][case] = {
                "log": str(log),
                **assess_interval(
                    directory,
                    exit_code,
                    3,
                    set(targets),
                    # Coverage proves that each service type survives a real
                    # failure in every run. Repeated failures belong to the
                    # interval sweep, whose default remains two cycles.
                    minimum_confirmed_cycles=1,
                ),
            }
            report_path.write_text(
                json.dumps(report, indent=2) + "\n", encoding="utf-8"
            )
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Chaos matrix: {error}\n")

    print(f"Report: {report_path}")
    return (
        0
        if cases
        and not pending
        and all(result["passed"] for result in report["results"].values())
        else 1
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser)
    return run(parser.parse_args(), parser)


if __name__ == "__main__":
    raise SystemExit(main())
