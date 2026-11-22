#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Inject scheduled SIGKILL failures into selected services in a Compose project."""

import argparse
import json
import math
import random
import signal
import subprocess
import time
from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from typing import TextIO

EXCLUDED_SERVICES = {
    "gateway",
    "rabbitmq",
    "client",
    "cadvisor",
    "prometheus",
    "grafana",
}
SIGNAL = "SIGKILL"


def failure_batches(
    targets: Iterable[str], seed: int, max_failures: int, batch_size: int
) -> Iterator[list[str]]:
    randomizer = random.Random(seed)
    candidates = sorted(set(targets))
    while max_failures:
        size = min(max_failures, batch_size, len(candidates))
        yield randomizer.sample(candidates, size)
        max_failures -= size


def is_failure_manager(service: str) -> bool:
    return service == "failure-manager" or service.startswith("failure-manager-")


def eligible_targets(
    planned: Iterable[str], running: Iterable[str]
) -> tuple[list[str], dict[str, str]]:
    running_services = set(running)
    managers = {service for service in running_services if is_failure_manager(service)}
    selected = []
    skipped = {}
    for service in planned:
        if service not in running_services:
            skipped[service] = "not_running"
        elif service in managers and len(managers) == 1:
            skipped[service] = "last_failure_manager"
        else:
            selected.append(service)
            managers.discard(service)
    return selected, skipped


def run_chaos(
    compose: Sequence[str],
    targets: Sequence[str],
    *,
    seed: int,
    interval: float,
    max_failures: int,
    batch_size: int,
    log: TextIO,
    stop: Event | None = None,
) -> None:
    def record(event: str, **details: object) -> None:
        entry = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "seed": seed,
            **details,
        }
        print(json.dumps(entry), file=log, flush=True)

    def run_compose(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*compose, *args], capture_output=True, text=True, timeout=15
        )

    config = run_compose("config", "--services")
    config.check_returncode()
    invalid = (set(targets) - set(config.stdout.split())) | (
        set(targets) & EXCLUDED_SERVICES
    )
    if invalid:
        raise ValueError("Unknown or excluded target(s): " + ", ".join(sorted(invalid)))

    batches = list(failure_batches(targets, seed, max_failures, batch_size))
    record(
        "schedule",
        interval_seconds=interval,
        signal=SIGNAL,
        batches=[
            {"offset_seconds": cycle * interval, "targets": batch}
            for cycle, batch in enumerate(batches, 1)
        ],
    )
    started = time.monotonic()
    for cycle, planned in enumerate(batches, 1):
        delay = started + cycle * interval - time.monotonic()
        if stop is None:
            if delay > 0:
                time.sleep(delay)
        elif stop.wait(max(0, delay)):
            break

        running = run_compose("ps", "--status", "running", "--services")
        if running.returncode:
            record("discovery_failed", cycle=cycle, stderr=running.stderr)
            running.check_returncode()
        if stop is not None and stop.is_set():
            break
        targets, skipped = eligible_targets(planned, running.stdout.split())
        record("selection", cycle=cycle, targets=targets, skipped=skipped)
        if not targets:
            continue

        record("injection_started", cycle=cycle, targets=targets, signal=SIGNAL)
        result = run_compose("kill", "--signal", SIGNAL, *targets)
        record(
            "injection_finished",
            cycle=cycle,
            targets=targets,
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        result.check_returncode()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-name", help="Override the project name resolved by Docker Compose"
    )
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docker-compose.yaml",
    )
    parser.add_argument("--target", action="append", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--max-failures", type=int, required=True)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--log", type=Path, required=True)
    args = parser.parse_args()
    if not math.isfinite(args.interval) or args.interval <= 0:
        parser.error("--interval must be finite and positive")
    if args.max_failures < 1 or args.batch_size < 1:
        parser.error("--max-failures and --batch-size must be positive")
    # Service names become positional arguments in the Compose kill command.
    if any(target.startswith("-") for target in args.target):
        parser.error("--target must name a Compose service")

    compose = ["docker", "compose", "--file", str(args.compose_file)]
    if args.project_name is not None:
        compose.extend(["--project-name", args.project_name])
    stop = Event()
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())
    try:
        with args.log.open("x", encoding="utf-8") as log:
            run_chaos(
                compose,
                args.target,
                seed=args.seed,
                interval=args.interval,
                max_failures=args.max_failures,
                batch_size=args.batch_size,
                log=log,
                stop=stop,
            )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Chaos Monkey: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
