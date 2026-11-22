#!/usr/bin/env python3
"""Run reproducible end-to-end performance measurements with Docker Compose."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as parquet

QUERY_FILES = tuple(f"query{query_id}.jsonl" for query_id in range(1, 6))
FINGERPRINT_MODULUS = 1 << 256


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def multiset_fingerprint(path: Path) -> dict[str, Any]:
    """Return an order-independent fingerprint that preserves multiplicity."""
    count = 0
    hash_sum = 0
    hash_square_sum = 0
    hash_xor = 0

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                raise ValueError(f"Empty line in {path} at line {line_number}")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {path} at line {line_number}: {error}"
                ) from error
            if not isinstance(record, dict):
                raise ValueError(f"Expected an object in {path} at line {line_number}")
            normalized = json.dumps(
                record,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            value = int.from_bytes(hashlib.sha256(normalized).digest(), "big")
            count += 1
            hash_sum = (hash_sum + value) % FINGERPRINT_MODULUS
            hash_square_sum = (hash_square_sum + value * value) % FINGERPRINT_MODULUS
            hash_xor ^= value

    return {
        "records": count,
        "bytes": path.stat().st_size,
        "sum": f"{hash_sum:064x}",
        "sum_squares": f"{hash_square_sum:064x}",
        "xor": f"{hash_xor:064x}",
    }


def fingerprint_output(directory: Path) -> dict[str, dict[str, Any]]:
    fingerprints = {}
    for filename in QUERY_FILES:
        path = directory / filename
        if not path.is_file():
            raise FileNotFoundError(f"Missing benchmark output: {path}")
        fingerprints[filename] = multiset_fingerprint(path)
    return fingerprints


def parse_size_bytes(number_and_unit: str) -> int:
    number_and_unit = number_and_unit.strip()
    units = {
        "B": 1,
        "kB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    for unit in sorted(units, key=len, reverse=True):
        if number_and_unit.endswith(unit):
            number = float(number_and_unit[: -len(unit)].strip())
            return round(number * units[unit])
    raise ValueError(f"Unsupported Docker size value: {number_and_unit}")


def parse_memory_bytes(value: str) -> int:
    """Parse the used-memory side of Docker's 'usage / limit' value."""
    return parse_size_bytes(value.split("/", maxsplit=1)[0])


def parse_io_bytes(value: str) -> tuple[int, int]:
    """Parse Docker's 'received / transmitted' network I/O value."""
    received, transmitted = value.split("/", maxsplit=1)
    return parse_size_bytes(received), parse_size_bytes(transmitted)


def summarize_metrics(path: Path) -> dict[str, dict[str, float | int]]:
    by_container: dict[str, dict[str, list[float] | list[int]]] = defaultdict(
        lambda: {"cpu": [], "memory": [], "network_rx": [], "network_tx": []}
    )
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            sample = json.loads(line)
            if "error" in sample:
                continue
            name = sample.get("Name") or sample.get("Container") or "unknown"
            cpu_value = str(sample.get("CPUPerc", "0")).rstrip("%")
            memory_value = str(sample.get("MemUsage", "0B / 0B"))
            network_value = str(sample.get("NetIO", "0B / 0B"))
            try:
                by_container[name]["cpu"].append(float(cpu_value))
                by_container[name]["memory"].append(parse_memory_bytes(memory_value))
                received, transmitted = parse_io_bytes(network_value)
                by_container[name]["network_rx"].append(received)
                by_container[name]["network_tx"].append(transmitted)
            except ValueError:
                continue

    summary = {}
    for name, values in sorted(by_container.items()):
        cpu_values = values["cpu"]
        memory_values = values["memory"]
        network_rx_values = values["network_rx"]
        network_tx_values = values["network_tx"]
        if not cpu_values or not memory_values:
            continue
        summary[name] = {
            "samples": len(cpu_values),
            "cpu_percent_mean": sum(cpu_values) / len(cpu_values),
            "cpu_percent_peak": max(cpu_values),
            "memory_bytes_peak": max(memory_values),
            "network_receive_bytes_final": network_rx_values[-1],
            "network_transmit_bytes_final": network_tx_values[-1],
        }
    return summary


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    check: bool = True,
    capture_output: bool = False,
    announce: bool = True,
) -> subprocess.CompletedProcess[str]:
    if announce:
        print("+", " ".join(command), flush=True)
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=check,
        text=True,
        capture_output=capture_output,
    )


class MetricsSampler:
    def __init__(
        self,
        compose_command: list[str],
        cwd: Path,
        env: dict[str, str],
        output_path: Path,
        interval_seconds: float,
    ) -> None:
        self._compose_command = compose_command
        self._cwd = cwd
        self._env = env
        self._output_path = output_path
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="metrics-sampler")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()

    def _sample(self, output) -> None:
        try:
            containers = run_command(
                [*self._compose_command, "ps", "-q"],
                cwd=self._cwd,
                env=self._env,
                capture_output=True,
                announce=False,
            ).stdout.split()
            if not containers:
                raise RuntimeError("Docker Compose returned no running containers")
            result = run_command(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{json .}}",
                    *containers,
                ],
                cwd=self._cwd,
                env=self._env,
                capture_output=True,
                announce=False,
            )
            captured_at = utc_now()
            for line in result.stdout.splitlines():
                sample = json.loads(line)
                sample["captured_at"] = captured_at
                output.write(json.dumps(sample, sort_keys=True) + "\n")
        except Exception as error:  # Metrics must not terminate the benchmark.
            output.write(
                json.dumps({"captured_at": utc_now(), "error": str(error)}) + "\n"
            )
        output.flush()

    def _run(self) -> None:
        with self._output_path.open("w", encoding="utf-8") as output:
            while not self._stop.is_set():
                self._sample(output)
                self._stop.wait(self._interval_seconds)


def resolve_dataset(repo_root: Path, dataset: Path) -> tuple[Path, str]:
    dataset_path = dataset if dataset.is_absolute() else repo_root / dataset
    dataset_path = dataset_path.resolve()
    datasets_root = (repo_root / "datasets").resolve()
    try:
        relative = dataset_path.relative_to(datasets_root)
    except ValueError as error:
        raise ValueError(
            f"Dataset must be inside {datasets_root} for the client container"
        ) from error
    if not dataset_path.is_file():
        raise FileNotFoundError(f"Dataset does not exist: {dataset_path}")
    return dataset_path, f"/datasets/{relative.as_posix()}"


def dataset_metadata(dataset_path: Path, calculate_hash: bool) -> dict[str, Any]:
    parquet_file = parquet.ParquetFile(dataset_path)
    manifest_path = dataset_path.with_suffix(".manifest.json")
    metadata = {
        "path": str(dataset_path),
        "bytes": dataset_path.stat().st_size,
        "rows": parquet_file.metadata.num_rows,
        "row_groups": parquet_file.metadata.num_row_groups,
        "columns": parquet_file.schema_arrow.names,
    }
    if manifest_path.is_file():
        metadata["manifest_path"] = str(manifest_path)
        metadata["manifest_sha256"] = sha256_file(manifest_path)
        metadata["manifest"] = json.loads(manifest_path.read_text(encoding="utf-8"))
    if calculate_hash:
        print(f"Calculating SHA-256 for {dataset_path}...", flush=True)
        metadata["sha256"] = sha256_file(dataset_path)
    return metadata


def stop_process(process: subprocess.Popen[bytes]) -> int:
    if process.poll() is None:
        process.terminate()
        try:
            return process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
    return process.wait()


def count_injections(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open(encoding="utf-8") as log:
        events = (json.loads(line) for line in log if line.endswith("\n"))
        return sum(
            len(event["targets"])
            for event in events
            if event["event"] == "injection_finished" and event["returncode"] == 0
        )


def wait_for_process(
    process: subprocess.Popen[bytes],
    timeout_seconds: float | None,
    injector: subprocess.Popen[bytes] | None = None,
) -> tuple[int, bool]:
    started_at = time.monotonic()
    last_progress = started_at
    timed_out = False
    while process.poll() is None:
        now = time.monotonic()
        if timeout_seconds is not None and now - started_at >= timeout_seconds:
            timed_out = True
            break
        if injector is not None and injector.poll() not in (None, 0):
            break
        if now - last_progress >= 60:
            print(f"Benchmark client running for {(now - started_at) / 60:.1f} min")
            last_progress = now
        time.sleep(1)
    return stop_process(process), timed_out


def capture_compose_logs(
    compose_command: list[str], cwd: Path, env: dict[str, str], path: Path
) -> None:
    result = run_command(
        [*compose_command, "logs", "--no-color", "--timestamps"],
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
    )
    path.write_text(result.stdout + result.stderr, encoding="utf-8")


def git_value(repo_root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()


def write_summary_csv(path: Path, runs: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=(
                "run",
                "status",
                "exit_code",
                "duration_seconds",
                "articles_per_second",
                "matches_first_success",
                "matches_expected",
            ),
        )
        writer.writeheader()
        for run in runs:
            writer.writerow({field: run.get(field) for field in writer.fieldnames})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--results-root", type=Path, default=Path("benchmarks/results"))
    parser.add_argument("--expected-output", type=Path)
    parser.add_argument("--project-name", default="wikipedia-benchmark")
    parser.add_argument("--sample-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-dataset-hash", action="store_true")
    parser.add_argument("--chaos-target", action="append", default=[])
    parser.add_argument("--chaos-seed", type=int, default=42)
    parser.add_argument("--chaos-interval", type=float, default=5.0)
    parser.add_argument("--chaos-max-failures", type=int, default=8)
    parser.add_argument("--chaos-batch-size", type=int, default=1)
    args = parser.parse_args()
    if args.chaos_target:
        if args.expected_output is None or args.timeout_seconds is None:
            parser.error("chaos runs require --expected-output and --timeout-seconds")
        if not math.isfinite(args.timeout_seconds):
            parser.error("--timeout-seconds must be finite for chaos runs")
        if not math.isfinite(args.chaos_interval) or args.chaos_interval <= 0:
            parser.error("--chaos-interval must be finite and positive")
        if min(args.chaos_max_failures, args.chaos_batch_size) < 1:
            parser.error("chaos failure and batch limits must be positive")
    if args.runs <= 0:
        parser.error("--runs must be positive")
    if args.sample_seconds <= 0:
        parser.error("--sample-seconds must be positive")
    if args.timeout_seconds is not None and args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    return args


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    dataset_path, container_dataset = resolve_dataset(repo_root, args.dataset)
    expected_output = None
    if args.expected_output is not None:
        expected_output = (
            args.expected_output
            if args.expected_output.is_absolute()
            else repo_root / args.expected_output
        ).resolve()
        if not expected_output.is_dir():
            raise ValueError(f"Expected output is not a directory: {expected_output}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    commit = git_value(repo_root, "rev-parse", "HEAD")
    short_commit = commit[:12]
    results_root = (
        args.results_root
        if args.results_root.is_absolute()
        else repo_root / args.results_root
    ).resolve()
    benchmark_dir = results_root / f"{timestamp}-{short_commit}"
    benchmark_dir.mkdir(parents=True)

    compose_file = repo_root / "docker-compose.yaml"
    compose_command = [
        "docker",
        "compose",
        "--project-name",
        args.project_name,
        "--file",
        str(compose_file),
    ]
    base_env = os.environ.copy()
    base_env["COMPOSE_HTTP_TIMEOUT"] = "300"
    # All benchmark traffic uses the Compose network. Let Docker allocate host ports.
    for port in (
        "GATEWAY_HOST_PORT",
        "RABBITMQ_AMQP_HOST_PORT",
        "RABBITMQ_MANAGEMENT_HOST_PORT",
    ):
        base_env.setdefault(port, "0")

    metadata = {
        "created_at": utc_now(),
        "commit": commit,
        "branch": git_value(repo_root, "branch", "--show-current"),
        "dirty": bool(git_value(repo_root, "status", "--short")),
        "dataset": dataset_metadata(dataset_path, not args.skip_dataset_hash),
        "runs_requested": args.runs,
        "project_name": args.project_name,
        "sample_seconds": args.sample_seconds,
        "timeout_seconds": args.timeout_seconds,
        "docker_compose_version": run_command(
            ["docker", "compose", "version"],
            cwd=repo_root,
            env=base_env,
            capture_output=True,
        ).stdout.strip(),
    }
    if args.chaos_target:
        metadata["chaos"] = {
            "targets": args.chaos_target,
            "seed": args.chaos_seed,
            "interval_seconds": args.chaos_interval,
            "max_failures": args.chaos_max_failures,
            "batch_size": args.chaos_batch_size,
        }
    (benchmark_dir / "benchmark.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    expected_fingerprints = (
        fingerprint_output(expected_output) if expected_output is not None else None
    )
    if expected_fingerprints is not None:
        (benchmark_dir / "expected-fingerprints.json").write_text(
            json.dumps(expected_fingerprints, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    run_command(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        cwd=repo_root,
        env=base_env,
    )
    if not args.skip_build:
        run_command(
            [*compose_command, "--profile", "client", "build"],
            cwd=repo_root,
            env=base_env,
        )

    runs = []
    first_successful_fingerprints = None
    try:
        for run_number in range(1, args.runs + 1):
            run_dir = benchmark_dir / f"run-{run_number:02d}"
            output_dir = run_dir / "output"
            output_dir.mkdir(parents=True)
            run_env = base_env | {"DISTRIBUTED_OUTPUT_DIR": str(output_dir)}
            run_command(
                [*compose_command, "down", "--remove-orphans"],
                cwd=repo_root,
                env=run_env,
                check=False,
            )
            startup = run_command(
                [
                    *compose_command,
                    "up",
                    "--detach",
                    "--wait",
                    "--wait-timeout",
                    "300",
                    "--remove-orphans",
                ],
                cwd=repo_root,
                env=run_env,
                check=False,
                capture_output=True,
            )
            startup_log = run_dir / "startup.log"
            startup_log.write_text(startup.stdout + startup.stderr, encoding="utf-8")
            if startup.returncode:
                capture_compose_logs(
                    compose_command, repo_root, run_env, run_dir / "compose.log"
                )
                print(
                    f"Compose startup failed (exit {startup.returncode}). "
                    f"See {startup_log} and {run_dir / 'compose.log'}.",
                    file=sys.stderr,
                )
                print(startup.stderr.rstrip(), file=sys.stderr)
                return 1

            metrics_path = run_dir / "metrics.jsonl"
            sampler = MetricsSampler(
                compose_command,
                repo_root,
                run_env,
                metrics_path,
                args.sample_seconds,
            )
            client_command = [
                *compose_command,
                "--profile",
                "client",
                "run",
                "--rm",
                "--no-deps",
                "--env",
                f"INPUT_FILE={container_dataset}",
                "--env",
                "OUTPUT_DIR=/output",
                "client",
            ]
            print(f"Starting benchmark run {run_number}/{args.runs}")
            started_at = utc_now()
            monotonic_start = time.monotonic()
            chaos_events = run_dir / "chaos.jsonl"
            injector_status = None
            with (run_dir / "client.log").open("wb") as client_log:
                sampler.start()
                try:
                    process = subprocess.Popen(
                        client_command,
                        cwd=repo_root,
                        env=run_env,
                        stdout=client_log,
                        stderr=subprocess.STDOUT,
                    )
                    injector = None
                    try:
                        if args.chaos_target:
                            injector_command = [
                                sys.executable,
                                str(repo_root / "scripts" / "chaos_monkey.py"),
                                "--compose-file",
                                str(compose_file),
                                "--project-name",
                                args.project_name,
                                "--seed",
                                str(args.chaos_seed),
                                "--interval",
                                str(args.chaos_interval),
                                "--max-failures",
                                str(args.chaos_max_failures),
                                "--batch-size",
                                str(args.chaos_batch_size),
                                "--log",
                                str(chaos_events),
                            ]
                            for target in args.chaos_target:
                                injector_command.extend(["--target", target])
                            with (run_dir / "chaos.log").open("wb") as chaos_log:
                                injector = subprocess.Popen(
                                    injector_command,
                                    cwd=repo_root,
                                    env=run_env,
                                    stdout=chaos_log,
                                    stderr=subprocess.STDOUT,
                                )
                        exit_code, timed_out = wait_for_process(
                            process, args.timeout_seconds, injector
                        )
                    finally:
                        if injector is not None:
                            injector_status = stop_process(injector)
                        stop_process(process)
                finally:
                    sampler.stop()
            duration = time.monotonic() - monotonic_start
            capture_compose_logs(
                compose_command, repo_root, run_env, run_dir / "compose.log"
            )

            run_result: dict[str, Any] = {
                "run": run_number,
                "started_at": started_at,
                "finished_at": utc_now(),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "duration_seconds": duration,
                "articles_per_second": metadata["dataset"]["rows"] / duration,
                "status": "failed",
                "metrics": summarize_metrics(metrics_path),
            }
            chaos_ok = True
            if args.chaos_target:
                injected = count_injections(chaos_events)
                run_result["chaos"] = {
                    "exit_code": injector_status,
                    "confirmed_injections": injected,
                }
                chaos_ok = injected > 0 and injector_status == 0
            try:
                fingerprints = fingerprint_output(output_dir)
                run_result["fingerprints"] = fingerprints
                if expected_fingerprints is not None:
                    run_result["matches_expected"] = (
                        fingerprints == expected_fingerprints
                    )
                if (
                    exit_code == 0
                    and not timed_out
                    and chaos_ok
                    and run_result.get("matches_expected", True)
                ):
                    run_result["status"] = "success"
                    if first_successful_fingerprints is None:
                        first_successful_fingerprints = fingerprints
                    run_result["matches_first_success"] = (
                        fingerprints == first_successful_fingerprints
                    )
            except (OSError, ValueError) as error:
                run_result["output_error"] = str(error)

            (run_dir / "run.json").write_text(
                json.dumps(run_result, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            runs.append(run_result)
            run_command(
                [*compose_command, "down", "--remove-orphans"],
                cwd=repo_root,
                env=run_env,
                check=False,
            )
    finally:
        run_command(
            [*compose_command, "down", "--remove-orphans"],
            cwd=repo_root,
            env=base_env,
            check=False,
        )

    summary = metadata | {"runs": runs}
    (benchmark_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_summary_csv(benchmark_dir / "summary.csv", runs)
    print(f"Benchmark artifacts: {benchmark_dir}")

    success = all(run["status"] == "success" for run in runs)
    stable = all(run.get("matches_first_success") is not False for run in runs)
    expected_match = all(run.get("matches_expected") is not False for run in runs)
    return 0 if success and stable and expected_match else 1


if __name__ == "__main__":
    raise SystemExit(main())
