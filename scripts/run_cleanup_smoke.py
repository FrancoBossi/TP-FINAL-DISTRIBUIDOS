#!/usr/bin/env python3
"""Verify distributed cleanup after a real client disconnects."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import time
from pathlib import Path

from common.message_protocol.external import recv_msg, send_msg
from common.message_protocol.message import AckMessage, ArticleMessage
from common.partition_router import PartitionRouter

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
EXECUTION_LOG = re.compile(r"Execution ([A-Za-z0-9._-]+): starting upload")

CONTROLLED_SERVICES = (
    *(f"query-router-{partition}" for partition in range(3)),
    *(f"until-2020-filter-{partition}" for partition in range(3)),
    *(f"q1-{partition}" for partition in range(3)),
    *(f"q2-biography-filter-{partition}" for partition in range(3)),
    *(f"image-fetcher-{partition}" for partition in range(2)),
    *(f"word-extractor-{partition}" for partition in range(3)),
    *(f"q3-word-projector-{partition}" for partition in range(3)),
    *(f"q3-word-aggregator-{partition}" for partition in range(3)),
    *(f"q4-q5-word-projector-{partition}" for partition in range(3)),
    *(f"q4-word-counter-{partition}" for partition in range(3)),
    *(f"q4-top20-reducer-{partition}" for partition in range(3)),
    *(f"q5-date-router-{partition}" for partition in range(3)),
    *(f"q5-stats-worker-{partition}" for partition in range(3)),
    *(f"q5-average-reducer-{partition}" for partition in range(3)),
    *(f"q5-comparator-{partition}" for partition in range(3)),
    "result-sink",
)

ARTICLES = (
    {
        "abstract": "A distributed systems article about durable messages.",
        "date_modified": "2019-01-10T12:00:00+00:00",
        "image": None,
        "in_language": {"identifier": "en"},
        "name": "Durable systems",
        "sections": '[{"name":"History"}]',
        "url": "https://example.com/durable-systems",
    },
    {
        "abstract": "Un article sur les systèmes distribués et les messages.",
        "date_modified": "2018-04-12T12:00:00+00:00",
        "image": None,
        "in_language": {"identifier": "fr"},
        "name": "Systèmes durables",
        "sections": '[{"name":"Histoire"}]',
        "url": "https://example.com/systemes-durables",
    },
    {
        "abstract": "An article modified after the historical cutoff.",
        "date_modified": "2023-06-01T12:00:00+00:00",
        "image": None,
        "in_language": {"identifier": "en"},
        "name": "Recent systems",
        "sections": '[{"name":"Overview"}]',
        "url": "https://example.com/recent-systems",
    },
    {
        "abstract": "Un article récent pour la cinquième requête.",
        "date_modified": "2024-02-20T12:00:00+00:00",
        "image": None,
        "in_language": {"identifier": "fr"},
        "name": "Systèmes récents",
        "sections": '[{"name":"Aperçu"}]',
        "url": "https://example.com/systemes-recents",
    },
)


def run(
    command: list[str],
    *,
    env: dict[str, str],
    check: bool = True,
    announce: bool = True,
) -> subprocess.CompletedProcess[str]:
    if announce:
        print("+", " ".join(command), flush=True)
    return subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=env,
        check=check,
        text=True,
        capture_output=True,
    )


def compose(
    arguments: list[str],
    *,
    project_name: str,
    env: dict[str, str],
    check: bool = True,
    announce: bool = True,
) -> subprocess.CompletedProcess[str]:
    return run(
        [
            "docker",
            "compose",
            "--project-name",
            project_name,
            "--file",
            "docker-compose.yaml",
            *arguments,
        ],
        env=env,
        check=check,
        announce=announce,
    )


def wait_until(description: str, timeout: float, condition) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if condition():
                print(f"OK: {description}", flush=True)
                return
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            last_error = error
        time.sleep(1)
    detail = f": {last_error}" if last_error is not None else ""
    raise TimeoutError(f"Timeout waiting for {description}{detail}")


def execution_id_from_logs(project_name: str, env: dict[str, str]) -> str | None:
    result = compose(
        ["logs", "--no-color", "gateway"],
        project_name=project_name,
        env=env,
        announce=False,
    )
    matches = EXECUTION_LOG.findall(result.stdout)
    return matches[-1] if matches else None


def service_has_state(
    service: str,
    execution_id: str,
    project_name: str,
    env: dict[str, str],
) -> bool:
    result = compose(
        [
            "exec",
            "-T",
            service,
            "sh",
            "-c",
            f'test -n "$(find /state -type d -path "*/executions/{execution_id}" '
            '-print -quit)"',
        ],
        project_name=project_name,
        env=env,
        check=False,
        announce=False,
    )
    return result.returncode == 0


def service_is_clean(
    service: str,
    execution_id: str,
    project_name: str,
    env: dict[str, str],
) -> bool:
    command = (
        f'test -z "$(find /state -type d -path "*/executions/{execution_id}" '
        '-print -quit)" && '
        f'test -n "$(find /state -type f -path "*/tombstones/{execution_id}.json" '
        '-print -quit)"'
    )
    result = compose(
        ["exec", "-T", service, "sh", "-c", command],
        project_name=project_name,
        env=env,
        check=False,
        announce=False,
    )
    return result.returncode == 0


def queues_are_drained(
    execution_id: str, project_name: str, env: dict[str, str]
) -> bool:
    result = compose(
        [
            "exec",
            "-T",
            "rabbitmq",
            "rabbitmqctl",
            "list_queues",
            "name",
            "messages_ready",
            "messages_unacknowledged",
            "--formatter",
            "json",
        ],
        project_name=project_name,
        env=env,
        announce=False,
    )
    start = result.stdout.find("[")
    end = result.stdout.rfind("]")
    if start < 0 or end < start:
        raise ValueError("rabbitmqctl did not return a JSON array")
    queues = json.loads(result.stdout[start : end + 1])
    result_queue = f"results.{execution_id}"
    return all(
        queue["name"] != result_queue
        and queue["messages_ready"] == 0
        and queue["messages_unacknowledged"] == 0
        for queue in queues
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-name", default="wikipedia-cleanup-smoke")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep", action="store_true")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    env = os.environ.copy()
    env["GATEWAY_HOST_PORT"] = str(arguments.port)
    candidate_path: Path | None = None
    execution_id: str | None = None

    try:
        up_arguments = ["up", "--detach", "--wait", "--wait-timeout", "300"]
        if not arguments.skip_build:
            up_arguments.append("--build")
        compose(up_arguments, project_name=arguments.project_name, env=env)

        with socket.create_connection(
            (arguments.host, arguments.port), timeout=10
        ) as client_socket:
            client_socket.settimeout(10)
            for article in ARTICLES:
                send_msg(client_socket, ArticleMessage(article=article))
                response = recv_msg(client_socket)
                if not isinstance(response, AckMessage):
                    raise RuntimeError(
                        f"Gateway returned {type(response).__name__} instead of ACK"
                    )

            def find_execution() -> bool:
                nonlocal execution_id
                execution_id = execution_id_from_logs(arguments.project_name, env)
                return execution_id is not None

            wait_until("execution ID appears in Gateway logs", 30, find_execution)
            assert execution_id is not None
            input_partition = PartitionRouter("input_batcher.input", 3).partition_for(
                execution_id
            )
            input_service = f"input-batcher-{input_partition}"
            wait_until(
                f"{input_service} persists execution state",
                30,
                lambda: service_has_state(
                    input_service, execution_id, arguments.project_name, env
                ),
            )
            candidate_path = (
                REPOSITORY_ROOT
                / "q5_candidate_buffer"
                / f"{hashlib.sha256(execution_id.encode()).hexdigest()}.jsonl"
            )
            print(
                f"Disconnecting client for execution {execution_id} without INPUT_END",
                flush=True,
            )

        expected_services = (input_service, *CONTROLLED_SERVICES)
        wait_until(
            "all stateful replicas remove execution state and persist tombstones",
            arguments.timeout,
            lambda: all(
                service_is_clean(service, execution_id, arguments.project_name, env)
                for service in expected_services
            ),
        )
        wait_until(
            "RabbitMQ queues drain and the exclusive result queue disappears",
            arguments.timeout,
            lambda: queues_are_drained(execution_id, arguments.project_name, env),
        )
        if candidate_path.exists():
            raise RuntimeError(f"Q5 candidate buffer was not removed: {candidate_path}")

        print(
            f"PASS: cleanup completed for disconnected execution {execution_id}",
            flush=True,
        )
    except Exception:
        logs = compose(
            ["logs", "--no-color", "--tail", "200"],
            project_name=arguments.project_name,
            env=env,
            check=False,
        )
        print(logs.stdout, flush=True)
        print(logs.stderr, flush=True)
        raise
    finally:
        if not arguments.keep:
            compose(
                ["down", "--volumes", "--remove-orphans"],
                project_name=arguments.project_name,
                env=env,
                check=False,
            )


if __name__ == "__main__":
    main()
