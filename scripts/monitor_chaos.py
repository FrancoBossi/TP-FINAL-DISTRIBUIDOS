#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Read or follow the events recorded by Chaos Monkey."""

import argparse
import json
import os
import signal
import subprocess
import sys
from collections.abc import Iterator
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any

SKIP_REASONS = {
    "not_running": "Compose did not list the service as running",
    "last_failure_manager": "kept the last running FailureManager alive",
}


def read_lines(path: Path, follow: bool) -> Iterator[str]:
    if not follow:
        with path.open(encoding="utf-8") as log:
            yield from log
        return

    command = [
        "tail",
        "-n",
        "+1",
        "-F",
        f"--pid={os.getpid()}",
        "--",
        str(path.absolute()),
    ]
    with subprocess.Popen(command, stdout=subprocess.PIPE, encoding="utf-8") as tail:
        try:
            assert tail.stdout is not None
            yield from tail.stdout
            if tail.wait() != 0:
                raise OSError(f"tail exited with status {tail.returncode}")
        finally:
            if tail.poll() is None:
                tail.terminate()


def format_event(event: dict[str, Any]) -> Iterator[str]:
    timestamp = datetime.fromisoformat(event["timestamp"]).strftime("%H:%M:%S")
    kind = event["event"]
    if kind == "schedule":
        yield (
            f"{timestamp} SCHEDULE seed={event['seed']} "
            f"interval={event['interval_seconds']}s signal={event['signal']}"
        )
        for cycle, batch in enumerate(event["batches"], 1):
            targets = ", ".join(batch["targets"])
            yield f"  cycle {cycle}, +{batch['offset_seconds']}s: {targets}"
        return

    prefix = f"{timestamp} cycle {event.get('cycle', '?')}"
    targets = ", ".join(event.get("targets", []))
    if kind == "selection":
        if targets:
            yield (f"{prefix} SELECTED {targets} (scheduled by seed {event['seed']})")
        for service, reason in event["skipped"].items():
            yield f"{prefix} SKIPPED {service}: {SKIP_REASONS.get(reason, reason)}"
    elif kind == "injection_started":
        yield f"{prefix} STOP REQUESTED {targets} ({event['signal']})"
    elif kind == "injection_finished":
        code = event["returncode"]
        if code == 0:
            yield f"{prefix} STOP CONFIRMED {targets}"
        else:
            yield f"{prefix} STOP FAILED for batch {targets} (Compose exit {code})"
            for stream in ("stdout", "stderr"):
                for line in event.get(stream, "").splitlines():
                    yield f"  {line}"
    elif kind == "discovery_failed":
        yield f"{prefix} SERVICE DISCOVERY FAILED"
        for line in event.get("stderr", "").splitlines():
            yield f"  {line}"
    else:
        yield f"{prefix} {kind}: {json.dumps(event, ensure_ascii=False)}"


def main() -> int:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path, help="Path to chaos.jsonl")
    parser.add_argument(
        "-f", "--follow", action="store_true", help="Wait for new events until Ctrl+C"
    )
    args = parser.parse_args()
    try:
        with closing(read_lines(args.log, args.follow)) as lines:
            for line_number, line in enumerate(lines, 1):
                try:
                    for message in format_event(json.loads(line)):
                        print(message, flush=True)
                except (ValueError, TypeError, KeyError, AttributeError) as error:
                    print(f"{args.log}:{line_number}: {error}", file=sys.stderr)
    except KeyboardInterrupt:
        return 0
    except (OSError, UnicodeError) as error:
        parser.exit(1, f"Chaos monitor: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
