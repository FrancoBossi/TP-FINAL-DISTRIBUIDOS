import logging
import os
from dataclasses import dataclass
from pathlib import Path

from common.health import DEFAULT_HEALTH_PORT
from failure_manager.bully import BullyTimeouts


def configure_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _positive_float(name: str, default: float) -> float:
    value = float(os.environ.get(name, str(default)))

    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")

    return value


def _positive_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))

    if value < 1:
        raise ValueError(f"{name} must be at least 1")

    return value


def parse_peers(raw: str) -> dict[int, tuple[str, int]]:
    """Parse ``0=host:port,1=host:port`` into ``{0: (host, port), ...}``."""
    peers: dict[int, tuple[str, int]] = {}

    for entry in filter(None, (part.strip() for part in raw.split(","))):
        identifier, _, address = entry.partition("=")
        host, _, port = address.rpartition(":")

        if not identifier.strip().isdigit() or not host or not port.isdigit():
            raise ValueError(f"Invalid FM_PEERS entry: {entry!r}")

        peers[int(identifier)] = (host, int(port))

    if not peers:
        raise ValueError("FM_PEERS must list every FailureManager")

    return peers


@dataclass(frozen=True)
class DetectionSettings:
    ping_interval: float = 1.0
    ping_timeout: float = 0.5
    ping_attempts: int = 2
    failure_threshold: int = 2
    restart_cooldown: float = 10.0
    startup_grace: float = 60.0
    initial_grace: float = 60.0

    def __post_init__(self) -> None:
        for name in (
            "ping_interval",
            "ping_timeout",
            "restart_cooldown",
            "startup_grace",
            "initial_grace",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero")

        if self.ping_attempts < 1 or self.failure_threshold < 1:
            raise ValueError("ping_attempts and failure_threshold must be at least 1")


class FailureManagerConfig:
    def __init__(self) -> None:
        self.manager_id = int(os.environ["FM_ID"])
        self.peers = parse_peers(os.environ["FM_PEERS"])

        if self.manager_id not in self.peers:
            raise ValueError("FM_ID must appear in FM_PEERS")

        self.health_port = int(os.environ.get("HEALTH_PORT", DEFAULT_HEALTH_PORT))
        self.state_dir = Path(os.environ.get("FM_STATE_DIR", "/state"))
        self.docker_socket = os.environ.get("FM_DOCKER_SOCKET", "/var/run/docker.sock")
        self.restart_kill_after = int(os.environ.get("FM_RESTART_KILL_AFTER", "0"))
        self.docker_timeout = _positive_float("FM_DOCKER_TIMEOUT", 30.0)

        self.bully = BullyTimeouts(
            heartbeat_interval=_positive_float("FM_HEARTBEAT_INTERVAL", 0.5),
            coordinator_timeout=_positive_float("FM_COORDINATOR_TIMEOUT", 2.0),
            election_timeout=_positive_float("FM_ELECTION_TIMEOUT", 0.75),
            announcement_timeout=_positive_float("FM_ANNOUNCEMENT_TIMEOUT", 2.0),
            startup_listen=_positive_float("FM_STARTUP_LISTEN", 1.0),
        )
        self.detection = DetectionSettings(
            ping_interval=_positive_float("FM_PING_INTERVAL", 1.0),
            ping_timeout=_positive_float("FM_PING_TIMEOUT", 0.5),
            ping_attempts=_positive_int("FM_PING_ATTEMPTS", 2),
            failure_threshold=_positive_int("FM_FAILURE_THRESHOLD", 2),
            restart_cooldown=_positive_float("FM_RESTART_COOLDOWN", 10.0),
            startup_grace=_positive_float("FM_STARTUP_GRACE", 60.0),
            initial_grace=_positive_float("FM_INITIAL_GRACE", 60.0),
        )

    @property
    def manager_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self.peers))
