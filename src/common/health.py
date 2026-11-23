"""Health endpoint of a supervised process.

``Liveness`` records whether the consuming loop is making progress. A health
server answers ``PING`` with ``PONG`` carrying the component name, the
``incarnation_id`` of this process instance and a status. Supervisors decide
availability only from these answers: Docker's state is never consulted.
"""

import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from common.control_wire import ProtocolError, recv_frame, send_frame

logger = logging.getLogger(__name__)

STATUS_STARTING = "STARTING"
STATUS_OK = "OK"
STATUS_DEGRADED = "DEGRADED"

DEFAULT_HEALTH_PORT = 7000
TICK_INTERVAL_SECONDS = 0.1
_CONNECTION_TIMEOUT_SECONDS = 2.0


class Liveness:
    """Progress of the consuming loop, as seen from the health thread.

    The loop calls ``tick`` while it waits for deliveries and wraps every
    handler in ``busy``. A handler that takes long is not a failure by itself:
    it only degrades the process after ``busy_max_seconds``. Extra checks, such
    as the control thread being alive, are added with ``add_check``.
    """

    def __init__(
        self,
        idle_max_seconds: float = 5.0,
        busy_max_seconds: float = 120.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if idle_max_seconds <= 0 or busy_max_seconds <= 0:
            raise ValueError("Liveness thresholds must be greater than zero")

        self._idle_max_seconds = idle_max_seconds
        self._busy_max_seconds = busy_max_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._ready = False
        self._last_tick = clock()
        self._busy_depth = 0
        self._busy_since = 0.0
        self._checks: dict[str, Callable[[], bool]] = {}

    def mark_ready(self) -> None:
        with self._lock:
            self._ready = True
            self._last_tick = self._clock()

    def tick(self) -> None:
        with self._lock:
            self._last_tick = self._clock()

    @contextmanager
    def busy(self):
        with self._lock:
            if self._busy_depth == 0:
                self._busy_since = self._clock()
            self._busy_depth += 1
        try:
            yield
        finally:
            with self._lock:
                self._busy_depth -= 1
                if self._busy_depth == 0:
                    self._last_tick = self._clock()

    def add_check(self, name: str, check: Callable[[], bool]) -> None:
        with self._lock:
            self._checks[name] = check

    def remove_check(self, name: str) -> None:
        with self._lock:
            self._checks.pop(name, None)

    def status(self) -> tuple[str, str]:
        with self._lock:
            ready = self._ready
            last_tick = self._last_tick
            busy_depth = self._busy_depth
            busy_since = self._busy_since
            checks = list(self._checks.items())

        if not ready:
            return STATUS_STARTING, "starting"

        for name, check in checks:
            try:
                healthy = check()
            except Exception:
                healthy = False

            if not healthy:
                return STATUS_DEGRADED, f"check failed: {name}"

        now = self._clock()

        if busy_depth > 0:
            elapsed = now - busy_since
            if elapsed > self._busy_max_seconds:
                return STATUS_DEGRADED, f"handler running for {elapsed:.0f}s"
            return STATUS_OK, "busy"

        idle = now - last_tick
        if idle > self._idle_max_seconds:
            return STATUS_DEGRADED, f"no consumption progress for {idle:.0f}s"

        return STATUS_OK, "ok"


LIVENESS = Liveness(
    idle_max_seconds=float(os.environ.get("HEALTH_IDLE_MAX_SECONDS", "5")),
    busy_max_seconds=float(os.environ.get("HEALTH_BUSY_MAX_SECONDS", "120")),
)


class HealthServer:
    """TCP endpoint answering ``PING``; other frames go to an optional handler.

    A disabled server (no ``HEALTH_NAME`` in the environment) does nothing, so
    components still run outside the supervised deployment.
    """

    def __init__(
        self,
        name: str,
        port: int = DEFAULT_HEALTH_PORT,
        liveness: Liveness | None = None,
        host: str = "0.0.0.0",
        handler: Callable[[dict[str, Any]], None] | None = None,
        enabled: bool = True,
    ) -> None:
        self.name = name
        self.incarnation_id = uuid.uuid4().hex
        self._host = host
        self._port = port
        self._liveness = liveness if liveness is not None else LIVENESS
        self._handler = handler
        self._enabled = enabled
        self._server: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stopped = threading.Event()

    @classmethod
    def from_env(
        cls,
        handler: Callable[[dict[str, Any]], None] | None = None,
        liveness: Liveness | None = None,
    ) -> "HealthServer":
        name = os.environ.get("HEALTH_NAME", "")
        port = int(os.environ.get("HEALTH_PORT", str(DEFAULT_HEALTH_PORT)))
        return cls(
            name=name,
            port=port,
            liveness=liveness,
            handler=handler,
            enabled=bool(name),
        )

    @property
    def port(self) -> int:
        if self._server is None:
            return self._port
        return self._server.getsockname()[1]

    def set_ready(self) -> None:
        self._liveness.mark_ready()

    def start(self) -> None:
        if not self._enabled or self._server is not None:
            return

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self._host, self._port))
            server.listen(64)
            server.settimeout(0.5)
        except Exception:
            server.close()
            raise

        self._server = server
        self._thread = threading.Thread(
            target=self._accept_loop,
            name=f"health-{self.name}",
            daemon=True,
        )
        self._thread.start()
        logger.info("Health endpoint of %s listening on port %s", self.name, self.port)

    def close(self) -> None:
        self._stopped.set()

        if self._server is not None:
            self._server.close()
            self._server = None

        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _accept_loop(self) -> None:
        server = self._server
        while not self._stopped.is_set() and server is not None:
            try:
                connection, _address = server.accept()
            except socket.timeout:
                continue
            except OSError:
                return

            threading.Thread(
                target=self._serve_connection,
                args=(connection,),
                name=f"health-{self.name}-connection",
                daemon=True,
            ).start()

    def _serve_connection(self, connection: socket.socket) -> None:
        with connection:
            connection.settimeout(_CONNECTION_TIMEOUT_SECONDS)
            try:
                message = recv_frame(connection)

                if message["type"] == "PING":
                    send_frame(connection, self._pong(message))
                elif self._handler is not None:
                    self._handler(message)
            except (ProtocolError, EOFError, OSError):
                logger.debug("Dropped an invalid or interrupted control connection")
            except Exception:
                logger.exception("Control handler of %s failed", self.name)

    def _pong(self, ping_message: dict[str, Any]) -> dict[str, Any]:
        status, detail = self._liveness.status()
        return {
            "type": "PONG",
            "name": self.name,
            "incarnation_id": self.incarnation_id,
            "status": status,
            "detail": detail,
            "nonce": ping_message.get("nonce"),
        }


def ping(host: str, port: int, timeout: float, nonce: int = 0) -> dict[str, Any]:
    """Send one PING and return the matching PONG.

    The whole exchange is bounded by ``timeout`` seconds. Any failure to obtain a
    valid answer raises ``OSError``, ``EOFError`` or ``ProtocolError``.
    """
    deadline = time.monotonic() + timeout

    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.settimeout(max(0.01, deadline - time.monotonic()))
        send_frame(connection, {"type": "PING", "nonce": nonce})
        connection.settimeout(max(0.01, deadline - time.monotonic()))
        answer = recv_frame(connection)

    if answer["type"] != "PONG" or answer.get("nonce") != nonce:
        raise ProtocolError("Answer is not the PONG of this PING")

    return answer
