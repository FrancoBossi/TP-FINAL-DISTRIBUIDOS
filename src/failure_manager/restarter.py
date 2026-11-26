"""Restart of a container through the Docker Engine API on its unix socket.

Docker is used for nothing else: no listing, no inspection, no events. The only
call is a restart of a container named in the static topology, which is safe to
repeat. Restarting a stopped container starts it, so the same call also revives
a process that died.
"""

import http.client
import logging
import socket
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import quote

logger = logging.getLogger(__name__)


class RestartError(Exception):
    """The restart could not be carried out."""


@dataclass(frozen=True)
class RestartOrder:
    """What the coordinator decided, and under which authority.

    ``incarnation_id`` is the instance that was found failing and
    ``coordinator_epoch`` the epoch of the coordinator that decided.
    """

    target_id: str
    incarnation_id: str | None
    coordinator_epoch: int


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self._socket_path)
        except OSError:
            sock.close()
            raise
        self.sock = sock


class DockerRestarter:
    def __init__(
        self,
        socket_path: str,
        allowed_containers: Iterable[str],
        kill_after_seconds: int = 0,
        timeout: float = 30.0,
    ) -> None:
        if kill_after_seconds < 0:
            raise ValueError("kill_after_seconds cannot be negative")

        self._socket_path = socket_path
        self._allowed = frozenset(allowed_containers)
        self._kill_after_seconds = kill_after_seconds
        self._timeout = timeout

    def restart(self, order: RestartOrder) -> None:
        if order.target_id not in self._allowed:
            raise RestartError(f"{order.target_id} is not in the static topology")

        path = f"/containers/{quote(order.target_id, safe='')}/restart"
        path += f"?t={self._kill_after_seconds}"
        connection = _UnixHTTPConnection(self._socket_path, self._timeout)

        try:
            connection.request("POST", path)
            response = connection.getresponse()
            body = response.read()
        except (OSError, http.client.HTTPException) as error:
            raise RestartError(
                f"Docker restart of {order.target_id} failed: {error}"
            ) from error
        finally:
            connection.close()

        if response.status != 204:
            detail = body.decode("utf-8", errors="replace").strip()[:200]
            raise RestartError(
                f"Docker restart of {order.target_id} answered {response.status}: {detail}"
            )

        logger.info(
            "Restarted %s (incarnation %s, epoch %s)",
            order.target_id,
            order.incarnation_id,
            order.coordinator_epoch,
        )
