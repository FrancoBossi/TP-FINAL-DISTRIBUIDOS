"""Sending of Bully messages between FailureManagers over TCP.

Every peer has its own sender thread and a small queue, so an unreachable peer
can never delay the election timers or the messages to the other peers. A full
queue drops its oldest message: Bully messages are periodic or retried, and only
the latest matter.
"""

import logging
import queue
import socket
import threading
from typing import Any

from common.control_wire import ProtocolError, send_frame

logger = logging.getLogger(__name__)

_QUEUE_SIZE = 32


class PeerTransport:
    def __init__(
        self,
        peers: dict[int, tuple[str, int]],
        own_id: int,
        connect_timeout: float = 0.5,
    ) -> None:
        self._peers = {i: address for i, address in peers.items() if i != own_id}
        self._connect_timeout = connect_timeout
        self._stopped = threading.Event()
        self._queues = {i: queue.Queue(maxsize=_QUEUE_SIZE) for i in self._peers}
        self._threads = [
            threading.Thread(
                target=self._run,
                args=(peer_id,),
                name=f"fm-send-{peer_id}",
                daemon=True,
            )
            for peer_id in self._peers
        ]

    def start(self) -> None:
        for thread in self._threads:
            thread.start()

    def close(self) -> None:
        self._stopped.set()

        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=2)

    def send(self, destination: int, message: dict[str, Any]) -> None:
        outbox = self._queues.get(destination)

        if outbox is None:
            return

        while True:
            try:
                outbox.put_nowait(message)
                return
            except queue.Full:
                try:
                    outbox.get_nowait()
                except queue.Empty:
                    pass

    def _run(self, peer_id: int) -> None:
        host, port = self._peers[peer_id]
        outbox = self._queues[peer_id]

        while not self._stopped.is_set():
            try:
                message = outbox.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                with socket.create_connection(
                    (host, port), timeout=self._connect_timeout
                ) as connection:
                    connection.settimeout(self._connect_timeout)
                    send_frame(connection, message)
            except (OSError, ProtocolError) as error:
                logger.debug("Could not reach FailureManager %s: %s", peer_id, error)
