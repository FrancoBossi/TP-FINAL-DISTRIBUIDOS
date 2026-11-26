"""One FailureManager: Bully membership plus failure detection while it leads.

Two threads cooperate. The main thread owns the Bully node and never blocks on
the network, so heartbeats and election timers keep their schedule. While the
node is coordinator, a supervisor thread probes the targets and orders restarts;
it stops acting the moment leadership is lost or the epoch changes.
"""

import logging
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from common.health import LIVENESS, Liveness
from failure_manager.bully import BULLY_MESSAGE_TYPES, BullyNode
from failure_manager.detector import Detector, Suspect
from failure_manager.restarter import DockerRestarter, RestartError, RestartOrder

logger = logging.getLogger(__name__)

_MAX_WAIT_SECONDS = 0.1
_MAX_CONCURRENT_RESTARTS = 16


class FailureManager:
    def __init__(
        self,
        node: BullyNode,
        detector: Detector,
        restarter: DockerRestarter,
        ping_interval: float,
        clock: Callable[[], float] = time.monotonic,
        liveness: Liveness = LIVENESS,
    ) -> None:
        self._node = node
        self._detector = detector
        self._restarter = restarter
        self._ping_interval = ping_interval
        self._clock = clock
        self._liveness = liveness

        self._inbox: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1024)
        self._stopped = threading.Event()
        self._leadership_changed = threading.Event()
        self._lock = threading.Lock()
        self._leading_epoch: int | None = None
        self._in_flight: set[str] = set()
        self._restarts = ThreadPoolExecutor(
            max_workers=_MAX_CONCURRENT_RESTARTS, thread_name_prefix="restart"
        )
        self._supervisor = threading.Thread(
            target=self._supervise, name="fm-supervisor", daemon=True
        )

    def on_role_change(self, is_coordinator: bool, epoch: int) -> None:
        """Bully callback; runs on the main thread."""
        with self._lock:
            self._leading_epoch = epoch if is_coordinator else None

        self._leadership_changed.set()

    def on_message(self, message: dict[str, Any]) -> None:
        """Entry point for frames received by the health endpoint."""
        if message.get("type") not in BULLY_MESSAGE_TYPES:
            return

        try:
            self._inbox.put_nowait(message)
        except queue.Full:
            logger.warning("Dropping a Bully message: inbox is full")

    def stop(self) -> None:
        # Revoke local authority before waking or shutting down worker threads.
        # Otherwise an in-flight confirmation could still issue a restart while
        # this manager is already leaving the group.
        with self._lock:
            self._leading_epoch = None
        self._stopped.set()
        self._leadership_changed.set()

    def run(self) -> None:
        self._liveness.add_check("supervisor", self._supervisor.is_alive)
        self._supervisor.start()
        self._node.start(self._clock())

        try:
            while not self._stopped.is_set():
                self._step()
                self._liveness.tick()
        finally:
            self._stopped.set()
            self._leadership_changed.set()
            self._liveness.remove_check("supervisor")
            self._supervisor.join(timeout=5)
            self._restarts.shutdown(wait=False, cancel_futures=True)
            self._detector.close()

    def _step(self) -> None:
        now = self._clock()
        deadline = self._node.next_deadline()
        wait = _MAX_WAIT_SECONDS if deadline is None else deadline - now
        wait = min(max(wait, 0.0), _MAX_WAIT_SECONDS)

        try:
            message = self._inbox.get(timeout=wait)
        except queue.Empty:
            message = None

        while message is not None:
            self._node.handle(message, self._clock())

            try:
                message = self._inbox.get_nowait()
            except queue.Empty:
                message = None

        self._node.tick(self._clock())

    def _current_epoch(self) -> int | None:
        with self._lock:
            return self._leading_epoch

    def _supervise(self) -> None:
        last_epoch: int | None = None

        while not self._stopped.is_set():
            epoch = self._current_epoch()

            if epoch is None:
                last_epoch = None
                self._leadership_changed.wait(timeout=1.0)
                self._leadership_changed.clear()
                continue

            if epoch != last_epoch:
                logger.info("Supervising the targets as coordinator of epoch %s", epoch)
                self._detector.reset()
                last_epoch = epoch

            started = self._clock()

            try:
                suspects = self._detector.check_round()
            except Exception:
                logger.exception("Detection round failed")
                suspects = []

            for suspect in suspects:
                self._dispatch(suspect, epoch)

            remaining = self._ping_interval - (self._clock() - started)
            self._stopped.wait(timeout=max(remaining, 0.0))

    def _dispatch(self, suspect: Suspect, epoch: int) -> None:
        name = suspect.target.name

        with self._lock:
            if name in self._in_flight:
                return
            self._in_flight.add(name)

        try:
            self._restarts.submit(self._restart, suspect, epoch)
        except RuntimeError:
            with self._lock:
                self._in_flight.discard(name)

    def _restart(self, suspect: Suspect, epoch: int) -> None:
        name = suspect.target.name

        try:
            if self._current_epoch() != epoch:
                return

            if not self._detector.confirm(suspect):
                return

            # Leadership may have moved while the suspicion was being confirmed.
            if self._current_epoch() != epoch:
                return

            order = RestartOrder(
                target_id=suspect.target.container,
                incarnation_id=suspect.incarnation_id,
                coordinator_epoch=epoch,
            )
            detected_after = self._clock() - suspect.first_failure_at
            began = self._clock()

            try:
                self._restarter.restart(order)
            except RestartError as error:
                logger.error("%s", error)
            else:
                logger.info(
                    "%s: %s; detected after %.2fs, restart call took %.2fs",
                    name,
                    suspect.reason,
                    detected_after,
                    self._clock() - began,
                )
            finally:
                self._detector.mark_restart_attempted(suspect.target)
        finally:
            with self._lock:
                self._in_flight.discard(name)
