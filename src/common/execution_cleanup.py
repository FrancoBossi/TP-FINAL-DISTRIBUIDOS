"""Durable cleanup for one replica's execution state."""

import math
import shutil
import time
from collections.abc import Callable

from common.execution_id import execution_cutoff
from common.message_protocol.message import ExecutionCleanupMessage, Message
from common.persistence import AtomicJsonStore, PersistenceLayout


class ExecutionCleanup:
    """Use on the same thread that owns the execution state and outbox."""

    def __init__(self, layout: PersistenceLayout, retention_seconds: float) -> None:
        if not math.isfinite(retention_seconds) or retention_seconds <= 0:
            raise ValueError("cleanup retention must be positive and finite")
        self._layout = layout
        self._retention_seconds = retention_seconds
        self._directory = layout.executions_path.with_name("tombstones")
        self._watermark_store = AtomicJsonStore(self._directory.parent / "retired.json")
        watermark = self._watermark_store.load()
        self._retired_through = watermark["through"] if watermark is not None else 0
        self._next_sweep = 0.0

    def _marker(self, execution_id: str) -> AtomicJsonStore:
        state_path = self._layout.state_path(execution_id)
        return AtomicJsonStore(self._directory / f"{state_path.parent.name}.json")

    def is_closed(self, execution_id: str) -> bool:
        """Check before applying work, including startup outbox replay."""
        return self._is_retired(execution_id) or self.has_marker(execution_id)

    def has_marker(self, execution_id: str) -> bool:
        return self._marker(execution_id).load() is not None

    def _is_retired(self, execution_id: str) -> bool:
        cutoff = execution_cutoff(execution_id)
        return cutoff is not None and cutoff <= self._retired_through

    def handle_message(
        self, message: Message, clear_local: Callable[[str], None]
    ) -> bool:
        """Handle a cleanup order or identify work for an already closed execution."""
        if isinstance(message, ExecutionCleanupMessage):
            self.cleanup(message.execution_id, clear_local)
            return True
        execution_id = getattr(message, "execution_id", None)
        return execution_id is not None and self.is_closed(execution_id)

    def cleanup(self, execution_id: str, clear_local: Callable[[str], None]) -> None:
        """Persist closure before deleting state. The local callback must be idempotent."""
        marker = self._marker(execution_id)
        state = marker.load()
        retired = self._is_retired(execution_id)
        if state is None and not retired:
            marker.save({"expires_at": None})
        elif state is not None and state["expires_at"] is not None:
            return

        clear_local(execution_id)
        directory = self._layout.state_path(execution_id).parent
        try:
            shutil.rmtree(directory)
        except FileNotFoundError:
            if directory.exists():
                raise
        if retired:
            marker.path.unlink(missing_ok=True)
        else:
            marker.save({"expires_at": time.time() + self._retention_seconds})

    def recover(self, clear_local: Callable[[str], None]) -> None:
        """Finish interrupted cleanups before restoring active executions."""
        for execution_id, state in self._markers():
            if state["expires_at"] is None:
                self.cleanup(execution_id, clear_local)
        self.sweep()

    def sweep(self) -> None:
        """Retire expired timed IDs and keep the rejection watermark across restarts."""
        now_monotonic = time.monotonic()
        if now_monotonic < self._next_sweep:
            return
        now = time.time()
        for execution_id, state in self._markers():
            cutoff = execution_cutoff(execution_id)
            expires_at = state["expires_at"]
            if cutoff is None or expires_at is None or max(cutoff, expires_at) > now:
                continue
            if cutoff > self._retired_through:
                self._watermark_store.save({"through": cutoff})
                self._retired_through = cutoff
            self._marker(execution_id).path.unlink(missing_ok=True)
        self._next_sweep = now_monotonic + 60

    def expire(self, is_drained: Callable[[str], bool]) -> None:
        """Remove old tombstones once the caller verifies no late work can arrive."""
        now = time.time()
        for execution_id, state in self._markers():
            expires_at = state["expires_at"]
            if (
                expires_at is not None
                and expires_at <= now
                and is_drained(execution_id)
            ):
                self._marker(execution_id).path.unlink(missing_ok=True)

    def _markers(self):
        for path in self._directory.glob("*.json"):
            state = self._marker(path.stem).load()
            if state is not None:
                yield path.stem, state
