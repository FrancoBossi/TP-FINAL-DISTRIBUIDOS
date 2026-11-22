from collections.abc import Iterator
from time import time

from common.persistence import AtomicJsonStore, PersistenceLayout

# Give Gateway time to notify the client and publish the terminal event first.
_TERMINAL_GRACE_SECONDS = 5


class ActiveExecutions:
    """Track accepted sessions until their terminal decision is durable."""

    def __init__(self, layout: PersistenceLayout) -> None:
        self._layout = layout
        self._directory = layout.executions_path.with_name("active")
        self._deadlines: dict[str, int] = {}
        self._scheduled: set[str] = set()

    def _store(self, execution_id: str) -> AtomicJsonStore:
        name = self._layout.state_path(execution_id).parent.name
        return AtomicJsonStore(self._directory / f"{name}.json")

    def register(self, execution_id: str, expires_at: int) -> None:
        if type(expires_at) is not int or expires_at <= 0:
            raise ValueError("execution deadline must be a positive Unix timestamp")
        previous = self._deadlines.get(execution_id)
        if previous is not None:
            if previous != expires_at:
                raise ValueError("execution startup events disagree on deadline")
            return
        self._store(execution_id).save({"expires_at": expires_at})
        self._deadlines[execution_id] = expires_at

    def remove(self, execution_id: str) -> None:
        self._store(execution_id).path.unlink(missing_ok=True)
        self._deadlines.pop(execution_id, None)
        self._scheduled.discard(execution_id)

    def recover(self) -> None:
        for path in self._directory.glob("*.json"):
            state = self._store(path.stem).load()
            if state is not None:
                self._deadlines[path.stem] = state["expires_at"]

    def expired(self) -> Iterator[str]:
        threshold = time() - _TERMINAL_GRACE_SECONDS
        return (
            execution_id
            for execution_id, deadline in tuple(self._deadlines.items())
            if deadline <= threshold and execution_id not in self._scheduled
        )

    def mark_scheduled(self, execution_id: str) -> None:
        self._scheduled.add(execution_id)
