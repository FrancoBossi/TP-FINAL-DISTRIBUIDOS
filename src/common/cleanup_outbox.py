from collections.abc import Callable, Iterable

from common.message_identity import build_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    ExecutionLifecycleMessage,
)
from common.persistence import AtomicJsonStore, PendingOutput, PersistenceLayout


class CleanupOutbox:
    """Persist the terminal decision and its outstanding replica destinations."""

    def __init__(
        self,
        layout: PersistenceLayout,
        destinations_for: Callable[[str], Iterable[str]],
        publish: Callable[[str, ExecutionCleanupMessage], None],
    ) -> None:
        self._layout = layout
        self._destinations_for = destinations_for
        self._publish = publish

    def prepare(self, terminal: ExecutionLifecycleMessage) -> None:
        execution_id = terminal.execution_id
        store = AtomicJsonStore(self._layout.state_path(execution_id))
        if store.load() is not None:
            return
        cleanup = ExecutionCleanupMessage(
            execution_id=execution_id,
            message_id=build_message_id(
                execution_id=execution_id,
                producer_id="result_sink",
                operation="execution_cleanup",
                identity={},
            ),
            cause=terminal.cause,
        )
        destinations = dict.fromkeys(self._destinations_for(execution_id))
        store.save(
            {
                "pending": [
                    PendingOutput.from_message(destination, cleanup).to_record()
                    for destination in destinations
                ],
            }
        )

    def drain(self, execution_id: str) -> None:
        path = self._layout.state_path(execution_id)
        store = AtomicJsonStore(path)
        state = store.load()
        if state is None:
            return
        pending = state["pending"]
        while pending:
            output = PendingOutput.from_record(pending[0])
            message = output.deserialize_message()
            if (
                not isinstance(message, ExecutionCleanupMessage)
                or message.execution_id != execution_id
            ):
                raise ValueError("invalid cleanup outbox message")
            self._publish(output.destination, message)
            pending = pending[1:]
            store.save({"pending": pending})
        path.unlink()
        path.parent.rmdir()

    def execution_ids(self) -> tuple[str, ...]:
        return self._layout.execution_ids()
