from collections.abc import Callable, Mapping

from common.execution_cleanup import ExecutionCleanup
from common.message_protocol.message import Message, PartitionEndMessage
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)

Publisher = Callable[[Message], None]
OutputFactory = Callable[[PartitionEndMessage], Mapping[str, Message]]


class DurableBarrier:
    """Persist one replicated control barrier and its completion fan-out."""

    def __init__(
        self,
        layout: PersistenceLayout,
        expected_partitions: int,
        publishers: Mapping[str, Publisher],
        output_factory: OutputFactory,
    ) -> None:
        if expected_partitions <= 0:
            raise ValueError("expected_partitions must be positive")
        if not publishers or any(not destination for destination in publishers):
            raise ValueError("durable barrier publishers require named destinations")
        self._layout = layout
        self._expected_partitions = expected_partitions
        self._publishers = dict(publishers)
        self._output_factory = output_factory
        self._cleanup = ExecutionCleanup(layout, retention_seconds=3600)
        self._stores: dict[str, DurableExecutionStore] = {}

    def is_closed(self, message: Message) -> bool:
        execution_id = getattr(message, "execution_id", None)
        return execution_id is not None and self._cleanup.is_closed(execution_id)

    def cleanup(self, execution_id: str) -> None:
        self._cleanup.cleanup(execution_id, self._clear_execution)

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)

    def apply(self, message: PartitionEndMessage) -> None:
        store = self._store_for(message.execution_id)
        finished, reply_queue, completed = self._decode_state(store.state)
        if store.has_applied(message.message_id):
            self._publish_pending(store)
            return
        if completed:
            raise PersistenceCorruptionError(
                "completed durable barrier is missing a control inbox entry"
            )
        if reply_queue is not None and reply_queue != message.reply_queue:
            raise ValueError("barrier controls disagree on reply_queue")

        finished.add(message.partition_id)
        completed = len(finished) == self._expected_partitions
        outputs: list[PendingOutput] = []
        if completed:
            produced = dict(self._output_factory(message))
            if set(produced) != set(self._publishers):
                raise ValueError("barrier output destinations do not match publishers")
            outputs = [
                PendingOutput.from_message(destination, output)
                for destination, output in produced.items()
            ]

        store.apply(
            message.message_id,
            {
                "finished_partitions": sorted(finished),
                "reply_queue": message.reply_queue,
                "completed": completed,
            },
            outputs,
        )
        self._publish_pending(store)

    def recover(self) -> None:
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _store_for(self, execution_id: str) -> DurableExecutionStore:
        store = self._stores.get(execution_id)
        if store is None:
            store = DurableExecutionStore(
                AtomicJsonStore(self._layout.state_path(execution_id))
            )
            self._stores[execution_id] = store
        return store

    def _decode_state(self, state: dict) -> tuple[set[int], str | None, bool]:
        if not state:
            return set(), None, False
        if set(state) != {"finished_partitions", "reply_queue", "completed"}:
            raise PersistenceCorruptionError("invalid durable barrier state")
        finished = state["finished_partitions"]
        reply_queue = state["reply_queue"]
        completed = state["completed"]
        if (
            not isinstance(finished, list)
            or any(type(partition) is not int for partition in finished)
            or finished != sorted(set(finished))
            or any(not 0 <= item < self._expected_partitions for item in finished)
            or not isinstance(reply_queue, str)
            or not reply_queue
            or type(completed) is not bool
            or completed != (len(finished) == self._expected_partitions)
        ):
            raise PersistenceCorruptionError("invalid durable barrier state")
        return set(finished), reply_queue, completed

    def _publish_pending(self, store: DurableExecutionStore) -> None:
        for output in store.pending_outputs:
            publisher = self._publishers.get(output.destination)
            if publisher is None:
                raise PersistenceCorruptionError(
                    "durable barrier contains an unexpected destination"
                )
            publisher(output.deserialize_message())
            store.confirm(output)
