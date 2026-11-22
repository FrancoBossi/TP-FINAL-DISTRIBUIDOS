from collections import Counter

from common.execution_cleanup import ExecutionCleanup
from common.message_identity import build_control_message_id, build_message_id
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    Q4WordBatchMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from q4.word_counter.messaging import Ack, Nack, Q4WordCounterMessaging

TOP = 20


class Q4WordCounterController:
    def __init__(
        self,
        messaging: Q4WordCounterMessaging,
        partition_id: int = 0,
        projector_partition_count: int = 1,
        persistence_layout: PersistenceLayout | None = None,
        output_destination: str = "q4_top20_reducer.input",
    ) -> None:
        if partition_id < 0:
            raise ValueError("partition_id must not be negative")
        if projector_partition_count <= 0:
            raise ValueError("projector_partition_count must be positive")
        self._messaging = messaging
        self._partition_id = partition_id
        self._projector_partition_count = projector_partition_count
        self._counts: dict[str, Counter[str]] = {}
        self._finished_projector_partitions: dict[str, set[int]] = {}
        self._persistence_layout = persistence_layout
        self._cleanup = (
            ExecutionCleanup(persistence_layout, retention_seconds=3600)
            if persistence_layout is not None
            else None
        )
        self._output_destination = output_destination
        self._stores: dict[str, DurableExecutionStore] = {}

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        if self._cleanup is not None:
            try:
                handled = self._cleanup.handle_message(message, self._clear_execution)
            except Exception:
                nack()
                raise
            if handled:
                ack()
                return
            self._process_durable(message, ack, nack)
            return
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

    def _process_durable(self, message: Message, ack: Ack, nack: Nack) -> None:
        if not isinstance(message, (Q4WordBatchMessage, PartitionEndMessage)):
            nack()
            raise ValueError("Q4WordCounter expected Q4_WORD_BATCH or PARTITION_END")
        try:
            if isinstance(message, PartitionEndMessage):
                if message.stage != "q4_q5_word_projector":
                    raise ValueError(
                        "Q4WordCounter received an invalid partition stage"
                    )
                if not 0 <= message.partition_id < self._projector_partition_count:
                    raise ValueError(
                        "Q4 projector partition id is outside configured range"
                    )
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            if state["completed"]:
                raise PersistenceCorruptionError("Q4WordCounter input after completion")
            outputs = []
            if isinstance(message, Q4WordBatchMessage):
                for word in message.words:
                    state["counts"][word] = state["counts"].get(word, 0) + 1
            else:
                state["finished_partitions"] = sorted(
                    {*state["finished_partitions"], message.partition_id}
                )
                if len(state["finished_partitions"]) == self._projector_partition_count:
                    state["completed"] = True
                    top = sorted(
                        state["counts"].items(), key=lambda item: (-item[1], item[0])
                    )[:TOP]
                    outputs = [
                        Q4PartialTopBatchMessage(
                            [{"word": word, "count": count} for word, count in top],
                            message.execution_id,
                            message.reply_queue,
                            build_message_id(
                                execution_id=message.execution_id,
                                producer_id="q4_word_counter",
                                operation="partial_top",
                                identity={"partition_id": self._partition_id},
                            ),
                        ),
                        Q4PartitionEndMessage(
                            self._partition_id,
                            message.execution_id,
                            message.reply_queue,
                            build_control_message_id(
                                execution_id=message.execution_id,
                                stage="q4_word_counter",
                                partition_id=self._partition_id,
                                operation="q4_partition_end",
                            ),
                        ),
                    ]
            store.apply(
                message.message_id,
                state,
                [
                    PendingOutput.from_message(self._output_destination, item)
                    for item in outputs
                ],
            )
            self._publish_pending(store)
        except Exception:
            nack()
            raise
        ack()

    def _decode_state(self, state):
        if not state:
            return {"counts": {}, "finished_partitions": [], "completed": False}
        if set(state) != {"counts", "finished_partitions", "completed"}:
            raise PersistenceCorruptionError("invalid Q4WordCounter state")
        counts, finished, completed = (
            state["counts"],
            state["finished_partitions"],
            state["completed"],
        )
        if (
            not isinstance(counts, dict)
            or any(
                not isinstance(word, str) or type(count) is not int or count < 0
                for word, count in counts.items()
            )
            or not isinstance(finished, list)
            or finished != sorted(set(finished))
            or any(
                type(item) is not int or not 0 <= item < self._projector_partition_count
                for item in finished
            )
            or type(completed) is not bool
            or completed != (len(finished) == self._projector_partition_count)
        ):
            raise PersistenceCorruptionError("invalid Q4WordCounter state")
        return state

    def _store_for(self, execution_id):
        if execution_id not in self._stores:
            self._stores[execution_id] = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
        return self._stores[execution_id]

    def _publish_pending(self, store):
        for output in store.pending_outputs:
            if output.destination != self._output_destination:
                raise PersistenceCorruptionError("invalid Q4WordCounter destination")
            message = output.deserialize_message()
            if not isinstance(
                message, (Q4PartialTopBatchMessage, Q4PartitionEndMessage)
            ):
                raise PersistenceCorruptionError("invalid Q4WordCounter outbox")
            self._messaging.publish(message)
            store.confirm(output)

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)
        self._counts.pop(execution_id, None)
        self._finished_projector_partitions.pop(execution_id, None)

    def _recover(self):
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._persistence_layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _handle_message(self, message: Message) -> None:
        if isinstance(message, Q4WordBatchMessage):
            self._counts.setdefault(message.execution_id, Counter()).update(
                message.words
            )
            return

        if isinstance(message, PartitionEndMessage):
            if message.stage != "q4_q5_word_projector":
                raise ValueError("Q4WordCounter received an invalid partition stage")
            if not 0 <= message.partition_id < self._projector_partition_count:
                raise ValueError(
                    "Q4 projector partition id is outside configured range"
                )
            finished = self._finished_projector_partitions.setdefault(
                message.execution_id, set()
            )
            finished.add(message.partition_id)
            if len(finished) < self._projector_partition_count:
                return
            counts = self._counts.get(message.execution_id, Counter())
            top = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:TOP]
            self._messaging.publish(
                Q4PartialTopBatchMessage(
                    words=[{"word": word, "count": count} for word, count in top],
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                    message_id=build_message_id(
                        execution_id=message.execution_id,
                        producer_id="q4_word_counter",
                        operation="partial_top",
                        identity={"partition_id": self._partition_id},
                    ),
                )
            )
            self._messaging.publish(
                Q4PartitionEndMessage(
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                    message_id=build_control_message_id(
                        execution_id=message.execution_id,
                        stage="q4_word_counter",
                        partition_id=self._partition_id,
                        operation="q4_partition_end",
                    ),
                )
            )
            self._counts.pop(message.execution_id, None)
            self._finished_projector_partitions.pop(message.execution_id)
            return

        raise ValueError("Q4WordCounter expected Q4_WORD_BATCH or PARTITION_END")

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
