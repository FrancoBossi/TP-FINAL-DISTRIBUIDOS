from dataclasses import dataclass, field

from common.execution_cleanup import ExecutionCleanup
from common.message_identity import build_result_message_ids
from common.message_protocol.message import (
    Message,
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    QueryEndMessage,
    ResultMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from q4.top20_reducer.messaging import Ack, Nack, Q4Top20ReducerMessaging

TOP = 20


@dataclass
class ExecutionState:
    candidates: dict[str, int] = field(default_factory=dict)
    ended_partitions: set[int] = field(default_factory=set)


class Q4Top20ReducerController:
    def __init__(
        self,
        messaging: Q4Top20ReducerMessaging,
        partition_count: int = 1,
        persistence_layout: PersistenceLayout | None = None,
        output_destination: str = "result_sink.input",
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")
        self._messaging = messaging
        self._partition_count = partition_count
        self._executions: dict[str, ExecutionState] = {}
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
        if not isinstance(message, (Q4PartialTopBatchMessage, Q4PartitionEndMessage)):
            nack()
            raise ValueError(
                "Q4Top20Reducer expected Q4_PARTIAL_TOP_BATCH or Q4_PARTITION_END"
            )
        try:
            if (
                isinstance(message, Q4PartitionEndMessage)
                and not 0 <= message.partition_id < self._partition_count
            ):
                raise ValueError("Q4 partition id is outside configured range")
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            if state["completed"]:
                raise PersistenceCorruptionError("Q4 reducer input after completion")
            outputs = []
            if isinstance(message, Q4PartialTopBatchMessage):
                for candidate in message.words:
                    word, count = candidate["word"], candidate["count"]
                    if not isinstance(word, str) or type(count) is not int:
                        raise ValueError("Invalid Q4 partial candidate")
                    state["candidates"][word] = state["candidates"].get(word, 0) + count
            else:
                state["ended_partitions"] = sorted(
                    {*state["ended_partitions"], message.partition_id}
                )
                if len(state["ended_partitions"]) == self._partition_count:
                    state["completed"] = True
                    top = sorted(
                        state["candidates"].items(),
                        key=lambda item: (-item[1], item[0]),
                    )[:TOP]
                    for word, count in top:
                        result_id, message_id = build_result_message_ids(
                            execution_id=message.execution_id,
                            query_id=4,
                            result_kind="result",
                            identity={"word": word},
                            producer_id="q4_top20_reducer",
                        )
                        outputs.append(
                            ResultMessage(
                                4,
                                {"word": word, "count": count},
                                message.execution_id,
                                message_id,
                                result_id,
                            )
                        )
                    result_id, message_id = build_result_message_ids(
                        execution_id=message.execution_id,
                        query_id=4,
                        result_kind="end",
                        identity={"end": True},
                        producer_id="q4_top20_reducer",
                    )
                    outputs.append(
                        QueryEndMessage(4, message.execution_id, message_id, result_id)
                    )
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
            return {"candidates": {}, "ended_partitions": [], "completed": False}
        if set(state) != {"candidates", "ended_partitions", "completed"}:
            raise PersistenceCorruptionError("invalid Q4Top20Reducer state")
        candidates, ended, completed = (
            state["candidates"],
            state["ended_partitions"],
            state["completed"],
        )
        if (
            not isinstance(candidates, dict)
            or any(
                not isinstance(word, str) or type(count) is not int or count < 0
                for word, count in candidates.items()
            )
            or not isinstance(ended, list)
            or ended != sorted(set(ended))
            or any(
                type(item) is not int or not 0 <= item < self._partition_count
                for item in ended
            )
            or type(completed) is not bool
            or completed != (len(ended) == self._partition_count)
        ):
            raise PersistenceCorruptionError("invalid Q4Top20Reducer state")
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
                raise PersistenceCorruptionError("invalid Q4Top20Reducer destination")
            message = output.deserialize_message()
            if not isinstance(message, (ResultMessage, QueryEndMessage)):
                raise PersistenceCorruptionError("invalid Q4Top20Reducer outbox")
            self._messaging.publish(message)
            store.confirm(output)

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)
        self._executions.pop(execution_id, None)

    def _recover(self):
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._persistence_layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _handle_message(self, message: Message) -> None:
        if isinstance(message, Q4PartialTopBatchMessage):
            state = self._executions.setdefault(message.execution_id, ExecutionState())
            for candidate in message.words:
                word = candidate["word"]
                count = candidate["count"]
                if not isinstance(word, str) or not isinstance(count, int):
                    raise ValueError("Invalid Q4 partial candidate")
                state.candidates[word] = state.candidates.get(word, 0) + count
            return

        if isinstance(message, Q4PartitionEndMessage):
            state = self._executions.setdefault(message.execution_id, ExecutionState())
            if not 0 <= message.partition_id < self._partition_count:
                raise ValueError("Q4 partition id is outside configured range")
            state.ended_partitions.add(message.partition_id)
            if len(state.ended_partitions) != self._partition_count:
                return

            top = sorted(
                state.candidates.items(),
                key=lambda item: (-item[1], item[0]),
            )[:TOP]
            for word, count in top:
                result_id, message_id = build_result_message_ids(
                    execution_id=message.execution_id,
                    query_id=4,
                    result_kind="result",
                    identity={"word": word},
                    producer_id="q4_top20_reducer",
                )
                self._messaging.publish(
                    ResultMessage(
                        query_id=4,
                        record={"word": word, "count": count},
                        execution_id=message.execution_id,
                        message_id=message_id,
                        result_id=result_id,
                    )
                )
            result_id, message_id = build_result_message_ids(
                execution_id=message.execution_id,
                query_id=4,
                result_kind="end",
                identity={"end": True},
                producer_id="q4_top20_reducer",
            )
            self._messaging.publish(
                QueryEndMessage(
                    query_id=4,
                    execution_id=message.execution_id,
                    message_id=message_id,
                    result_id=result_id,
                )
            )
            self._executions.pop(message.execution_id, None)
            return

        raise ValueError(
            "Q4Top20Reducer expected Q4_PARTIAL_TOP_BATCH or Q4_PARTITION_END"
        )

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
