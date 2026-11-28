from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from common.execution_cleanup import ExecutionCleanup
from common.message_identity import (
    build_control_message_id,
    build_result_message_ids,
)
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q5AverageMessage,
    Q5CandidateBatchMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from q5.comparator.buffer import CandidateBuffer
from q5.comparator.messaging import Ack, Nack, Q5ComparatorMessaging


@dataclass
class ExecutionState:
    average_received: bool = False
    average_word_count: float | None = None
    candidate_input_ended: bool = False
    ended_date_router_partitions: set[int] = field(default_factory=set)


class Q5ComparatorController:
    def __init__(
        self,
        messaging: Q5ComparatorMessaging,
        candidate_buffer: CandidateBuffer,
        partition_id: int = 0,
        date_router_partition_count: int = 1,
        persistence_layout: PersistenceLayout | None = None,
        output_destination: str = "result_sink.input",
    ) -> None:
        if date_router_partition_count <= 0:
            raise ValueError("date_router_partition_count must be positive")
        self._messaging = messaging
        self._candidate_buffer = candidate_buffer
        self._partition_id = partition_id
        self._date_router_partition_count = date_router_partition_count
        self._executions: dict[str, ExecutionState] = {}
        self._persistence_layout = persistence_layout
        self._cleanup = (
            ExecutionCleanup(persistence_layout, retention_seconds=3600)
            if persistence_layout is not None
            else None
        )
        self._output_destination = output_destination
        self._stores: dict[str, DurableExecutionStore] = {}

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
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
        if not isinstance(
            message, (Q5CandidateBatchMessage, PartitionEndMessage, Q5AverageMessage)
        ):
            nack()
            raise ValueError(
                "Q5Comparator expected Q5_CANDIDATE_BATCH, PARTITION_END or Q5_AVERAGE"
            )
        try:
            if isinstance(message, PartitionEndMessage):
                if message.stage != "q5_date_router":
                    raise ValueError("Q5Comparator received an invalid partition stage")
                if not 0 <= message.partition_id < self._date_router_partition_count:
                    raise ValueError(
                        "Q5DateRouter partition id is outside configured range"
                    )
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            if state["completed"]:
                raise PersistenceCorruptionError("Q5Comparator input after completion")

            outputs: list[Message] = []
            clear_buffer = False
            if isinstance(message, Q5CandidateBatchMessage):
                if not state["average_received"]:
                    self._candidate_buffer.append(
                        message.execution_id, message.articles
                    )
                elif state["average_word_count"] is not None:
                    outputs.extend(
                        self._matching_results(
                            message.execution_id,
                            message.articles,
                            state["average_word_count"],
                        )
                    )
            elif isinstance(message, Q5AverageMessage):
                state["average_received"] = True
                state["average_word_count"] = message.average_word_count
                if message.average_word_count is not None:
                    outputs.extend(
                        self._matching_results(
                            message.execution_id,
                            self._candidate_buffer.iterate(message.execution_id),
                            message.average_word_count,
                        )
                    )
                clear_buffer = True
            else:
                state["ended_partitions"] = sorted(
                    {*state["ended_partitions"], message.partition_id}
                )

            if (
                state["average_received"]
                and len(state["ended_partitions"]) == self._date_router_partition_count
            ):
                state["completed"] = True
                outputs.append(
                    QueryPartitionEndMessage(
                        query_id=5,
                        partition_id=self._partition_id,
                        execution_id=message.execution_id,
                        message_id=build_control_message_id(
                            execution_id=message.execution_id,
                            stage="q5_comparator",
                            partition_id=self._partition_id,
                            operation="query_partition_end",
                        ),
                    )
                )
                clear_buffer = True
            store.apply(
                message.message_id,
                state,
                [
                    PendingOutput.from_message(self._output_destination, item)
                    for item in outputs
                ],
            )
            if clear_buffer:
                self._candidate_buffer.clear(message.execution_id)
            self._publish_pending(store)
        except Exception:
            nack()
            raise
        ack()

    def _matching_results(self, execution_id, articles, average):
        outputs = []
        for article in articles:
            if article["word_count"] <= average:
                continue
            result_id, message_id = build_result_message_ids(
                execution_id=execution_id,
                query_id=5,
                result_kind="result",
                identity={"url": article["url"]},
                producer_id="q5_comparator",
            )
            outputs.append(
                ResultMessage(
                    5,
                    {"url": article["url"], "word_count": article["word_count"]},
                    execution_id,
                    message_id,
                    result_id,
                )
            )
        return outputs

    def _decode_state(self, state):
        if not state:
            return {
                "average_received": False,
                "average_word_count": None,
                "ended_partitions": [],
                "completed": False,
            }
        if set(state) != {
            "average_received",
            "average_word_count",
            "ended_partitions",
            "completed",
        }:
            raise PersistenceCorruptionError("invalid Q5Comparator state")
        received, average, ended, completed = (
            state["average_received"],
            state["average_word_count"],
            state["ended_partitions"],
            state["completed"],
        )
        if (
            type(received) is not bool
            or (average is not None and not isinstance(average, (int, float)))
            or (not received and average is not None)
            or not isinstance(ended, list)
            or ended != sorted(set(ended))
            or any(
                type(item) is not int
                or not 0 <= item < self._date_router_partition_count
                for item in ended
            )
            or type(completed) is not bool
            or completed
            != (received and len(ended) == self._date_router_partition_count)
        ):
            raise PersistenceCorruptionError("invalid Q5Comparator state")
        return state

    def _store_for(self, execution_id):
        if execution_id not in self._stores:
            self._stores[execution_id] = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
        return self._stores[execution_id]

    def _publish_pending(self, store):
        pending = store.pending_outputs
        for output in pending:
            if output.destination != self._output_destination:
                raise PersistenceCorruptionError("invalid Q5Comparator destination")
            message = output.deserialize_message()
            if not isinstance(message, (ResultMessage, QueryPartitionEndMessage)):
                raise PersistenceCorruptionError("invalid Q5Comparator outbox")
            self._messaging.publish(message)
        store.confirm_many(pending)

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)
        self._executions.pop(execution_id, None)
        self._candidate_buffer.clear(execution_id)

    def _recover(self):
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._persistence_layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            state = self._decode_state(store.state)
            if state["average_received"]:
                self._candidate_buffer.clear(execution_id)
            self._publish_pending(store)

    def _handle_message(self, message: Message) -> None:
        if isinstance(message, Q5CandidateBatchMessage):
            state = self._state_for(message.execution_id)

            if not state.average_received:
                self._candidate_buffer.append(
                    message.execution_id,
                    message.articles,
                )
            elif state.average_word_count is not None:
                self._publish_matches(
                    message.execution_id,
                    message.articles,
                    state.average_word_count,
                )
            return

        if isinstance(message, PartitionEndMessage):
            if message.stage != "q5_date_router":
                raise ValueError("Q5Comparator received an invalid partition stage")
            if not 0 <= message.partition_id < self._date_router_partition_count:
                raise ValueError(
                    "Q5DateRouter partition id is outside configured range"
                )
            state = self._state_for(message.execution_id)
            state.ended_date_router_partitions.add(message.partition_id)
            state.candidate_input_ended = (
                len(state.ended_date_router_partitions)
                == self._date_router_partition_count
            )
            self._try_finish(message.execution_id)
            return

        if isinstance(message, Q5AverageMessage):
            state = self._state_for(message.execution_id)
            state.average_received = True
            state.average_word_count = message.average_word_count

            if message.average_word_count is not None:
                self._publish_matches(
                    message.execution_id,
                    self._candidate_buffer.iterate(message.execution_id),
                    message.average_word_count,
                )

            self._candidate_buffer.clear(message.execution_id)
            self._try_finish(message.execution_id)
            return

        raise ValueError(
            "Q5Comparator expected Q5_CANDIDATE_BATCH, PARTITION_END or Q5_AVERAGE"
        )

    def _state_for(self, execution_id: str) -> ExecutionState:
        return self._executions.setdefault(
            execution_id,
            ExecutionState(),
        )

    def _publish_matches(
        self,
        execution_id: str,
        articles: Iterable[dict[str, Any]],
        average_word_count: float,
    ) -> None:
        for article in articles:
            if article["word_count"] <= average_word_count:
                continue

            result_id, message_id = build_result_message_ids(
                execution_id=execution_id,
                query_id=5,
                result_kind="result",
                identity={"url": article["url"]},
                producer_id="q5_comparator",
            )
            self._messaging.publish(
                ResultMessage(
                    query_id=5,
                    record={
                        "url": article["url"],
                        "word_count": article["word_count"],
                    },
                    execution_id=execution_id,
                    message_id=message_id,
                    result_id=result_id,
                )
            )

    def _try_finish(self, execution_id: str) -> None:
        state = self._executions[execution_id]

        if not state.average_received or not state.candidate_input_ended:
            return

        self._messaging.publish(
            QueryPartitionEndMessage(
                query_id=5,
                partition_id=self._partition_id,
                execution_id=execution_id,
                message_id=build_control_message_id(
                    execution_id=execution_id,
                    stage="q5_comparator",
                    partition_id=self._partition_id,
                    operation="query_partition_end",
                ),
            )
        )
        self._candidate_buffer.clear(execution_id)
        self._executions.pop(execution_id, None)

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(
            self.process_message,
            **({"on_idle": self._cleanup.sweep} if self._cleanup is not None else {}),
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
