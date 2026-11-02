from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q5AverageMessage,
    Q5CandidateBatchMessage,
    QueryPartitionEndMessage,
    ResultMessage,
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
    ) -> None:
        if date_router_partition_count <= 0:
            raise ValueError("date_router_partition_count must be positive")
        self._messaging = messaging
        self._candidate_buffer = candidate_buffer
        self._partition_id = partition_id
        self._date_router_partition_count = date_router_partition_count
        self._executions: dict[str, ExecutionState] = {}

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise

        ack()

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

            self._messaging.publish(
                ResultMessage(
                    query_id=5,
                    record={
                        "url": article["url"],
                        "word_count": article["word_count"],
                    },
                    execution_id=execution_id,
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
            )
        )
        self._candidate_buffer.clear(execution_id)
        self._executions.pop(execution_id, None)

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
