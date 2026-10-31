from dataclasses import dataclass, field

from common.message_protocol.message import (
    Message,
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    QueryEndMessage,
    ResultMessage,
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
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")
        self._messaging = messaging
        self._partition_count = partition_count
        self._executions: dict[str, ExecutionState] = {}

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

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
                self._messaging.publish(
                    ResultMessage(
                        query_id=4,
                        record={"word": word, "count": count},
                        execution_id=message.execution_id,
                    )
                )
            self._messaging.publish(
                QueryEndMessage(query_id=4, execution_id=message.execution_id)
            )
            self._executions.pop(message.execution_id, None)
            return

        raise ValueError(
            "Q4Top20Reducer expected Q4_PARTIAL_TOP_BATCH or Q4_PARTITION_END"
        )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
