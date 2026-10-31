from dataclasses import dataclass, field

from common.message_protocol.message import (
    Message,
    Q5AverageMessage,
    Q5PartialStatsMessage,
    Q5StatsPartitionEndMessage,
)
from q5.average_reducer.messaging import (
    Ack,
    Nack,
    Q5AverageReducerMessaging,
)


@dataclass
class ExecutionState:
    total_word_count: int = 0
    abstract_count: int = 0
    ended_partitions: set[int] = field(default_factory=set)


class Q5AverageReducerController:
    def __init__(
        self,
        messaging: Q5AverageReducerMessaging,
        partition_count: int = 1,
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")

        self._messaging = messaging
        self._partition_count = partition_count
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
        if isinstance(message, Q5PartialStatsMessage):
            state = self._executions.setdefault(
                message.execution_id,
                ExecutionState(),
            )
            state.total_word_count += message.total_word_count
            state.abstract_count += message.abstract_count
            return

        if isinstance(message, Q5StatsPartitionEndMessage):
            if not 0 <= message.partition_id < self._partition_count:
                raise ValueError("Q5 stats partition id is outside configured range")

            state = self._executions.setdefault(
                message.execution_id,
                ExecutionState(),
            )
            state.ended_partitions.add(message.partition_id)

            if len(state.ended_partitions) != self._partition_count:
                return

            if state.abstract_count == 0:
                raise ValueError(
                    "Cannot calculate Q5 average without historical abstracts"
                )

            self._messaging.publish(
                Q5AverageMessage(
                    average_word_count=(state.total_word_count / state.abstract_count),
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
            )
            self._executions.pop(message.execution_id, None)
            return

        raise ValueError(
            "Q5AverageReducer expected Q5_PARTIAL_STATS or Q5_STATS_PARTITION_END"
        )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
