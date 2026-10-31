from dataclasses import dataclass

from common.message_protocol.message import (
    Message,
    Q5HistoricalWordCountBatchMessage,
    Q5PartialStatsMessage,
    Q5StatsPartitionEndMessage,
    RoutedInputEndMessage,
)
from q5.stats_worker.messaging import Ack, Nack, Q5StatsWorkerMessaging


@dataclass
class ExecutionStats:
    total_word_count: int = 0
    abstract_count: int = 0


class Q5StatsWorkerController:
    def __init__(
        self,
        messaging: Q5StatsWorkerMessaging,
        partition_id: int = 0,
    ) -> None:
        if partition_id < 0:
            raise ValueError("partition_id must not be negative")

        self._messaging = messaging
        self._partition_id = partition_id
        self._executions: dict[str, ExecutionStats] = {}

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
        if isinstance(message, Q5HistoricalWordCountBatchMessage):
            stats = self._executions.setdefault(
                message.execution_id,
                ExecutionStats(),
            )
            stats.total_word_count += sum(message.word_counts)
            stats.abstract_count += len(message.word_counts)
            return

        if isinstance(message, RoutedInputEndMessage):
            stats = self._executions.get(
                message.execution_id,
                ExecutionStats(),
            )

            self._messaging.publish(
                Q5PartialStatsMessage(
                    total_word_count=stats.total_word_count,
                    abstract_count=stats.abstract_count,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
            )
            self._messaging.publish(
                Q5StatsPartitionEndMessage(
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
            )

            self._executions.pop(message.execution_id, None)
            return

        raise ValueError(
            "Q5StatsWorker expected Q5_HISTORICAL_WORD_COUNT_BATCH or ROUTED_INPUT_END"
        )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
