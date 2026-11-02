from collections import Counter

from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    Q4WordBatchMessage,
)
from q4.word_counter.messaging import Ack, Nack, Q4WordCounterMessaging

TOP = 20


class Q4WordCounterController:
    def __init__(
        self,
        messaging: Q4WordCounterMessaging,
        partition_id: int = 0,
        projector_partition_count: int = 1,
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

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

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
                )
            )
            self._messaging.publish(
                Q4PartitionEndMessage(
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
            )
            self._counts.pop(message.execution_id, None)
            self._finished_projector_partitions.pop(message.execution_id)
            return

        raise ValueError("Q4WordCounter expected Q4_WORD_BATCH or PARTITION_END")

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
