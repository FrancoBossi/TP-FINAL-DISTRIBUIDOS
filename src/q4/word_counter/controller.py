from collections import Counter

from common.message_protocol.message import (
    Message,
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    Q4WordBatchMessage,
    RoutedInputEndMessage,
)
from q4.word_counter.messaging import Ack, Nack, Q4WordCounterMessaging

TOP = 20


class Q4WordCounterController:
    def __init__(
        self,
        messaging: Q4WordCounterMessaging,
        partition_id: int = 0,
    ) -> None:
        if partition_id < 0:
            raise ValueError("partition_id must not be negative")
        self._messaging = messaging
        self._partition_id = partition_id
        self._counts: dict[str, Counter[str]] = {}

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

        if isinstance(message, RoutedInputEndMessage):
            counts = self._counts.get(message.execution_id, Counter())
            top = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:TOP]
            self._messaging.publish(
                Q4PartialTopBatchMessage(
                    words=[
                        {"word": word, "count": count}
                        for word, count in top
                    ],
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
            return

        raise ValueError(
            "Q4WordCounter expected Q4_WORD_BATCH or ROUTED_INPUT_END"
        )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
