from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
)
from word_extractor.extractor import tokenize_batch
from word_extractor.messaging import Ack, Nack, WordExtractorMessaging


class WordExtractorController:
    def __init__(
        self,
        messaging: WordExtractorMessaging,
        query_router_partition_count: int,
    ) -> None:
        if query_router_partition_count <= 0:
            raise ValueError("QueryRouter partition count must be positive")

        self._messaging = messaging
        self._query_router_partition_count = query_router_partition_count
        self._finished_query_router_partitions: dict[str, set[int]] = {}

    def _process_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "query_router":
            raise ValueError(
                f"WordExtractor expected query_router partition end, "
                f"got {message.stage}"
            )

        if not 0 <= message.partition_id < self._query_router_partition_count:
            raise ValueError(
                f"Invalid QueryRouter partition id: {message.partition_id}"
            )

        finished_partitions = self._finished_query_router_partitions.setdefault(
            message.execution_id,
            set(),
        )
        finished_partitions.add(message.partition_id)

        if len(finished_partitions) < self._query_router_partition_count:
            return

        input_end = RoutedInputEndMessage(
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
        )

        self._messaging.publish_q3(input_end)
        self._messaging.publish_q4_q5(input_end)
        self._finished_query_router_partitions.pop(message.execution_id)

    def _handle_message(self, message: Message) -> None:
        match message:
            case TextArticleBatchMessage():
                batch = tokenize_batch(message)

                if batch.articles:
                    self._messaging.publish_q3(batch)
                    self._messaging.publish_q4_q5(batch)

            case PartitionEndMessage():
                self._process_partition_end(message)

            case _:
                raise ValueError(
                    "WordExtractor expected TEXT_ARTICLE_BATCH or PARTITION_END"
                )

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

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
