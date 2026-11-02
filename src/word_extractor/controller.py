from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    TextArticleBatchMessage,
)
from word_extractor.extractor import tokenize_batch
from word_extractor.messaging import WordExtractorMessaging


class WordExtractorController:
    def __init__(
        self,
        messaging: WordExtractorMessaging,
        partition_id: int,
        query_router_partition_count: int,
    ) -> None:
        if query_router_partition_count <= 0:
            raise ValueError("QueryRouter partition count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._query_router_partition_count = query_router_partition_count
        self._finished_query_router_partitions: dict[str, set[int]] = {}

    def _validate_query_router_partition_end(
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

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case TextArticleBatchMessage():
                    batch = tokenize_batch(message)

                    if batch.articles:
                        self._messaging.publish_q3(batch)
                        self._messaging.publish_q4_q5(batch)

                case PartitionEndMessage():
                    self._validate_query_router_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "WordExtractor expected TEXT_ARTICLE_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def process_control_message(
        self,
        message: Message,
    ) -> None:
        match message:
            case PartitionEndMessage():
                self._validate_query_router_partition_end(message)

                finished_partitions = self._finished_query_router_partitions.setdefault(
                    message.execution_id,
                    set(),
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._query_router_partition_count:
                    return

                partition_end = PartitionEndMessage(
                    stage="word_extractor",
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
                self._messaging.publish_q3(partition_end)
                self._messaging.publish_q4_q5(partition_end)
                self._finished_query_router_partitions.pop(message.execution_id)

            case _:
                raise ValueError(
                    "WordExtractor expected PARTITION_END from control exchange"
                )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
