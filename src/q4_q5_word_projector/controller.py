import json

from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q4WordBatchMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter
from q4_q5_word_projector.messaging import Q4Q5WordProjectorMessaging
from q4_q5_word_projector.projector import project_batch


class Q4Q5WordProjectorController:
    def __init__(
        self,
        messaging: Q4Q5WordProjectorMessaging,
        router: PartitionRouter,
        partition_id: int,
        word_extractor_partition_count: int,
    ) -> None:
        if word_extractor_partition_count <= 0:
            raise ValueError("WordExtractor partition count must be positive")
        self._messaging = messaging
        self._router = router
        self._partition_id = partition_id
        self._word_extractor_partition_count = word_extractor_partition_count
        self._finished_word_extractor_partitions: dict[str, set[int]] = {}

    def _validate_word_extractor_end(self, message: PartitionEndMessage) -> None:
        if message.stage != "word_extractor":
            raise ValueError(
                "Q4Q5WordProjector expected word_extractor partition end, "
                f"got {message.stage}"
            )
        if not 0 <= message.partition_id < self._word_extractor_partition_count:
            raise ValueError(
                f"Invalid WordExtractor partition id: {message.partition_id}"
            )

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            match message:
                case TokenizedArticleBatchMessage():
                    self._project_batch(message)
                case PartitionEndMessage():
                    self._validate_word_extractor_end(message)
                    self._messaging.broadcast_control(message)
                case _:
                    raise ValueError(
                        "Q4Q5WordProjector expected "
                        "TOKENIZED_ARTICLE_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise
        ack()

    def _project_batch(self, message: TokenizedArticleBatchMessage) -> None:
        q4_batch, q5_batch = project_batch(message)
        by_queue: dict[str, list[str]] = {}
        for word in q4_batch.words:
            key = json.dumps([message.execution_id, word], ensure_ascii=False)
            by_queue.setdefault(self._router.queue_for(key), []).append(word)
        for queue, words in by_queue.items():
            self._messaging.publish_q4_to(
                queue,
                Q4WordBatchMessage(
                    words=words,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                ),
            )
        if q5_batch.articles:
            self._messaging.publish_q5(q5_batch)

    def process_control_message(self, message: Message) -> None:
        match message:
            case PartitionEndMessage():
                self._validate_word_extractor_end(message)
                finished = self._finished_word_extractor_partitions.setdefault(
                    message.execution_id, set()
                )
                finished.add(message.partition_id)
                if len(finished) < self._word_extractor_partition_count:
                    return

                partition_end = PartitionEndMessage(
                    stage="q4_q5_word_projector",
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
                for partition in range(self._router.partition_count):
                    self._messaging.publish_q4_to(
                        f"{self._router.queue_prefix}.{partition}", partition_end
                    )
                self._messaging.publish_q5(partition_end)
                self._finished_word_extractor_partitions.pop(message.execution_id)
            case _:
                raise ValueError(
                    "Q4Q5WordProjector expected PARTITION_END from control exchange"
                )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message, self.process_control_message
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
