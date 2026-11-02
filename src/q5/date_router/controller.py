import json

from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q5ArticleWordCountBatchMessage,
    Q5CandidateBatchMessage,
)
from common.partition_router import PartitionRouter
from q5.date_router.messaging import Q5DateRouterMessaging
from q5.date_router.router import route_batch


class Q5DateRouterController:
    def __init__(
        self,
        messaging: Q5DateRouterMessaging,
        historical_router: PartitionRouter | None = None,
        partition_id: int = 0,
        projector_partition_count: int = 1,
        candidate_router: PartitionRouter | None = None,
    ) -> None:
        if projector_partition_count <= 0:
            raise ValueError("projector_partition_count must be positive")
        self._messaging = messaging
        self._historical_router = historical_router
        self._partition_id = partition_id
        self._projector_partition_count = projector_partition_count
        self._candidate_router = candidate_router
        self._finished_projector_partitions: dict[str, set[int]] = {}

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case Q5ArticleWordCountBatchMessage():
                    historical_batch, candidate_batch = route_batch(message)

                    if historical_batch.word_counts:
                        self._publish_historical(historical_batch)

                    if candidate_batch.articles:
                        self._publish_candidates(candidate_batch)

                case PartitionEndMessage():
                    self._validate_projector_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "Q5DateRouter expected "
                        "Q5_ARTICLE_WORD_COUNT_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def _validate_projector_end(self, message: PartitionEndMessage) -> None:
        if message.stage != "q4_q5_word_projector":
            raise ValueError("Q5DateRouter received an invalid partition stage")
        if not 0 <= message.partition_id < self._projector_partition_count:
            raise ValueError("Q4Q5 projector partition id is outside configured range")

    def process_control_message(self, message: Message) -> None:
        if not isinstance(message, PartitionEndMessage):
            raise ValueError(
                "Q5DateRouter expected PARTITION_END from control exchange"
            )
        self._validate_projector_end(message)
        finished = self._finished_projector_partitions.setdefault(
            message.execution_id, set()
        )
        finished.add(message.partition_id)
        if len(finished) < self._projector_partition_count:
            return

        partition_end = PartitionEndMessage(
            "q5_date_router",
            self._partition_id,
            message.execution_id,
            message.reply_queue,
        )
        if self._historical_router is None:
            self._messaging.publish_historical(partition_end)
        else:
            for partition in range(self._historical_router.partition_count):
                self._messaging.publish_historical_to(
                    f"{self._historical_router.queue_prefix}.{partition}",
                    partition_end,
                )
        if self._candidate_router is None:
            self._messaging.publish_candidates(partition_end)
        else:
            for partition in range(self._candidate_router.partition_count):
                self._messaging.publish_candidates_to(
                    f"{self._candidate_router.queue_prefix}.{partition}",
                    partition_end,
                )
        self._finished_projector_partitions.pop(message.execution_id)

    def _publish_historical(self, message: Message) -> None:
        if self._historical_router is None:
            self._messaging.publish_historical(message)
        else:
            self._messaging.publish_historical_to(
                self._historical_router.queue_for(message.execution_id), message
            )

    def _publish_candidates(self, message: Q5CandidateBatchMessage) -> None:
        if self._candidate_router is None:
            self._messaging.publish_candidates(message)
            return
        by_queue: dict[str, list[dict]] = {}
        for article in message.articles:
            key = json.dumps(
                [message.execution_id, article["article_id"]],
                ensure_ascii=False,
            )
            queue = self._candidate_router.queue_for(key)
            by_queue.setdefault(queue, []).append(article)
        for queue, articles in by_queue.items():
            self._messaging.publish_candidates_to(
                queue,
                Q5CandidateBatchMessage(
                    articles,
                    message.execution_id,
                    message.reply_queue,
                ),
            )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message, self.process_control_message
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
