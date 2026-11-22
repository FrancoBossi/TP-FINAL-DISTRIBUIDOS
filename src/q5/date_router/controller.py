import json

from common.coordinated_consumer import Ack, Nack
from common.durable_barrier import DurableBarrier
from common.message_identity import build_control_message_id, derive_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    Message,
    PartitionEndMessage,
    Q5ArticleWordCountBatchMessage,
    Q5CandidateBatchMessage,
)
from common.partition_router import PartitionRouter
from common.persistence import PersistenceLayout
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
        persistence_layout: PersistenceLayout | None = None,
    ) -> None:
        if projector_partition_count <= 0:
            raise ValueError("projector_partition_count must be positive")
        self._messaging = messaging
        self._historical_router = historical_router
        self._partition_id = partition_id
        self._projector_partition_count = projector_partition_count
        self._candidate_router = candidate_router
        self._finished_projector_partitions: dict[str, set[int]] = {}
        publishers = self._barrier_publishers()
        self._durable_barrier = (
            DurableBarrier(
                persistence_layout,
                projector_partition_count,
                publishers,
                lambda message: {
                    destination: self._partition_end(message)
                    for destination in publishers
                },
            )
            if persistence_layout is not None
            else None
        )

    def _barrier_publishers(self):
        publishers = {}
        if self._historical_router is None:
            publishers["historical"] = self._messaging.publish_historical
        else:
            for partition in range(self._historical_router.partition_count):
                queue = f"{self._historical_router.queue_prefix}.{partition}"
                publishers[queue] = lambda message, queue=queue: (
                    self._messaging.publish_historical_to(queue, message)
                )
        if self._candidate_router is None:
            publishers["candidates"] = self._messaging.publish_candidates
        else:
            for partition in range(self._candidate_router.partition_count):
                queue = f"{self._candidate_router.queue_prefix}.{partition}"
                publishers[queue] = lambda message, queue=queue: (
                    self._messaging.publish_candidates_to(queue, message)
                )
        return publishers

    def _partition_end(self, message: PartitionEndMessage) -> PartitionEndMessage:
        return PartitionEndMessage(
            "q5_date_router",
            self._partition_id,
            message.execution_id,
            message.reply_queue,
            build_control_message_id(
                execution_id=message.execution_id,
                stage="q5_date_router",
                partition_id=self._partition_id,
            ),
        )

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            if self._durable_barrier is not None and self._durable_barrier.is_closed(
                message
            ):
                ack()
                return
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
        if self._durable_barrier is not None:
            if isinstance(message, ExecutionCleanupMessage):
                self._durable_barrier.cleanup(message.execution_id)
                return
            if self._durable_barrier.is_closed(message):
                return
        if not isinstance(message, PartitionEndMessage):
            raise ValueError(
                "Q5DateRouter expected PARTITION_END from control exchange"
            )
        self._validate_projector_end(message)
        if self._durable_barrier is not None:
            self._durable_barrier.apply(message)
            return
        finished = self._finished_projector_partitions.setdefault(
            message.execution_id, set()
        )
        finished.add(message.partition_id)
        if len(finished) < self._projector_partition_count:
            return

        partition_end = self._partition_end(message)
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
                    derive_message_id(
                        execution_id=message.execution_id,
                        input_message_id=message.message_id,
                        producer_id="q5_date_router",
                        operation="candidate_partition_batch",
                        identity={"destination": queue},
                    ),
                ),
            )

    def start(self) -> None:
        if self._durable_barrier is not None:
            self._durable_barrier.recover()
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
            **(
                {"on_idle": self._durable_barrier.sweep}
                if self._durable_barrier is not None
                else {}
            ),
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
