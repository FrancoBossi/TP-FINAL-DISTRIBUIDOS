from common.coordinated_consumer import Ack, Nack
from common.durable_barrier import DurableBarrier
from common.message_identity import build_control_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    Message,
    PartitionEndMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter
from common.persistence import PersistenceLayout
from q3.word_projector.messaging import Q3WordProjectorMessaging
from q3.word_projector.projector import project_batch


class Q3WordProjectorController:
    def __init__(
        self,
        messaging: Q3WordProjectorMessaging,
        router: PartitionRouter,
        partition_id: int,
        word_extractor_partition_count: int,
        persistence_layout: PersistenceLayout | None = None,
    ) -> None:
        if word_extractor_partition_count <= 0:
            raise ValueError("WordExtractor partition count must be positive")

        self._messaging = messaging
        self._router = router
        self._partition_id = partition_id
        self._word_extractor_partition_count = word_extractor_partition_count
        self._finished_word_extractor_partitions: dict[str, set[int]] = {}
        destinations = [
            f"{self._router.queue_prefix}.{partition}"
            for partition in range(self._router.partition_count)
        ]
        self._durable_barrier = (
            DurableBarrier(
                persistence_layout,
                word_extractor_partition_count,
                {
                    queue: (
                        lambda message, queue=queue: self._messaging.publish(
                            queue, message
                        )
                    )
                    for queue in destinations
                },
                lambda message: {
                    queue: self._partition_end(message) for queue in destinations
                },
            )
            if persistence_layout is not None
            else None
        )

    def _partition_end(self, message: PartitionEndMessage) -> PartitionEndMessage:
        return PartitionEndMessage(
            stage="q3_word_projector",
            partition_id=self._partition_id,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
            message_id=build_control_message_id(
                execution_id=message.execution_id,
                stage="q3_word_projector",
                partition_id=self._partition_id,
            ),
        )

    def _validate_word_extractor_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "word_extractor":
            raise ValueError(
                "Q3WordProjector expected word_extractor partition end, "
                f"got {message.stage}"
            )

        if not 0 <= message.partition_id < self._word_extractor_partition_count:
            raise ValueError(
                f"Invalid WordExtractor partition id: {message.partition_id}"
            )

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            if self._durable_barrier is not None and self._durable_barrier.is_closed(
                message
            ):
                ack()
                return
            match message:
                case TokenizedArticleBatchMessage():
                    for queue, batch in project_batch(message, self._router).items():
                        self._messaging.publish(queue, batch)

                case PartitionEndMessage():
                    self._validate_word_extractor_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "Q3WordProjector expected "
                        "TOKENIZED_ARTICLE_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def process_control_message(self, message: Message) -> None:
        if self._durable_barrier is not None:
            if isinstance(message, ExecutionCleanupMessage):
                self._durable_barrier.cleanup(message.execution_id)
                return
            if self._durable_barrier.is_closed(message):
                return
        match message:
            case PartitionEndMessage():
                self._validate_word_extractor_partition_end(message)
                if self._durable_barrier is not None:
                    self._durable_barrier.apply(message)
                    return

                finished_partitions = (
                    self._finished_word_extractor_partitions.setdefault(
                        message.execution_id,
                        set(),
                    )
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._word_extractor_partition_count:
                    return

                partition_end = self._partition_end(message)
                for partition in range(self._router.partition_count):
                    queue = f"{self._router.queue_prefix}.{partition}"
                    self._messaging.publish(queue, partition_end)

                self._finished_word_extractor_partitions.pop(message.execution_id)

            case _:
                raise ValueError(
                    "Q3WordProjector expected PARTITION_END from control exchange"
                )

    def start(self) -> None:
        if self._durable_barrier is not None:
            self._durable_barrier.recover()
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
