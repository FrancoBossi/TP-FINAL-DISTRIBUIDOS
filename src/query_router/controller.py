from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    Message,
    NormalizedArticleBatchMessage,
    PartitionEndMessage,
)
from query_router.messaging import QueryRouterMessaging
from query_router.router import route_batch


class QueryRouterController:
    def __init__(
        self,
        messaging: QueryRouterMessaging,
        partition_id: int,
        preprocessor_partition_count: int,
    ) -> None:
        if preprocessor_partition_count <= 0:
            raise ValueError("Preprocessor partition count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._preprocessor_partition_count = preprocessor_partition_count
        self._finished_preprocessor_partitions: dict[str, set[int]] = {}

    def _validate_preprocessor_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "preprocessor":
            raise ValueError(
                f"QueryRouter expected preprocessor partition end, got {message.stage}"
            )

        if not 0 <= message.partition_id < self._preprocessor_partition_count:
            raise ValueError(
                f"Invalid preprocessor partition id: {message.partition_id}"
            )

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case NormalizedArticleBatchMessage():
                    q1_q2_batch, text_batch = route_batch(message)

                    self._messaging.publish_q1_q2(q1_q2_batch)
                    self._messaging.publish_text(text_batch)

                case PartitionEndMessage():
                    self._validate_preprocessor_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "QueryRouter expected NORMALIZED_ARTICLE_BATCH or PARTITION_END"
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
                self._validate_preprocessor_partition_end(message)

                finished_partitions = self._finished_preprocessor_partitions.setdefault(
                    message.execution_id,
                    set(),
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._preprocessor_partition_count:
                    return

                partition_end = PartitionEndMessage(
                    stage="query_router",
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
                self._messaging.publish_q1_q2(partition_end)
                self._messaging.publish_text(partition_end)
                self._finished_preprocessor_partitions.pop(message.execution_id)

            case _:
                raise ValueError(
                    "QueryRouter expected PARTITION_END from control exchange"
                )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
