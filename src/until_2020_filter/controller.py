from common.coordinated_consumer import Ack, Nack
from common.durable_barrier import DurableBarrier
from common.message_identity import build_control_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    Message,
    PartitionEndMessage,
    Q1Q2ArticleBatchMessage,
)
from common.persistence import PersistenceLayout
from until_2020_filter.filter import filter_batch
from until_2020_filter.messaging import Until2020FilterMessaging


class Until2020FilterController:
    def __init__(
        self,
        messaging: Until2020FilterMessaging,
        partition_id: int,
        query_router_partition_count: int,
        persistence_layout: PersistenceLayout | None = None,
        q1_destination: str = "q1.input",
        q2_destination: str = "q2_biography_filter.input",
    ) -> None:
        if query_router_partition_count <= 0:
            raise ValueError("QueryRouter partition count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._query_router_partition_count = query_router_partition_count
        self._finished_query_router_partitions: dict[str, set[int]] = {}
        self._durable_barrier = (
            DurableBarrier(
                persistence_layout,
                query_router_partition_count,
                {
                    q1_destination: self._messaging.publish_q1,
                    q2_destination: self._messaging.publish_q2,
                },
                lambda message: {
                    q1_destination: self._partition_end(message),
                    q2_destination: self._partition_end(message),
                },
            )
            if persistence_layout is not None
            else None
        )

    def _partition_end(self, message: PartitionEndMessage) -> PartitionEndMessage:
        return PartitionEndMessage(
            stage="until_2020_filter",
            partition_id=self._partition_id,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
            message_id=build_control_message_id(
                execution_id=message.execution_id,
                stage="until_2020_filter",
                partition_id=self._partition_id,
            ),
        )

    def _validate_query_router_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "query_router":
            raise ValueError(
                f"Until2020Filter expected query_router partition end, "
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
            if self._durable_barrier is not None and self._durable_barrier.is_closed(
                message
            ):
                ack()
                return
            match message:
                case Q1Q2ArticleBatchMessage():
                    q1_batch, q2_batch = filter_batch(message)

                    if q1_batch.articles:
                        self._messaging.publish_q1(q1_batch)

                    if q2_batch.articles:
                        self._messaging.publish_q2(q2_batch)

                case PartitionEndMessage():
                    self._validate_query_router_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "Until2020Filter expected Q1_Q2_ARTICLE_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def process_control_message(
        self,
        message: Message,
    ) -> None:
        if self._durable_barrier is not None:
            if isinstance(message, ExecutionCleanupMessage):
                self._durable_barrier.cleanup(message.execution_id)
                return
            if self._durable_barrier.is_closed(message):
                return
        match message:
            case PartitionEndMessage():
                self._validate_query_router_partition_end(message)
                if self._durable_barrier is not None:
                    self._durable_barrier.apply(message)
                    return

                finished_partitions = self._finished_query_router_partitions.setdefault(
                    message.execution_id,
                    set(),
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._query_router_partition_count:
                    return

                partition_end = self._partition_end(message)
                self._messaging.publish_q1(partition_end)
                self._messaging.publish_q2(partition_end)
                self._finished_query_router_partitions.pop(message.execution_id)

            case _:
                raise ValueError(
                    "Until2020Filter expected PARTITION_END from control exchange"
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
