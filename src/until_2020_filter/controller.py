from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q1Q2ArticleBatchMessage,
    RoutedInputEndMessage,
)
from until_2020_filter.filter import filter_batch
from until_2020_filter.messaging import (
    Ack,
    Nack,
    Until2020FilterMessaging,
)


class Until2020FilterController:
    def __init__(
        self,
        messaging: Until2020FilterMessaging,
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
                f"Until2020Filter expected query_router partition end, "
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

        self._messaging.publish_q1(input_end)
        self._messaging.publish_q2(input_end)
        self._finished_query_router_partitions.pop(message.execution_id)

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case Q1Q2ArticleBatchMessage():
                    q1_batch, q2_batch = filter_batch(message)

                    if q1_batch.articles:
                        self._messaging.publish_q1(q1_batch)

                    if q2_batch.articles:
                        self._messaging.publish_q2(q2_batch)

                case PartitionEndMessage():
                    self._process_partition_end(message)

                case _:
                    raise ValueError(
                        "Until2020Filter expected Q1_Q2_ARTICLE_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
