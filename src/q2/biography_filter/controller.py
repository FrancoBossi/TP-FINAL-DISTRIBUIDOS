from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q2CandidateBatchMessage,
)
from q2.biography_filter.filter import filter_biography_batch
from q2.biography_filter.messaging import Q2BiographyFilterMessaging


class Q2BiographyFilterController:
    def __init__(
        self,
        messaging: Q2BiographyFilterMessaging,
        partition_id: int,
        until_2020_partition_count: int,
    ) -> None:
        if until_2020_partition_count <= 0:
            raise ValueError("Until2020Filter partition count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._until_2020_partition_count = until_2020_partition_count
        self._finished_until_2020_partitions: dict[str, set[int]] = {}

    def _validate_until_2020_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "until_2020_filter":
            raise ValueError(
                f"Q2BiographyFilter expected until_2020_filter "
                f"partition end, got {message.stage}"
            )

        if not 0 <= message.partition_id < self._until_2020_partition_count:
            raise ValueError(
                f"Invalid Until2020Filter partition id: {message.partition_id}"
            )

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case Q2CandidateBatchMessage():
                    image_candidates = filter_biography_batch(message)

                    if image_candidates.articles:
                        self._messaging.publish(image_candidates)

                case PartitionEndMessage():
                    self._validate_until_2020_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "Q2BiographyFilter expected Q2_CANDIDATE_BATCH or PARTITION_END"
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
                self._validate_until_2020_partition_end(message)

                finished_partitions = self._finished_until_2020_partitions.setdefault(
                    message.execution_id,
                    set(),
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._until_2020_partition_count:
                    return

                self._messaging.publish(
                    PartitionEndMessage(
                        stage="q2_biography_filter",
                        partition_id=self._partition_id,
                        execution_id=message.execution_id,
                        reply_queue=message.reply_queue,
                    )
                )
                self._finished_until_2020_partitions.pop(message.execution_id)

            case _:
                raise ValueError(
                    "Q2BiographyFilter expected PARTITION_END from control exchange"
                )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
