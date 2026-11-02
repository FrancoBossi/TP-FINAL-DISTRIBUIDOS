from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    Message,
    PartitionEndMessage,
    QueryPartitionEndMessage,
)
from q2.image_fetcher.messaging import ImageFetcherMessaging
from q2.image_fetcher.processor import build_query2_results


class ImageFetcherController:
    def __init__(
        self,
        messaging: ImageFetcherMessaging,
        partition_id: int,
        biography_filter_partition_count: int,
    ) -> None:
        if biography_filter_partition_count <= 0:
            raise ValueError("Q2BiographyFilter partition count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._biography_filter_partition_count = biography_filter_partition_count
        self._finished_biography_filter_partitions: dict[
            str,
            set[int],
        ] = {}

    def _validate_biography_filter_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "q2_biography_filter":
            raise ValueError(
                "ImageFetcher expected q2_biography_filter "
                f"partition end, got {message.stage}"
            )

        if not 0 <= message.partition_id < self._biography_filter_partition_count:
            raise ValueError(
                f"Invalid Q2BiographyFilter partition id: {message.partition_id}"
            )

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case ImageCandidateBatchMessage():
                    results = build_query2_results(message)

                    for result in results:
                        self._messaging.publish(result)

                case PartitionEndMessage():
                    self._validate_biography_filter_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "ImageFetcher expected IMAGE_CANDIDATE_BATCH or PARTITION_END"
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
                self._validate_biography_filter_partition_end(message)

                finished_partitions = (
                    self._finished_biography_filter_partitions.setdefault(
                        message.execution_id,
                        set(),
                    )
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._biography_filter_partition_count:
                    return

                self._messaging.publish(
                    QueryPartitionEndMessage(
                        query_id=2,
                        partition_id=self._partition_id,
                        execution_id=message.execution_id,
                    )
                )
                self._finished_biography_filter_partitions.pop(message.execution_id)

            case _:
                raise ValueError(
                    "ImageFetcher expected PARTITION_END from control exchange"
                )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
