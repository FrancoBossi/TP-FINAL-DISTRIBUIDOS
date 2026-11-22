from common.coordinated_consumer import Ack, Nack
from common.durable_barrier import DurableBarrier
from common.message_identity import build_control_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    Message,
    PartitionEndMessage,
    Q1CandidateBatchMessage,
    QueryPartitionEndMessage,
)
from common.persistence import PersistenceLayout
from q1.filter import build_query1_results
from q1.messaging import Q1Messaging


class Q1Controller:
    def __init__(
        self,
        messaging: Q1Messaging,
        partition_id: int,
        until_2020_partition_count: int,
        persistence_layout: PersistenceLayout | None = None,
        output_destination: str = "result_sink.input",
    ) -> None:
        if until_2020_partition_count <= 0:
            raise ValueError("Until2020Filter partition count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._until_2020_partition_count = until_2020_partition_count
        self._finished_until_2020_partitions: dict[str, set[int]] = {}
        self._durable_barrier = (
            DurableBarrier(
                persistence_layout,
                until_2020_partition_count,
                {output_destination: self._messaging.publish},
                lambda message: {
                    output_destination: QueryPartitionEndMessage(
                        query_id=1,
                        partition_id=self._partition_id,
                        execution_id=message.execution_id,
                        message_id=build_control_message_id(
                            execution_id=message.execution_id,
                            stage="q1",
                            partition_id=self._partition_id,
                            operation="query_partition_end",
                        ),
                    )
                },
            )
            if persistence_layout is not None
            else None
        )

    def _validate_until_2020_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "until_2020_filter":
            raise ValueError(
                f"Q1 expected until_2020_filter partition end, got {message.stage}"
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
            if self._durable_barrier is not None and self._durable_barrier.is_closed(
                message
            ):
                ack()
                return
            match message:
                case Q1CandidateBatchMessage():
                    results = build_query1_results(message)

                    for result in results:
                        self._messaging.publish(result)

                case PartitionEndMessage():
                    self._validate_until_2020_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError("Q1 expected Q1_CANDIDATE_BATCH or PARTITION_END")
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
                self._validate_until_2020_partition_end(message)
                if self._durable_barrier is not None:
                    self._durable_barrier.apply(message)
                    return

                finished_partitions = self._finished_until_2020_partitions.setdefault(
                    message.execution_id,
                    set(),
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._until_2020_partition_count:
                    return

                self._messaging.publish(
                    QueryPartitionEndMessage(
                        query_id=1,
                        partition_id=self._partition_id,
                        execution_id=message.execution_id,
                        message_id=build_control_message_id(
                            execution_id=message.execution_id,
                            stage="q1",
                            partition_id=self._partition_id,
                            operation="query_partition_end",
                        ),
                    )
                )
                self._finished_until_2020_partitions.pop(message.execution_id)

            case _:
                raise ValueError("Q1 expected PARTITION_END from control exchange")

    def start(self) -> None:
        if self._durable_barrier is not None:
            self._durable_barrier.recover()
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
