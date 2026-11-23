from dataclasses import dataclass

from common.message_identity import build_control_message_id, build_message_id
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q5HistoricalWordCountBatchMessage,
    Q5PartialStatsMessage,
    Q5StatsPartitionEndMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from q5.stats_worker.messaging import Ack, Nack, Q5StatsWorkerMessaging


@dataclass
class ExecutionStats:
    total_word_count: int = 0
    abstract_count: int = 0


class Q5StatsWorkerController:
    def __init__(
        self,
        messaging: Q5StatsWorkerMessaging,
        partition_id: int = 0,
        date_router_partition_count: int = 1,
        persistence_layout: PersistenceLayout | None = None,
        output_destination: str = "q5_average_reducer.input",
    ) -> None:
        if partition_id < 0:
            raise ValueError("partition_id must not be negative")
        if date_router_partition_count <= 0:
            raise ValueError("date_router_partition_count must be positive")

        self._messaging = messaging
        self._partition_id = partition_id
        self._date_router_partition_count = date_router_partition_count
        self._executions: dict[str, ExecutionStats] = {}
        self._finished_date_router_partitions: dict[str, set[int]] = {}
        self._persistence_layout = persistence_layout
        self._output_destination = output_destination
        self._stores: dict[str, DurableExecutionStore] = {}

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        if self._persistence_layout is not None:
            self._process_durable(message, ack, nack)
            return
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise

        ack()

    def _process_durable(self, message: Message, ack: Ack, nack: Nack) -> None:
        if not isinstance(
            message, (Q5HistoricalWordCountBatchMessage, PartitionEndMessage)
        ):
            nack()
            raise ValueError(
                "Q5StatsWorker expected Q5_HISTORICAL_WORD_COUNT_BATCH or PARTITION_END"
            )
        try:
            if isinstance(message, PartitionEndMessage):
                if message.stage != "q5_date_router":
                    raise ValueError(
                        "Q5StatsWorker received an invalid partition stage"
                    )
                if not 0 <= message.partition_id < self._date_router_partition_count:
                    raise ValueError(
                        "Q5DateRouter partition id is outside configured range"
                    )
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            if state["completed"]:
                raise PersistenceCorruptionError("Q5StatsWorker input after completion")
            outputs = []
            if isinstance(message, Q5HistoricalWordCountBatchMessage):
                state["total_word_count"] += sum(message.word_counts)
                state["abstract_count"] += len(message.word_counts)
            else:
                state["finished_partitions"] = sorted(
                    {*state["finished_partitions"], message.partition_id}
                )
                if (
                    len(state["finished_partitions"])
                    == self._date_router_partition_count
                ):
                    state["completed"] = True
                    outputs = [
                        Q5PartialStatsMessage(
                            state["total_word_count"],
                            state["abstract_count"],
                            message.execution_id,
                            message.reply_queue,
                            build_message_id(
                                execution_id=message.execution_id,
                                producer_id="q5_stats_worker",
                                operation="partial_stats",
                                identity={"partition_id": self._partition_id},
                            ),
                        ),
                        Q5StatsPartitionEndMessage(
                            self._partition_id,
                            message.execution_id,
                            message.reply_queue,
                            build_control_message_id(
                                execution_id=message.execution_id,
                                stage="q5_stats_worker",
                                partition_id=self._partition_id,
                                operation="stats_partition_end",
                            ),
                        ),
                    ]
            store.apply(
                message.message_id,
                state,
                [
                    PendingOutput.from_message(self._output_destination, item)
                    for item in outputs
                ],
            )
            self._publish_pending(store)
        except Exception:
            nack()
            raise
        ack()

    def _decode_state(self, state):
        if not state:
            return {
                "total_word_count": 0,
                "abstract_count": 0,
                "finished_partitions": [],
                "completed": False,
            }
        if set(state) != {
            "total_word_count",
            "abstract_count",
            "finished_partitions",
            "completed",
        }:
            raise PersistenceCorruptionError("invalid Q5StatsWorker state")
        total, count, finished, completed = (
            state["total_word_count"],
            state["abstract_count"],
            state["finished_partitions"],
            state["completed"],
        )
        if (
            type(total) is not int
            or total < 0
            or type(count) is not int
            or count < 0
            or not isinstance(finished, list)
            or finished != sorted(set(finished))
            or any(
                type(item) is not int
                or not 0 <= item < self._date_router_partition_count
                for item in finished
            )
            or type(completed) is not bool
            or completed != (len(finished) == self._date_router_partition_count)
        ):
            raise PersistenceCorruptionError("invalid Q5StatsWorker state")
        return state

    def _store_for(self, execution_id):
        if execution_id not in self._stores:
            self._stores[execution_id] = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
        return self._stores[execution_id]

    def _publish_pending(self, store):
        for output in store.pending_outputs:
            if output.destination != self._output_destination:
                raise PersistenceCorruptionError("invalid Q5StatsWorker destination")
            message = output.deserialize_message()
            if not isinstance(
                message, (Q5PartialStatsMessage, Q5StatsPartitionEndMessage)
            ):
                raise PersistenceCorruptionError("invalid Q5StatsWorker outbox")
            self._messaging.publish(message)
            store.confirm(output)

    def _recover(self):
        for execution_id in self._persistence_layout.execution_ids():
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _handle_message(self, message: Message) -> None:
        if isinstance(message, Q5HistoricalWordCountBatchMessage):
            stats = self._executions.setdefault(
                message.execution_id,
                ExecutionStats(),
            )
            stats.total_word_count += sum(message.word_counts)
            stats.abstract_count += len(message.word_counts)
            return

        if isinstance(message, PartitionEndMessage):
            if message.stage != "q5_date_router":
                raise ValueError("Q5StatsWorker received an invalid partition stage")
            if not 0 <= message.partition_id < self._date_router_partition_count:
                raise ValueError(
                    "Q5DateRouter partition id is outside configured range"
                )
            finished = self._finished_date_router_partitions.setdefault(
                message.execution_id, set()
            )
            finished.add(message.partition_id)
            if len(finished) < self._date_router_partition_count:
                return
            stats = self._executions.get(
                message.execution_id,
                ExecutionStats(),
            )

            self._messaging.publish(
                Q5PartialStatsMessage(
                    total_word_count=stats.total_word_count,
                    abstract_count=stats.abstract_count,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                    message_id=build_message_id(
                        execution_id=message.execution_id,
                        producer_id="q5_stats_worker",
                        operation="partial_stats",
                        identity={"partition_id": self._partition_id},
                    ),
                )
            )
            self._messaging.publish(
                Q5StatsPartitionEndMessage(
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                    message_id=build_control_message_id(
                        execution_id=message.execution_id,
                        stage="q5_stats_worker",
                        partition_id=self._partition_id,
                        operation="stats_partition_end",
                    ),
                )
            )

            self._executions.pop(message.execution_id, None)
            self._finished_date_router_partitions.pop(message.execution_id)
            return

        raise ValueError(
            "Q5StatsWorker expected Q5_HISTORICAL_WORD_COUNT_BATCH or PARTITION_END"
        )

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
