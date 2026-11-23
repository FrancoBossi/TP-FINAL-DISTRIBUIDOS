from dataclasses import dataclass, field

from common.message_identity import build_message_id
from common.message_protocol.message import (
    Message,
    Q5AverageMessage,
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
from q5.average_reducer.messaging import (
    Ack,
    Nack,
    Q5AverageReducerMessaging,
)


@dataclass
class ExecutionState:
    total_word_count: int = 0
    abstract_count: int = 0
    ended_partitions: set[int] = field(default_factory=set)


class Q5AverageReducerController:
    def __init__(
        self,
        messaging: Q5AverageReducerMessaging,
        partition_count: int = 1,
        persistence_layout: PersistenceLayout | None = None,
        output_destinations: list[str] | None = None,
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")

        self._messaging = messaging
        self._partition_count = partition_count
        self._executions: dict[str, ExecutionState] = {}
        self._persistence_layout = persistence_layout
        self._output_destinations = output_destinations or []
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
        if not isinstance(message, (Q5PartialStatsMessage, Q5StatsPartitionEndMessage)):
            nack()
            raise ValueError(
                "Q5AverageReducer expected Q5_PARTIAL_STATS or Q5_STATS_PARTITION_END"
            )
        try:
            if (
                isinstance(message, Q5StatsPartitionEndMessage)
                and not 0 <= message.partition_id < self._partition_count
            ):
                raise ValueError("Q5 stats partition id is outside configured range")
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            if state["completed"]:
                raise PersistenceCorruptionError(
                    "Q5AverageReducer input after completion"
                )
            outputs = []
            if isinstance(message, Q5PartialStatsMessage):
                state["total_word_count"] += message.total_word_count
                state["abstract_count"] += message.abstract_count
            else:
                state["ended_partitions"] = sorted(
                    {*state["ended_partitions"], message.partition_id}
                )
                if len(state["ended_partitions"]) == self._partition_count:
                    state["completed"] = True
                    average = (
                        None
                        if state["abstract_count"] == 0
                        else state["total_word_count"] / state["abstract_count"]
                    )
                    output = Q5AverageMessage(
                        average,
                        message.execution_id,
                        message.reply_queue,
                        build_message_id(
                            execution_id=message.execution_id,
                            producer_id="q5_average_reducer",
                            operation="average",
                            identity={"aggregate": "historical_word_count"},
                        ),
                    )
                    outputs = [
                        PendingOutput.from_message(destination, output)
                        for destination in self._output_destinations
                    ]
            store.apply(message.message_id, state, outputs)
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
                "ended_partitions": [],
                "completed": False,
            }
        if set(state) != {
            "total_word_count",
            "abstract_count",
            "ended_partitions",
            "completed",
        }:
            raise PersistenceCorruptionError("invalid Q5AverageReducer state")
        total, count, ended, completed = (
            state["total_word_count"],
            state["abstract_count"],
            state["ended_partitions"],
            state["completed"],
        )
        if (
            type(total) is not int
            or total < 0
            or type(count) is not int
            or count < 0
            or not isinstance(ended, list)
            or ended != sorted(set(ended))
            or any(
                type(item) is not int or not 0 <= item < self._partition_count
                for item in ended
            )
            or type(completed) is not bool
            or completed != (len(ended) == self._partition_count)
        ):
            raise PersistenceCorruptionError("invalid Q5AverageReducer state")
        return state

    def _store_for(self, execution_id):
        if execution_id not in self._stores:
            self._stores[execution_id] = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
        return self._stores[execution_id]

    def _publish_pending(self, store):
        for output in store.pending_outputs:
            if output.destination not in self._output_destinations:
                raise PersistenceCorruptionError("invalid Q5AverageReducer destination")
            message = output.deserialize_message()
            if not isinstance(message, Q5AverageMessage):
                raise PersistenceCorruptionError("invalid Q5AverageReducer outbox")
            self._messaging.publish_to(output.destination, message)
            store.confirm(output)

    def _recover(self):
        for execution_id in self._persistence_layout.execution_ids():
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _handle_message(self, message: Message) -> None:
        if isinstance(message, Q5PartialStatsMessage):
            state = self._executions.setdefault(
                message.execution_id,
                ExecutionState(),
            )
            state.total_word_count += message.total_word_count
            state.abstract_count += message.abstract_count
            return

        if isinstance(message, Q5StatsPartitionEndMessage):
            if not 0 <= message.partition_id < self._partition_count:
                raise ValueError("Q5 stats partition id is outside configured range")

            state = self._executions.setdefault(
                message.execution_id,
                ExecutionState(),
            )
            state.ended_partitions.add(message.partition_id)

            if len(state.ended_partitions) != self._partition_count:
                return

            average_word_count = None
            if state.abstract_count > 0:
                average_word_count = state.total_word_count / state.abstract_count

            self._messaging.publish(
                Q5AverageMessage(
                    average_word_count=average_word_count,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                    message_id=build_message_id(
                        execution_id=message.execution_id,
                        producer_id="q5_average_reducer",
                        operation="average",
                        identity={"aggregate": "historical_word_count"},
                    ),
                )
            )
            self._executions.pop(message.execution_id, None)
            return

        raise ValueError(
            "Q5AverageReducer expected Q5_PARTIAL_STATS or Q5_STATS_PARTITION_END"
        )

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
