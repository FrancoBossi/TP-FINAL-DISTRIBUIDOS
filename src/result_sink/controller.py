import logging
from collections.abc import Mapping

from common.message_identity import build_result_message_ids
from common.message_protocol.external import QUERY_IDS, validate_result
from common.message_protocol.message import (
    Message,
    QueryEndMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from common.middleware.middleware import MessageMiddlewareUnroutableError
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from result_sink.messaging import Ack, Nack, ResultSinkMessaging

logger = logging.getLogger(__name__)


class ResultSinkController:
    def __init__(
        self,
        messaging: ResultSinkMessaging,
        partition_counts: Mapping[int, int],
        persistence_layout: PersistenceLayout | None = None,
    ) -> None:
        if set(partition_counts) != QUERY_IDS:
            raise ValueError("partition_counts must configure queries 1 through 5")

        if any(count <= 0 for count in partition_counts.values()):
            raise ValueError("partition counts must be greater than zero")

        self._messaging = messaging
        self._partition_counts = dict(partition_counts)
        self._ended_partitions: dict[tuple[str, int], set[int]] = {}
        self._persistence_layout = persistence_layout
        self._stores: dict[str, DurableExecutionStore] = {}

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        if self._persistence_layout is not None:
            self._process_durable(message, ack, nack)
            return
        try:
            self._process_message(message)
        except MessageMiddlewareUnroutableError:
            self._discard_barrier_state(message)
            logger.warning(
                "Discarding result for execution %s: destination results.%s is gone",
                message.execution_id,
                message.execution_id,
            )
        except Exception:
            nack()
            raise

        ack()

    def _process_durable(self, message: Message, ack: Ack, nack: Nack) -> None:
        if not isinstance(
            message, (ResultMessage, QueryEndMessage, QueryPartitionEndMessage)
        ):
            nack()
            raise ValueError(
                "ResultSink expected RESULT, QUERY_END or QUERY_PARTITION_END"
            )
        try:
            self._validate_execution_id(message.execution_id)
            if isinstance(message, (ResultMessage, QueryEndMessage)):
                validate_result(message, set())
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            outputs = []
            if isinstance(message, ResultMessage):
                outputs = [message]
            elif isinstance(message, QueryEndMessage):
                state["completed_queries"] = sorted(
                    {*state["completed_queries"], message.query_id}
                )
                outputs = [message]
            else:
                if message.query_id not in QUERY_IDS:
                    raise ValueError("Invalid query id")
                expected = self._partition_counts[message.query_id]
                if (
                    type(message.partition_id) is not int
                    or not 0 <= message.partition_id < expected
                ):
                    raise ValueError("Query partition id is outside configured range")
                key = str(message.query_id)
                ended = set(state["ended_partitions"].get(key, []))
                ended.add(message.partition_id)
                state["ended_partitions"][key] = sorted(ended)
                if (
                    len(ended) == expected
                    and message.query_id not in state["completed_queries"]
                ):
                    state["completed_queries"] = sorted(
                        {*state["completed_queries"], message.query_id}
                    )
                    result_id, message_id = build_result_message_ids(
                        execution_id=message.execution_id,
                        query_id=message.query_id,
                        result_kind="end",
                        identity={"end": True},
                        producer_id="result_sink",
                    )
                    outputs = [
                        QueryEndMessage(
                            message.query_id,
                            message.execution_id,
                            message_id,
                            result_id,
                        )
                    ]
            destination = f"results.{message.execution_id}"
            store.apply(
                message.message_id,
                state,
                [PendingOutput.from_message(destination, item) for item in outputs],
            )
            self._publish_pending(store)
        except Exception:
            nack()
            raise
        ack()

    def _decode_state(self, state):
        if not state:
            return {"ended_partitions": {}, "completed_queries": []}
        if set(state) != {"ended_partitions", "completed_queries"}:
            raise PersistenceCorruptionError("invalid ResultSink state")
        ended, completed = state["ended_partitions"], state["completed_queries"]
        if (
            not isinstance(ended, dict)
            or not isinstance(completed, list)
            or completed != sorted(set(completed))
            or any(
                type(query) is not int or query not in QUERY_IDS for query in completed
            )
        ):
            raise PersistenceCorruptionError("invalid ResultSink state")
        for query, partitions in ended.items():
            if (
                query not in {str(item) for item in QUERY_IDS}
                or not isinstance(partitions, list)
                or partitions != sorted(set(partitions))
                or any(
                    type(item) is not int
                    or not 0 <= item < self._partition_counts[int(query)]
                    for item in partitions
                )
            ):
                raise PersistenceCorruptionError("invalid ResultSink state")
        return state

    def _store_for(self, execution_id):
        if execution_id not in self._stores:
            self._stores[execution_id] = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
        return self._stores[execution_id]

    def _publish_pending(self, store):
        for output in store.pending_outputs:
            message = output.deserialize_message()
            if (
                not isinstance(message, (ResultMessage, QueryEndMessage))
                or output.destination != f"results.{message.execution_id}"
            ):
                raise PersistenceCorruptionError("invalid ResultSink outbox")
            try:
                self._messaging.publish(message)
            except MessageMiddlewareUnroutableError:
                logger.warning(
                    "Discarding result for execution %s: destination is gone",
                    message.execution_id,
                )
            store.confirm(output)

    def _recover(self):
        for execution_id in self._persistence_layout.execution_ids():
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _process_message(self, message: Message) -> None:
        if isinstance(message, (ResultMessage, QueryEndMessage)):
            validate_result(message, set())
            self._validate_execution_id(message.execution_id)
            self._messaging.publish(message)
            return

        if isinstance(message, QueryPartitionEndMessage):
            self._process_partition_end(message)
            return

        raise ValueError("ResultSink expected RESULT, QUERY_END or QUERY_PARTITION_END")

    def _process_partition_end(
        self,
        message: QueryPartitionEndMessage,
    ) -> None:
        self._validate_execution_id(message.execution_id)

        if message.query_id not in QUERY_IDS:
            raise ValueError("Invalid query id")

        expected_count = self._partition_counts[message.query_id]

        if (
            type(message.partition_id) is not int
            or not 0 <= message.partition_id < expected_count
        ):
            raise ValueError("Query partition id is outside configured range")

        key = (message.execution_id, message.query_id)
        ended_partitions = self._ended_partitions.setdefault(key, set())
        ended_partitions.add(message.partition_id)

        if len(ended_partitions) != expected_count:
            return

        result_id, message_id = build_result_message_ids(
            execution_id=message.execution_id,
            query_id=message.query_id,
            result_kind="end",
            identity={"end": True},
            producer_id="result_sink",
        )
        self._messaging.publish(
            QueryEndMessage(
                query_id=message.query_id,
                execution_id=message.execution_id,
                message_id=message_id,
                result_id=result_id,
            )
        )
        self._ended_partitions.pop(key)

    def _validate_execution_id(self, execution_id: str) -> None:
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError("Result requires a nonempty execution_id")

    def _discard_barrier_state(self, message: Message) -> None:
        if not isinstance(message, QueryPartitionEndMessage):
            return

        key = (message.execution_id, message.query_id)
        self._ended_partitions.pop(key, None)

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
