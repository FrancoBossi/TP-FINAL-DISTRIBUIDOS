import logging
from collections.abc import Mapping

from common.message_protocol.external import QUERY_IDS, validate_result
from common.message_protocol.message import (
    Message,
    QueryEndMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from common.middleware.middleware import MessageMiddlewareUnroutableError
from result_sink.messaging import Ack, Nack, ResultSinkMessaging

logger = logging.getLogger(__name__)


class ResultSinkController:
    def __init__(
        self,
        messaging: ResultSinkMessaging,
        partition_counts: Mapping[int, int],
    ) -> None:
        if set(partition_counts) != QUERY_IDS:
            raise ValueError("partition_counts must configure queries 1 through 5")

        if any(count <= 0 for count in partition_counts.values()):
            raise ValueError("partition counts must be greater than zero")

        self._messaging = messaging
        self._partition_counts = dict(partition_counts)
        self._ended_partitions: dict[tuple[str, int], set[int]] = {}

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
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

        self._messaging.publish(
            QueryEndMessage(
                query_id=message.query_id,
                execution_id=message.execution_id,
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
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
