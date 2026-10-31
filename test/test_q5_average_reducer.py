import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    Q5AverageMessage,
    Q5PartialStatsMessage,
    Q5StatsPartitionEndMessage,
)
from q5.average_reducer.controller import Q5AverageReducerController


class TestQ5AverageReducerController(unittest.TestCase):
    def test_waits_for_all_partitions_and_publishes_global_average(self):
        messaging = Mock()
        controller = Q5AverageReducerController(
            messaging=messaging,
            partition_count=2,
        )
        ack = Mock()
        nack = Mock()

        first_partial = Q5PartialStatsMessage(
            total_word_count=6,
            abstract_count=3,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        first_end = Q5StatsPartitionEndMessage(
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        second_partial = Q5PartialStatsMessage(
            total_word_count=8,
            abstract_count=2,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        second_end = Q5StatsPartitionEndMessage(
            partition_id=1,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        controller.process_message(first_partial, ack, nack)
        controller.process_message(first_end, ack, nack)

        messaging.publish.assert_not_called()
        self.assertIn("execution-123", controller._executions)

        controller.process_message(second_partial, ack, nack)
        controller.process_message(second_end, ack, nack)

        messaging.publish.assert_called_once_with(
            Q5AverageMessage(
                average_word_count=2.8,
                execution_id="execution-123",
                reply_queue="results.execution-123",
            )
        )
        self.assertEqual(ack.call_count, 4)
        nack.assert_not_called()
        self.assertNotIn("execution-123", controller._executions)

    def test_rejects_partition_outside_configured_range(self):
        messaging = Mock()
        controller = Q5AverageReducerController(
            messaging=messaging,
            partition_count=2,
        )
        ack = Mock()
        nack = Mock()

        invalid_end = Q5StatsPartitionEndMessage(
            partition_id=2,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaises(ValueError):
            controller.process_message(invalid_end, ack, nack)

        messaging.publish.assert_not_called()
        ack.assert_not_called()
        nack.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
