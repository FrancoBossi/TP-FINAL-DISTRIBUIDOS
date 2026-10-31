import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    QueryEndMessage,
    ResultMessage,
)
from q4.top20_reducer.controller import Q4Top20ReducerController


class TestQ4Top20Reducer(unittest.TestCase):
    def test_waits_for_all_partitions_and_orders_global_top(self):
        messaging = Mock()
        controller = Q4Top20ReducerController(messaging, partition_count=2)
        ack, nack = Mock(), Mock()

        controller.process_message(
            Q4PartialTopBatchMessage(
                [{"word": "z", "count": 2}, {"word": "a", "count": 1}],
                "run",
                "reply",
            ),
            ack,
            nack,
        )
        controller.process_message(Q4PartitionEndMessage(0, "run", "reply"), ack, nack)
        messaging.publish.assert_not_called()

        controller.process_message(
            Q4PartialTopBatchMessage(
                [
                    {"word": "b", "count": 3},
                    {"word": "c", "count": 2},
                ],
                "run",
                "reply",
            ),
            ack,
            nack,
        )
        controller.process_message(Q4PartitionEndMessage(1, "run", "reply"), ack, nack)

        self.assertEqual(
            [
                call.publish(ResultMessage(4, {"word": "b", "count": 3}, "run")),
                call.publish(ResultMessage(4, {"word": "c", "count": 2}, "run")),
                call.publish(ResultMessage(4, {"word": "z", "count": 2}, "run")),
                call.publish(ResultMessage(4, {"word": "a", "count": 1}, "run")),
                call.publish(QueryEndMessage(4, "run")),
            ],
            messaging.mock_calls,
        )
        self.assertEqual({}, controller._executions)
        nack.assert_not_called()

    def test_failed_publication_is_nacked_and_keeps_candidates_for_retry(self):
        messaging = Mock()
        controller = Q4Top20ReducerController(messaging)
        ack, nack = Mock(), Mock()

        controller.process_message(
            Q4PartialTopBatchMessage(
                words=[
                    {"word": "network", "count": 3},
                    {"word": "system", "count": 2},
                ],
                execution_id="execution-123",
                reply_queue="results.execution-123",
            ),
            ack,
            nack,
        )

        messaging.publish.side_effect = RuntimeError("middleware unavailable")

        with self.assertRaises(RuntimeError):
            controller.process_message(
                Q4PartitionEndMessage(
                    partition_id=0,
                    execution_id="execution-123",
                    reply_queue="results.execution-123",
                ),
                ack,
                nack,
            )

        self.assertIn("execution-123", controller._executions)
        self.assertEqual(1, ack.call_count)
        nack.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
