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
        controller.process_message(
            Q4PartitionEndMessage(0, "run", "reply"), ack, nack
        )
        messaging.publish.assert_not_called()

        controller.process_message(
            Q4PartialTopBatchMessage(
                [{"word": "b", "count": 3}, {"word": "a", "count": 2}],
                "run",
                "reply",
            ),
            ack,
            nack,
        )
        controller.process_message(
            Q4PartitionEndMessage(1, "run", "reply"), ack, nack
        )

        self.assertEqual(
            [
                call.publish(ResultMessage(4, {"word": "a", "count": 3}, "run")),
                call.publish(ResultMessage(4, {"word": "b", "count": 3}, "run")),
                call.publish(ResultMessage(4, {"word": "z", "count": 2}, "run")),
                call.publish(QueryEndMessage(4, "run")),
            ],
            messaging.mock_calls,
        )
        self.assertEqual({}, controller._executions)
        nack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
