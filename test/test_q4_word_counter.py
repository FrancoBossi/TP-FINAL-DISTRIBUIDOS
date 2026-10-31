import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    Q4WordBatchMessage,
    RoutedInputEndMessage,
)
from q4.word_counter.controller import Q4WordCounterController


class TestQ4WordCounter(unittest.TestCase):
    def test_counts_words_once_per_projected_appearance_and_emits_local_top(self):
        messaging = Mock()
        controller = Q4WordCounterController(messaging, partition_id=2)
        ack, nack = Mock(), Mock()

        controller.process_message(
            Q4WordBatchMessage(["z", "a", "z", "b"], "run", "reply"),
            ack,
            nack,
        )
        controller.process_message(
            RoutedInputEndMessage("run", "reply"),
            ack,
            nack,
        )

        self.assertEqual(
            [
                call.publish(
                    Q4PartialTopBatchMessage(
                        [
                            {"word": "z", "count": 2},
                            {"word": "a", "count": 1},
                            {"word": "b", "count": 1},
                        ],
                        "run",
                        "reply",
                    )
                ),
                call.publish(Q4PartitionEndMessage(2, "run", "reply")),
            ],
            messaging.mock_calls,
        )
        self.assertEqual(2, ack.call_count)
        nack.assert_not_called()

    def test_failed_publication_is_nacked_and_keeps_counts_for_retry(self):
        messaging = Mock()
        controller = Q4WordCounterController(messaging, partition_id=0)
        ack, nack = Mock(), Mock()

        controller.process_message(
            Q4WordBatchMessage(
                words=["network", "network", "system"],
                execution_id="execution-123",
                reply_queue="results.execution-123",
            ),
            ack,
            nack,
        )

        messaging.publish.side_effect = RuntimeError("middleware unavailable")

        with self.assertRaises(RuntimeError):
            controller.process_message(
                RoutedInputEndMessage(
                    execution_id="execution-123",
                    reply_queue="results.execution-123",
                ),
                ack,
                nack,
            )

        self.assertIn("execution-123", controller._counts)
        self.assertEqual(1, ack.call_count)
        nack.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
