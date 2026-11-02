import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    PartitionEndMessage,
    Q4PartialTopBatchMessage,
    Q4PartitionEndMessage,
    Q4WordBatchMessage,
)
from common.partition_router import PartitionRouter
from q4.word_counter.controller import Q4WordCounterController
from q4.word_counter.messaging import Q4WordCounterMessaging


class TestQ4WordCounter(unittest.TestCase):
    def test_messaging_routes_one_execution_to_one_reducer(self):
        input_queue = Mock()
        output_queues = {
            f"q4_top20_reducer.input.{partition}": Mock() for partition in range(3)
        }
        router = PartitionRouter("q4_top20_reducer.input", 3)
        messaging = Q4WordCounterMessaging(
            input_queue,
            output_queues["q4_top20_reducer.input.0"],
            output_queues,
            router,
        )
        message = Q4PartialTopBatchMessage(
            [{"word": "network", "count": 2}],
            "execution-123",
            "results.execution-123",
        )

        messaging.publish(message)

        selected_queue = router.queue_for("execution-123")
        output_queues[selected_queue].send.assert_called_once_with(message.serialize())
        for queue_name, queue in output_queues.items():
            if queue_name != selected_queue:
                queue.send.assert_not_called()

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
            PartitionEndMessage("q4_q5_word_projector", 0, "run", "reply"),
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
                PartitionEndMessage(
                    "q4_q5_word_projector",
                    0,
                    "execution-123",
                    "results.execution-123",
                ),
                ack,
                nack,
            )

        self.assertIn("execution-123", controller._counts)
        self.assertEqual(1, ack.call_count)
        nack.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
