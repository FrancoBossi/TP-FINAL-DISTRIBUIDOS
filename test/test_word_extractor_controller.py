import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    AckMessage,
    PartitionEndMessage,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
    TokenizedArticleBatchMessage,
)
from word_extractor.controller import WordExtractorController


class TestWordExtractorController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()
        self.controller = WordExtractorController(
            messaging=self.messaging,
            query_router_partition_count=2,
        )
        self.batch = TextArticleBatchMessage(
            articles=[
                {
                    "article_id": "a",
                    "url": "https://example.com/a",
                    "language": "en",
                    "modification_date": "2024-01-01T00:00:00",
                    "abstract": "Network NETWORK and",
                }
            ],
            execution_id="run",
            reply_queue="results.run",
        )
        self.partition_end_0 = PartitionEndMessage(
            stage="query_router",
            partition_id=0,
            execution_id="run",
            reply_queue="results.run",
        )
        self.partition_end_1 = PartitionEndMessage(
            stage="query_router",
            partition_id=1,
            execution_id="run",
            reply_queue="results.run",
        )

    def test_publishes_to_both_branches_before_ack(self):
        events = Mock()
        events.attach_mock(self.messaging, "messaging")
        events.attach_mock(self.ack, "ack")
        expected = TokenizedArticleBatchMessage(
            articles=[
                {
                    "article_id": "a",
                    "url": "https://example.com/a",
                    "language": "en",
                    "modification_date": "2024-01-01T00:00:00",
                    "has_abstract": True,
                    "words": ["network", "network", "and"],
                }
            ],
            execution_id="run",
            reply_queue="results.run",
        )

        self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertEqual(
            events.mock_calls,
            [
                call.messaging.publish_q3(expected),
                call.messaging.publish_q4_q5(expected),
                call.ack(),
            ],
        )
        self.nack.assert_not_called()

    def test_empty_batch_publishes_nothing(self):
        message = TextArticleBatchMessage([], "run", "results.run")

        self.controller.process_message(message, self.ack, self.nack)

        self.assertEqual([], self.messaging.mock_calls)
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_waits_for_all_query_router_partitions(self):
        self.controller.process_message(
            self.partition_end_0,
            self.ack,
            self.nack,
        )

        self.messaging.publish_q3.assert_not_called()
        self.messaging.publish_q4_q5.assert_not_called()
        self.ack.assert_called_once()

    def test_forwards_one_input_end_after_barrier(self):
        self.controller.process_message(
            self.partition_end_0,
            self.ack,
            self.nack,
        )
        self.controller.process_message(
            self.partition_end_1,
            self.ack,
            self.nack,
        )

        expected = RoutedInputEndMessage("run", "results.run")
        self.assertEqual(
            self.messaging.mock_calls,
            [
                call.publish_q3(expected),
                call.publish_q4_q5(expected),
            ],
        )
        self.assertEqual(self.ack.call_count, 2)
        self.nack.assert_not_called()

    def test_does_not_count_same_partition_twice(self):
        self.controller.process_message(
            self.partition_end_0,
            self.ack,
            self.nack,
        )
        self.controller.process_message(
            self.partition_end_0,
            self.ack,
            self.nack,
        )

        self.messaging.publish_q3.assert_not_called()
        self.messaging.publish_q4_q5.assert_not_called()

    def test_publication_failure_never_acks(self):
        self.messaging.publish_q4_q5.side_effect = RuntimeError("unavailable")

        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            self.controller.process_message(self.batch, self.ack, self.nack)

        self.ack.assert_not_called()
        self.nack.assert_called_once()

    def test_unexpected_message_is_rejected(self):
        with self.assertRaises(ValueError):
            self.controller.process_message(AckMessage(), self.ack, self.nack)

        self.ack.assert_not_called()
        self.nack.assert_called_once()
        self.assertEqual([], self.messaging.mock_calls)

    def test_delegates_start_and_stop(self):
        self.controller.start()
        self.controller.stop()

        self.messaging.start_consuming.assert_called_once_with(
            self.controller.process_message
        )
        self.messaging.stop_consuming.assert_called_once()


if __name__ == "__main__":
    unittest.main()
