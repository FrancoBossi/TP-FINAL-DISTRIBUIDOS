import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    PartitionEndMessage,
    Q1Q2ArticleBatchMessage,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
)
from until_2020_filter.controller import Until2020FilterController
from until_2020_filter.filter import filter_batch


class TestUntil2020FilterController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()
        self.controller = Until2020FilterController(
            messaging=self.messaging,
            query_router_partition_count=2,
        )
        self.batch = Q1Q2ArticleBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "sections": ["Biography"],
                    "image_url": None,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.partition_end_0 = PartitionEndMessage(
            stage="query_router",
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.partition_end_1 = PartitionEndMessage(
            stage="query_router",
            partition_id=1,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_filters_and_publishes_non_empty_batches(self):
        expected_q1, expected_q2 = filter_batch(self.batch)

        self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1(expected_q1),
                call.publish_q2(expected_q2),
            ],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_does_not_publish_empty_batches(self):
        batch = Q1Q2ArticleBatchMessage(
            articles=[
                {
                    "name": "Article from 2021",
                    "url": "https://example.com/2021",
                    "language": "en",
                    "modification_date": "2021-01-01T00:00:00",
                    "sections": ["Biography"],
                    "image_url": None,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.controller.process_message(batch, self.ack, self.nack)

        self.messaging.publish_q1.assert_not_called()
        self.messaging.publish_q2.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_waits_for_all_query_router_partitions(self):
        self.controller.process_message(
            self.partition_end_0,
            self.ack,
            self.nack,
        )

        self.messaging.publish_q1.assert_not_called()
        self.messaging.publish_q2.assert_not_called()
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

        expected = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1(expected),
                call.publish_q2(expected),
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

        self.messaging.publish_q1.assert_not_called()
        self.messaging.publish_q2.assert_not_called()

    def test_nacks_when_second_publication_fails(self):
        error = RuntimeError("Q2 output unavailable")
        self.messaging.publish_q2.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertIs(raised.exception, error)
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = TextArticleBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Q1_Q2_ARTICLE_BATCH or PARTITION_END",
        ):
            self.controller.process_message(message, self.ack, self.nack)

        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
