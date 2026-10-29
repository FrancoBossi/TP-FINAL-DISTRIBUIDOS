import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
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

        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_filters_and_publishes_non_empty_batches(self):
        expected_q1, expected_q2 = filter_batch(self.batch)

        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

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

        self.controller.process_message(
            batch,
            self.ack,
            self.nack,
        )

        self.messaging.publish_q1.assert_not_called()
        self.messaging.publish_q2.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_forwards_input_end_to_both_branches(self):
        self.controller.process_message(
            self.input_end,
            self.ack,
            self.nack,
        )

        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1(self.input_end),
                call.publish_q2(self.input_end),
            ],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_nacks_when_second_publication_fails(self):
        error = RuntimeError("Q2 output unavailable")
        self.messaging.publish_q2.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.messaging.publish_q1.assert_called_once()
        self.messaging.publish_q2.assert_called_once()
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
            "Q1_Q2_ARTICLE_BATCH or ROUTED_INPUT_END",
        ):
            self.controller.process_message(
                message,
                self.ack,
                self.nack,
            )

        self.messaging.publish_q1.assert_not_called()
        self.messaging.publish_q2.assert_not_called()
        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
