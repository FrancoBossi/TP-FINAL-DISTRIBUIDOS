import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    ArticleBatchMessage,
    NormalizedArticleBatchMessage,
    RoutedInputEndMessage,
)
from query_router.controller import QueryRouterController
from query_router.router import route_batch


class TestQueryRouterController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = QueryRouterController(
            messaging=self.messaging,
        )

        self.batch = NormalizedArticleBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "abstract": "Example abstract.",
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

    def test_projects_and_publishes_batch_to_both_branches(self):
        expected_q1_q2, expected_text = route_batch(self.batch)

        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1_q2(expected_q1_q2),
                call.publish_text(expected_text),
            ],
        )
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
                call.publish_q1_q2(self.input_end),
                call.publish_text(self.input_end),
            ],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_nacks_when_second_publication_fails(self):
        error = RuntimeError("text output unavailable")
        self.messaging.publish_text.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.messaging.publish_q1_q2.assert_called_once()
        self.messaging.publish_text.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = ArticleBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "NORMALIZED_ARTICLE_BATCH or ROUTED_INPUT_END",
        ):
            self.controller.process_message(
                message,
                self.ack,
                self.nack,
            )

        self.messaging.publish_q1_q2.assert_not_called()
        self.messaging.publish_text.assert_not_called()
        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
