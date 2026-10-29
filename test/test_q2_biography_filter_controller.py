import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    Q2CandidateBatchMessage,
    RoutedInputEndMessage,
)
from q2.biography_filter.controller import Q2BiographyFilterController
from q2.biography_filter.filter import filter_biography_batch


class TestQ2BiographyFilterController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = Q2BiographyFilterController(
            messaging=self.messaging,
        )

        self.batch = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "Example biography",
                    "url": "https://example.com/article",
                    "sections": ["Biography"],
                    "image_url": "https://example.com/image.jpg",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_filters_and_publishes_non_empty_batch(self):
        expected = filter_biography_batch(self.batch)

        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(expected)
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_does_not_publish_empty_batch(self):
        batch = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "Article without biography",
                    "url": "https://example.com/article",
                    "sections": ["History"],
                    "image_url": "https://example.com/image.jpg",
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

        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_forwards_input_end(self):
        self.controller.process_message(
            self.input_end,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(self.input_end)
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_nacks_when_publication_fails(self):
        error = RuntimeError("ImageFetcher unavailable")
        self.messaging.publish.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.messaging.publish.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = Q1CandidateBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Q2_CANDIDATE_BATCH or ROUTED_INPUT_END",
        ):
            self.controller.process_message(
                message,
                self.ack,
                self.nack,
            )

        self.messaging.publish.assert_not_called()
        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
