import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    ArticleBatchMessage,
    NormalizedArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from preprocessor.controller import PreprocessorController


class TestPreprocessorController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = PreprocessorController(
            messaging=self.messaging,
        )

        self.batch = ArticleBatchMessage(
            articles=[
                {
                    "name": "École",
                    "url": "https://fr.wikipedia.org/wiki/École",
                    "in_language": {"identifier": "FR"},
                    "date_modified": "2020-01-02T03:04:05",
                    "abstract": None,
                    "sections": '[{"name":"Biographie"}]',
                    "image": None,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_normalizes_and_publishes_batch(self):
        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

        expected = NormalizedArticleBatchMessage(
            articles=[
                {
                    "name": "École",
                    "url": "https://fr.wikipedia.org/wiki/École",
                    "language": "fr",
                    "modification_date": "2020-01-02T03:04:05",
                    "abstract": None,
                    "sections": ["Biographie"],
                    "image_url": None,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.publish.assert_called_once_with(expected)
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

    def test_nacks_and_propagates_publication_error(self):
        error = RuntimeError("middleware unavailable")
        self.messaging.publish.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = RoutedArticleMessage(
            article={"name": "Example article"},
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "ARTICLE_BATCH or ROUTED_INPUT_END",
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
