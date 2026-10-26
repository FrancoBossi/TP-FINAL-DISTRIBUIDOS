import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    ArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from input_batcher.controller import InputBatcherController


class TestInputBatcherController(unittest.TestCase):
    def setUp(self):
        self.batcher = Mock()
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = InputBatcherController(
            batcher=self.batcher,
            messaging=self.messaging,
        )

        self.article = RoutedArticleMessage(
            article={"name": "Example article"},
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.batch = ArticleBatchMessage(
            articles=[{"name": "Example article"}],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_accumulates_article_without_publishing_incomplete_batch(self):
        self.batcher.add_article.return_value = None

        self.controller.process_message(
            self.article,
            self.ack,
            self.nack,
        )

        self.batcher.add_article.assert_called_once_with(self.article)
        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_completed_batch(self):
        self.batcher.add_article.return_value = self.batch

        self.controller.process_message(
            self.article,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(self.batch)
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_pending_batch_before_input_end(self):
        self.batcher.flush.return_value = self.batch

        self.controller.process_message(
            self.input_end,
            self.ack,
            self.nack,
        )

        self.batcher.flush.assert_called_once_with(self.input_end)
        self.assertEqual(
            self.messaging.publish.call_args_list,
            [
                call(self.batch),
                call(self.input_end),
            ],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_input_end_without_an_empty_batch(self):
        self.batcher.flush.return_value = None

        self.controller.process_message(
            self.input_end,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(self.input_end)
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_nacks_and_propagates_processing_error(self):
        error = ValueError("Article is too large")
        self.batcher.add_article.side_effect = error

        with self.assertRaises(ValueError) as raised:
            self.controller.process_message(
                self.article,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.nack.assert_called_once()
        self.ack.assert_not_called()
        self.messaging.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
