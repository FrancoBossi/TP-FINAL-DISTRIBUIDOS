import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call

from common.message_protocol.message import (
    ArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from common.persistence import PersistenceLayout
from input_batcher.batcher import BatchAccumulator
from input_batcher.controller import InputBatcherController


class TestInputBatcherController(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.batcher = Mock()
        self.batcher.export_state.return_value = {}
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = InputBatcherController(
            batcher=self.batcher,
            messaging=self.messaging,
            persistence_layout=PersistenceLayout(
                Path(self.temporary_directory.name),
                "input_batcher",
                0,
            ),
            output_destination="preprocessor.input",
        )

        self.article = RoutedArticleMessage(
            article={"name": "Example article"},
            execution_id="execution-123",
            reply_queue="results.execution-123",
            message_id="m1:article",
        )
        self.batch = ArticleBatchMessage(
            articles=[{"name": "Example article"}],
            execution_id="execution-123",
            reply_queue="results.execution-123",
            message_id="m1:batch",
        )
        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
            message_id="m1:input-end",
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

    def test_duplicate_article_is_not_added_twice(self):
        batcher = BatchAccumulator(4096)
        controller = InputBatcherController(
            batcher=batcher,
            messaging=self.messaging,
            persistence_layout=PersistenceLayout(
                Path(self.temporary_directory.name),
                "deduplicated_input_batcher",
                0,
            ),
            output_destination="preprocessor.input",
        )

        controller.process_message(self.article, Mock(), Mock())
        controller.process_message(self.article, Mock(), Mock())
        controller.process_message(self.input_end, Mock(), Mock())

        published = [call.args[0] for call in self.messaging.publish.call_args_list]
        self.assertEqual(1, len(published[0].articles))
        self.assertEqual([self.article.article], published[0].articles)

    def test_restart_recovers_outbox_without_reapplying_input_end(self):
        layout = PersistenceLayout(
            Path(self.temporary_directory.name),
            "recoverable_input_batcher",
            0,
        )
        batcher = BatchAccumulator(4096)
        failing_messaging = Mock()
        controller = InputBatcherController(
            batcher=batcher,
            messaging=failing_messaging,
            persistence_layout=layout,
            output_destination="preprocessor.input",
        )
        controller.process_message(self.article, Mock(), Mock())
        failing_messaging.publish.side_effect = RuntimeError("broker unavailable")

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            controller.process_message(self.input_end, Mock(), Mock())

        recovered_batcher = BatchAccumulator(4096)
        recovered_messaging = Mock()
        recovered = InputBatcherController(
            batcher=recovered_batcher,
            messaging=recovered_messaging,
            persistence_layout=layout,
            output_destination="preprocessor.input",
        )
        recovered.start()

        published = [
            call.args[0] for call in recovered_messaging.publish.call_args_list
        ]
        self.assertEqual([self.article.article], published[0].articles)
        self.assertEqual(self.input_end, published[1])
        self.assertEqual({}, recovered_batcher.current_batch)
        recovered_messaging.start_consuming.assert_called_once()

        redelivery_ack = Mock()
        recovered.process_message(self.input_end, redelivery_ack, Mock())
        self.assertEqual(2, recovered_messaging.publish.call_count)
        redelivery_ack.assert_called_once()


if __name__ == "__main__":
    unittest.main()
