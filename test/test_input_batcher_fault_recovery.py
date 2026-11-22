import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from common.message_protocol.message import (
    ArticleBatchMessage,
    ExecutionCleanupMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from common.persistence import AtomicJsonStore, DurableExecutionStore, PersistenceLayout
from input_batcher.batcher import BatchAccumulator
from input_batcher.controller import InputBatcherController
from input_batcher.journal import ArticleJournal


class InputBatcherFaultRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.layout = PersistenceLayout(
            Path(self.temporary_directory.name),
            "input_batcher",
            0,
        )
        self.article = RoutedArticleMessage(
            article={"name": "Network"},
            execution_id="execution-fault",
            reply_queue="results.execution-fault",
            message_id="m1:article",
        )
        self.input_end = RoutedInputEndMessage(
            execution_id="execution-fault",
            reply_queue="results.execution-fault",
            message_id="m1:input-end",
        )

    def _controller(
        self,
        messaging: Mock | None = None,
        batcher: BatchAccumulator | None = None,
    ) -> tuple[InputBatcherController, BatchAccumulator, Mock]:
        messaging = Mock() if messaging is None else messaging
        batcher = BatchAccumulator(4096) if batcher is None else batcher
        return (
            InputBatcherController(
                batcher=batcher,
                messaging=messaging,
                persistence_layout=self.layout,
                output_destination="preprocessor.input",
            ),
            batcher,
            messaging,
        )

    def _durable_store(self) -> DurableExecutionStore:
        return DurableExecutionStore(
            AtomicJsonStore(self.layout.state_path(self.article.execution_id))
        )

    def test_cleanup_discards_batch_and_late_article_after_restart(self):
        controller, batcher, messaging = self._controller()
        controller.process_message(self.article, Mock(), Mock())
        execution_id = self.article.execution_id
        path = self.layout.state_path(execution_id)
        self.assertTrue(path.with_name("pending-0.jsonl").exists())

        cleanup = ExecutionCleanupMessage(
            execution_id, "cleanup", "client_disconnected"
        )
        ack, nack = Mock(), Mock()
        controller.process_message(cleanup, ack, nack)
        self.assertNotIn(execution_id, batcher.current_batch)
        self.assertFalse(path.parent.exists())
        messaging.publish.assert_not_called()

        restarted, batcher, messaging = self._controller()
        restarted.start()
        restarted.process_message(self.article, ack, nack)
        self.assertNotIn(execution_id, batcher.current_batch)
        self.assertFalse(path.parent.exists())
        self.assertEqual(ack.call_count, 2)
        nack.assert_not_called()
        messaging.publish.assert_not_called()

    def test_failure_before_commit_rolls_back_memory_and_retry_applies_once(self):
        controller, batcher, messaging = self._controller()
        ack, nack = Mock(), Mock()

        with patch.object(
            ArticleJournal,
            "append",
            side_effect=OSError("disk unavailable"),
        ):
            with self.assertRaisesRegex(OSError, "disk unavailable"):
                controller.process_message(self.article, ack, nack)

        self.assertEqual({}, batcher.current_batch)
        ack.assert_not_called()
        nack.assert_called_once()
        messaging.publish.assert_not_called()

        retry_ack, retry_nack = Mock(), Mock()
        controller.process_message(self.article, retry_ack, retry_nack)

        self.assertEqual(
            [self.article.article],
            batcher.current_batch[self.article.execution_id].articles,
        )
        retry_ack.assert_called_once()
        retry_nack.assert_not_called()

    def test_restart_after_commit_retries_outbox_without_reapplying_end(self):
        controller, _, messaging = self._controller()
        controller.process_message(self.article, Mock(), Mock())
        messaging.publish.side_effect = RuntimeError("broker unavailable")

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            controller.process_message(self.input_end, Mock(), Mock())

        persisted = self._durable_store()
        self.assertTrue(persisted.has_applied(self.input_end.message_id))
        self.assertEqual(2, len(persisted.pending_outputs))
        self.assertEqual({"journal_generation": 1}, persisted.state)

        recovered, batcher, recovered_messaging = self._controller()
        recovered.start()

        recovered_messages = [
            call.args[0] for call in recovered_messaging.publish.call_args_list
        ]
        self.assertIsInstance(recovered_messages[0], ArticleBatchMessage)
        self.assertEqual(self.input_end, recovered_messages[1])
        self.assertEqual({}, batcher.current_batch)

    def test_failure_mid_fanout_recovers_only_unconfirmed_end(self):
        controller, _, messaging = self._controller()
        controller.process_message(self.article, Mock(), Mock())
        messaging.publish.side_effect = [None, RuntimeError("end unavailable")]

        with self.assertRaisesRegex(RuntimeError, "end unavailable"):
            controller.process_message(self.input_end, Mock(), Mock())

        persisted = self._durable_store()
        self.assertEqual(1, len(persisted.pending_outputs))
        self.assertEqual(
            self.input_end.message_id,
            persisted.pending_outputs[0].message_id,
        )

        recovered, _, recovered_messaging = self._controller()
        recovered.start()

        recovered_messaging.publish.assert_called_once_with(self.input_end)

    def test_lost_ack_redelivery_does_not_republish_confirmed_outputs(self):
        controller, _, messaging = self._controller()
        controller.process_message(self.article, Mock(), Mock())
        lost_ack = Mock(side_effect=RuntimeError("ack connection lost"))

        with self.assertRaisesRegex(RuntimeError, "ack connection lost"):
            controller.process_message(self.input_end, lost_ack, Mock())

        self.assertEqual(2, messaging.publish.call_count)
        persisted = self._durable_store()
        self.assertTrue(persisted.has_applied(self.input_end.message_id))
        self.assertEqual((), persisted.pending_outputs)

        recovered, _, recovered_messaging = self._controller()
        recovered.start()
        redelivery_ack, redelivery_nack = Mock(), Mock()
        recovered.process_message(
            self.input_end,
            redelivery_ack,
            redelivery_nack,
        )

        recovered_messaging.publish.assert_not_called()
        redelivery_ack.assert_called_once()
        redelivery_nack.assert_not_called()

    def test_journal_appends_without_snapshot_and_recovers_after_lost_ack(self):
        controller, _, messaging = self._controller()
        ack = Mock(side_effect=RuntimeError("ack lost"))
        with patch.object(
            AtomicJsonStore, "save", side_effect=AssertionError("snapshot write")
        ):
            with self.assertRaisesRegex(RuntimeError, "ack lost"):
                controller.process_message(self.article, ack, Mock())

        path = self.layout.state_path(self.article.execution_id)
        journal = path.with_name("pending-0.jsonl")
        self.assertFalse(path.exists())
        self.assertEqual(self.article.serialize() + b"\n", journal.read_bytes())
        with journal.open("ab") as output:
            output.write(b'{"incomplete":')

        recovered, batcher, _ = self._controller()
        recovered.start()
        recovered.process_message(self.article, Mock(), Mock())
        self.assertEqual(self.article.serialize() + b"\n", journal.read_bytes())
        self.assertEqual(
            [self.article.article],
            batcher.current_batch[self.article.execution_id].articles,
        )
        messaging.publish.assert_not_called()

    def test_checkpoint_ignores_old_journal_left_after_rollover(self):
        controller, _, messaging = self._controller(batcher=BatchAccumulator(230))
        controller.process_message(self.article, Mock(), Mock())
        second = RoutedArticleMessage(
            {"name": "Another article"},
            self.article.execution_id,
            self.article.reply_queue,
            "m1:article-2",
        )
        with patch.object(controller, "_remove_obsolete_journal"):
            controller.process_message(second, Mock(), Mock())
        messaging.publish.assert_called_once()

        recovered, batcher, recovered_messaging = self._controller()
        recovered.start()
        recovered_messaging.publish.assert_not_called()
        self.assertEqual(
            [second.article], batcher.current_batch[second.execution_id].articles
        )
        self.assertFalse(
            self.layout.state_path(second.execution_id)
            .with_name("pending-0.jsonl")
            .exists()
        )

    def test_restores_legacy_snapshot_and_journal_together(self):
        batcher = BatchAccumulator(4096)
        batcher.add_article(self.article)
        legacy_store = self._durable_store()
        legacy_store.apply(
            self.article.message_id, batcher.export_state(self.article.execution_id), []
        )
        second = RoutedArticleMessage(
            {"name": "After upgrade"},
            self.article.execution_id,
            self.article.reply_queue,
            "m1:article-2",
        )
        controller, _, _ = self._controller()
        controller.process_message(second, Mock(), Mock())

        recovered, batcher, messaging = self._controller(batcher=BatchAccumulator(200))
        recovered.start()
        recovered.process_message(self.input_end, Mock(), Mock())
        self.assertEqual(
            [self.article.article, second.article],
            messaging.publish.call_args_list[0].args[0].articles,
        )
        self.assertEqual({}, batcher.current_batch)


if __name__ == "__main__":
    unittest.main()
