import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call

from common.message_identity import build_control_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    NormalizedArticleBatchMessage,
    PartitionEndMessage,
)
from common.persistence import PersistenceLayout
from query_router.controller import QueryRouterController


class TestQueryRouterFaultRecovery(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.layout = PersistenceLayout(
            Path(self.temporary_directory.name),
            "query_router",
            1,
        )

    def _controller(self, messaging: Mock, partition_count: int = 2):
        return QueryRouterController(
            messaging=messaging,
            partition_id=1,
            preprocessor_partition_count=partition_count,
            persistence_layout=self.layout,
            q1_q2_destination="until_2020.input",
            text_destination="word_extractor.input",
        )

    @staticmethod
    def _end(partition_id: int) -> PartitionEndMessage:
        return PartitionEndMessage(
            stage="preprocessor",
            partition_id=partition_id,
            execution_id="execution-recovery",
            reply_queue="results.execution-recovery",
            message_id=build_control_message_id(
                execution_id="execution-recovery",
                stage="preprocessor",
                partition_id=partition_id,
            ),
        )

    def test_cleanup_discards_late_data_and_controls_after_restart(self):
        controller = self._controller(Mock())
        end = self._end(0)
        controller.process_control_message(end)
        controller.process_control_message(
            ExecutionCleanupMessage(end.execution_id, "cleanup", "client_disconnected")
        )

        messaging = Mock()
        restarted = self._controller(messaging)
        restarted.start()
        restarted.process_control_message(self._end(1))
        ack, nack = Mock(), Mock()
        restarted.process_message(
            NormalizedArticleBatchMessage([], end.execution_id, end.reply_queue),
            ack,
            nack,
        )

        self.assertFalse(self.layout.state_path(end.execution_id).parent.exists())
        ack.assert_called_once()
        nack.assert_not_called()
        messaging.publish_q1_q2.assert_not_called()
        messaging.publish_text.assert_not_called()

    def test_restart_restores_incomplete_barrier(self):
        first_messaging = Mock()
        self._controller(first_messaging).process_control_message(self._end(0))
        first_messaging.publish_q1_q2.assert_not_called()
        first_messaging.publish_text.assert_not_called()

        recovered_messaging = Mock()
        self._controller(recovered_messaging).process_control_message(self._end(1))

        self.assertEqual(
            [
                call.publish_q1_q2(unittest.mock.ANY),
                call.publish_text(unittest.mock.ANY),
            ],
            recovered_messaging.method_calls,
        )

    def test_restart_publishes_only_unconfirmed_fanout_destination(self):
        first_messaging = Mock()
        first_messaging.publish_text.side_effect = RuntimeError("text unavailable")
        controller = self._controller(first_messaging, partition_count=1)

        with self.assertRaisesRegex(RuntimeError, "text unavailable"):
            controller.process_control_message(self._end(0))

        first_messaging.publish_q1_q2.assert_called_once()
        first_messaging.publish_text.assert_called_once()

        recovered_messaging = Mock()
        self._controller(recovered_messaging, partition_count=1).start()

        recovered_messaging.publish_q1_q2.assert_not_called()
        recovered_messaging.publish_text.assert_called_once()
        recovered_messaging.start_consuming.assert_called_once()

    def test_redelivery_after_confirmed_outputs_does_not_republish(self):
        messaging = Mock()
        controller = self._controller(messaging, partition_count=1)
        message = self._end(0)
        controller.process_control_message(message)
        messaging.reset_mock()

        controller.process_control_message(message)

        messaging.publish_q1_q2.assert_not_called()
        messaging.publish_text.assert_not_called()


if __name__ == "__main__":
    unittest.main()
