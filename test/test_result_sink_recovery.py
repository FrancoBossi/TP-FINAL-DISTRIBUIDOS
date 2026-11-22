import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.message_protocol.message import (
    ExecutionCleanupMessage,
    ExecutionFailedMessage,
    QueryEndMessage,
    QueryPartitionEndMessage,
)
from common.persistence import PersistenceLayout
from result_sink.controller import ResultSinkController


class TestResultSinkRecovery(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.layout = PersistenceLayout(Path(temporary.name), "result_sink", 0)
        self.partition_counts = {1: 2, 2: 1, 3: 1, 4: 1, 5: 1}

    def _controller(self, messaging, cleanup_destinations=None):
        return ResultSinkController(
            messaging,
            self.partition_counts,
            persistence_layout=self.layout,
            cleanup_destinations=cleanup_destinations,
        )

    def test_restart_retries_only_unconfirmed_cleanup_destinations(self):
        def destinations(_):
            return ("first.input", "second.input")

        messaging = Mock()
        messaging.publish_cleanup.side_effect = [None, RuntimeError("unavailable")]
        controller = self._controller(messaging, destinations)
        event = ExecutionFailedMessage("execution-1", "terminal", "client_disconnected")
        ack, nack = Mock(), Mock()
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            controller.process_message(event, ack, nack)
        ack.assert_not_called()
        nack.assert_called_once()

        recovered_messaging = Mock()
        recovered = self._controller(recovered_messaging, destinations)
        recovered.start()
        recovered.process_message(event, ack, nack)

        recovered_messaging.publish_cleanup.assert_called_once()
        destination, cleanup = recovered_messaging.publish_cleanup.call_args.args
        self.assertEqual(destination, "second.input")
        self.assertIsInstance(cleanup, ExecutionCleanupMessage)
        self.assertEqual(
            cleanup.message_id, messaging.publish_cleanup.call_args.args[1].message_id
        )
        self.assertEqual(cleanup.cause, event.cause)
        ack.assert_called_once()

    @staticmethod
    def _end(partition_id: int, message_id: str):
        return QueryPartitionEndMessage(1, partition_id, "execution-1", message_id)

    def test_terminal_event_removes_state_and_late_message_is_acknowledged(self):
        controller = self._controller(Mock())
        controller.process_message(self._end(0, "partition-0-end"), Mock(), Mock())
        path = self.layout.state_path("execution-1")
        self.assertTrue(path.exists())

        ack, nack = Mock(), Mock()
        terminal = ExecutionFailedMessage(
            "execution-1", "failed", "client_disconnected"
        )
        controller.process_message(terminal, ack, nack)
        ack.assert_called_once()
        self.assertFalse(path.exists())

        messaging = Mock()
        restarted = self._controller(messaging)
        restarted.start()
        restarted.process_message(terminal, ack, nack)
        restarted.process_message(self._end(1, "partition-1-end"), ack, nack)
        self.assertEqual(ack.call_count, 3)
        nack.assert_not_called()
        messaging.publish.assert_not_called()
        self.assertFalse(path.exists())

    def test_restart_restores_partition_barrier(self):
        self._controller(Mock()).process_message(
            self._end(0, "partition-0-end"), Mock(), Mock()
        )
        recovered_messaging = Mock()
        self._controller(recovered_messaging).process_message(
            self._end(1, "partition-1-end"), Mock(), Mock()
        )

        recovered_messaging.publish.assert_called_once()
        self.assertIsInstance(
            recovered_messaging.publish.call_args.args[0], QueryEndMessage
        )

    def test_restart_drains_query_end_committed_before_publication_failure(self):
        messaging = Mock()
        controller = self._controller(messaging)
        controller.process_message(self._end(0, "partition-0-end"), Mock(), Mock())
        messaging.publish.side_effect = RuntimeError("unavailable")
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            controller.process_message(self._end(1, "partition-1-end"), Mock(), Mock())

        recovered_messaging = Mock()
        self._controller(recovered_messaging).start()

        recovered_messaging.publish.assert_called_once()
        self.assertIsInstance(
            recovered_messaging.publish.call_args.args[0], QueryEndMessage
        )
        recovered_messaging.start_consuming.assert_called_once()


if __name__ == "__main__":
    unittest.main()
