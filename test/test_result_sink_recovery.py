import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.message_protocol.message import QueryEndMessage, QueryPartitionEndMessage
from common.persistence import PersistenceLayout
from result_sink.controller import ResultSinkController


class TestResultSinkRecovery(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.layout = PersistenceLayout(Path(temporary.name), "result_sink", 0)
        self.partition_counts = {1: 2, 2: 1, 3: 1, 4: 1, 5: 1}

    def _controller(self, messaging):
        return ResultSinkController(
            messaging,
            self.partition_counts,
            persistence_layout=self.layout,
        )

    @staticmethod
    def _end(partition_id: int, message_id: str):
        return QueryPartitionEndMessage(1, partition_id, "execution-1", message_id)

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
