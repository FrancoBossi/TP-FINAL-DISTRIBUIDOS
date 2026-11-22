import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.message_protocol.message import (
    ExecutionCleanupMessage,
    PartitionEndMessage,
    Q5AverageMessage,
    Q5CandidateBatchMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from common.persistence import PersistenceLayout
from q5.comparator.buffer import CandidateBuffer
from q5.comparator.controller import Q5ComparatorController


class TestQ5ComparatorRecovery(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.layout = PersistenceLayout(root / "state", "q5_comparator", 0)
        self.buffer = CandidateBuffer(root / "candidates")

    def _controller(self, messaging):
        return Q5ComparatorController(
            messaging,
            self.buffer,
            partition_id=0,
            date_router_partition_count=1,
            persistence_layout=self.layout,
        )

    def test_cleanup_removes_candidate_file_and_discards_late_batch(self):
        message = Q5CandidateBatchMessage(
            [{"article_id": "a", "url": "https://a", "word_count": 5}],
            "execution-1",
            "results.execution-1",
            "candidate-batch",
        )
        controller = self._controller(Mock())
        controller.process_message(message, Mock(), Mock())
        self.assertTrue(list(self.buffer.iterate(message.execution_id)))

        ack, nack = Mock(), Mock()
        controller.process_message(
            ExecutionCleanupMessage(
                message.execution_id, "cleanup", "client_disconnected"
            ),
            ack,
            nack,
        )
        messaging = Mock()
        restarted = self._controller(messaging)
        restarted.start()
        restarted.process_message(message, ack, nack)

        self.assertFalse(self.layout.state_path(message.execution_id).parent.exists())
        self.assertEqual(list(self.buffer.iterate(message.execution_id)), [])
        self.assertEqual(ack.call_count, 2)
        nack.assert_not_called()
        messaging.publish.assert_not_called()

    def test_restart_restores_candidates_and_final_before_average(self):
        controller = self._controller(Mock())
        controller.process_message(
            Q5CandidateBatchMessage(
                [{"article_id": "a", "url": "https://a", "word_count": 5}],
                "execution-1",
                "results.execution-1",
                "candidate-batch",
            ),
            Mock(),
            Mock(),
        )
        controller.process_message(
            PartitionEndMessage(
                "q5_date_router",
                0,
                "execution-1",
                "results.execution-1",
                "date-router-end",
            ),
            Mock(),
            Mock(),
        )

        recovered_messaging = Mock()
        self._controller(recovered_messaging).process_message(
            Q5AverageMessage(
                3,
                "execution-1",
                "results.execution-1",
                "average",
            ),
            Mock(),
            Mock(),
        )

        published = [
            call.args[0] for call in recovered_messaging.publish.call_args_list
        ]
        self.assertEqual(len(published), 2)
        self.assertIsInstance(published[0], ResultMessage)
        self.assertIsInstance(published[1], QueryPartitionEndMessage)
        self.assertEqual(list(self.buffer.iterate("execution-1")), [])


if __name__ == "__main__":
    unittest.main()
