import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.durable_barrier import DurableBarrier
from common.message_identity import build_control_message_id
from common.message_protocol.message import PartitionEndMessage
from common.persistence import PersistenceLayout


class TestDurableBarrier(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.layout = PersistenceLayout(Path(temporary.name), "barrier", 0)
        self.publish_a = Mock()
        self.publish_b = Mock()

    @staticmethod
    def _input(partition: int) -> PartitionEndMessage:
        return PartitionEndMessage(
            stage="upstream",
            partition_id=partition,
            execution_id="execution-1",
            reply_queue="results.execution-1",
            message_id=build_control_message_id(
                execution_id="execution-1",
                stage="upstream",
                partition_id=partition,
            ),
        )

    @staticmethod
    def _output(message: PartitionEndMessage) -> PartitionEndMessage:
        return PartitionEndMessage(
            stage="downstream",
            partition_id=0,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
            message_id=build_control_message_id(
                execution_id=message.execution_id,
                stage="downstream",
                partition_id=0,
            ),
        )

    def _barrier(self) -> DurableBarrier:
        return DurableBarrier(
            self.layout,
            2,
            {"a": self.publish_a, "b": self.publish_b},
            lambda message: {"a": self._output(message), "b": self._output(message)},
        )

    def test_restart_restores_partial_barrier(self):
        self._barrier().apply(self._input(0))
        self._barrier().apply(self._input(1))
        self.publish_a.assert_called_once()
        self.publish_b.assert_called_once()

    def test_restart_only_retries_unconfirmed_destination(self):
        self.publish_b.side_effect = RuntimeError("unavailable")
        barrier = self._barrier()
        barrier.apply(self._input(0))
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            barrier.apply(self._input(1))

        self.publish_a.reset_mock()
        self.publish_b.reset_mock(side_effect=True)
        self._barrier().recover()
        self.publish_a.assert_not_called()
        self.publish_b.assert_called_once()

    def test_redelivery_after_completion_does_not_republish(self):
        barrier = self._barrier()
        barrier.apply(self._input(0))
        barrier.apply(self._input(1))
        self.publish_a.reset_mock()
        self.publish_b.reset_mock()
        barrier.apply(self._input(1))
        self.publish_a.assert_not_called()
        self.publish_b.assert_not_called()


if __name__ == "__main__":
    unittest.main()
