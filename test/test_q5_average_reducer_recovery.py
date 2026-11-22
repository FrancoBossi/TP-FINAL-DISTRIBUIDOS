import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.message_protocol.message import (
    Q5AverageMessage,
    Q5PartialStatsMessage,
    Q5StatsPartitionEndMessage,
)
from common.persistence import PersistenceLayout
from q5.average_reducer.controller import Q5AverageReducerController


class TestQ5AverageReducerRecovery(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.layout = PersistenceLayout(Path(temporary.name), "q5_average_reducer", 0)

    def _controller(self, messaging):
        return Q5AverageReducerController(
            messaging,
            partition_count=1,
            persistence_layout=self.layout,
            output_destinations=["comparator.0", "comparator.1"],
        )

    def test_restart_only_retries_unconfirmed_fanout_destination(self):
        messaging = Mock()
        self._controller(messaging).process_message(
            Q5PartialStatsMessage(30, 2, "execution-1", "results.execution-1", "m1"),
            Mock(),
            Mock(),
        )

        def publish_to(destination, _message):
            if destination == "comparator.1":
                raise RuntimeError("unavailable")

        messaging.publish_to.side_effect = publish_to
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            self._controller(messaging).process_message(
                Q5StatsPartitionEndMessage(
                    0, "execution-1", "results.execution-1", "m2"
                ),
                Mock(),
                Mock(),
            )

        recovered_messaging = Mock()
        self._controller(recovered_messaging).start()

        recovered_messaging.publish_to.assert_called_once()
        destination, message = recovered_messaging.publish_to.call_args.args
        self.assertEqual(destination, "comparator.1")
        self.assertIsInstance(message, Q5AverageMessage)
        self.assertEqual(message.average_word_count, 15)
        recovered_messaging.start_consuming.assert_called_once()


if __name__ == "__main__":
    unittest.main()
