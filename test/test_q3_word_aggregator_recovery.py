import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from common.message_protocol.message import Q3WordPresenceBatchMessage, ResultMessage
from common.persistence import PersistenceLayout
from q3.word_aggregator.controller import Q3WordAggregatorController


class TestQ3WordAggregatorRecovery(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.layout = PersistenceLayout(Path(temporary.name), "q3_word_aggregator", 0)

    def _controller(self, messaging):
        return Q3WordAggregatorController(
            messaging,
            persistence_layout=self.layout,
            output_destination="result_sink.input",
        )

    @staticmethod
    def _batch(language: str, message_id: str):
        return Q3WordPresenceBatchMessage(
            [{"word": "network", "language": language}],
            "execution-1",
            "results.execution-1",
            message_id,
        )

    def test_restart_restores_word_presence(self):
        self._controller(Mock()).process_message(
            self._batch("en", "m1:en"), Mock(), Mock()
        )
        recovered_messaging = Mock()
        self._controller(recovered_messaging).process_message(
            self._batch("fr", "m1:fr"), Mock(), Mock()
        )
        published = recovered_messaging.publish.call_args.args[0]
        self.assertIsInstance(published, ResultMessage)
        self.assertEqual({"word": "network"}, published.record)

    def test_restart_drains_result_committed_before_publication_failure(self):
        messaging = Mock()
        controller = self._controller(messaging)
        controller.process_message(self._batch("en", "m1:en"), Mock(), Mock())
        messaging.publish.side_effect = RuntimeError("unavailable")
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            controller.process_message(self._batch("fr", "m1:fr"), Mock(), Mock())

        recovered_messaging = Mock()
        self._controller(recovered_messaging).start()
        recovered_messaging.publish.assert_called_once()
        recovered_messaging.start_consuming.assert_called_once()


if __name__ == "__main__":
    unittest.main()
