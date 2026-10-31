import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    AckMessage,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
    TokenizedArticleBatchMessage,
)
from word_extractor.controller import WordExtractorController


class TestWordExtractorController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()
        self.controller = WordExtractorController(self.messaging)
        self.batch = TextArticleBatchMessage(
            articles=[
                {
                    "article_id": "a",
                    "url": "https://example.com/a",
                    "language": "en",
                    "modification_date": "2024-01-01T00:00:00",
                    "abstract": "Network NETWORK and",
                }
            ],
            execution_id="run",
            reply_queue="results.run",
        )

    def test_publishes_to_both_branches_before_ack(self):
        events = Mock()
        events.attach_mock(self.messaging, "messaging")
        events.attach_mock(self.ack, "ack")
        expected = TokenizedArticleBatchMessage(
            articles=[
                {
                    "article_id": "a",
                    "url": "https://example.com/a",
                    "language": "en",
                    "modification_date": "2024-01-01T00:00:00",
                    "has_abstract": True,
                    "words": ["network", "network", "and"],
                }
            ],
            execution_id="run",
            reply_queue="results.run",
        )

        self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertEqual(
            events.mock_calls,
            [
                call.messaging.publish_q3(expected),
                call.messaging.publish_q4_q5(expected),
                call.ack(),
            ],
        )
        self.nack.assert_not_called()

    def test_empty_input_still_forwards_end_to_both_branches(self):
        self.controller.process_message(
            TextArticleBatchMessage([], "run", "results.run"), self.ack, self.nack
        )
        self.assertEqual([], self.messaging.mock_calls)
        end = RoutedInputEndMessage("run", "results.run")
        self.controller.process_message(end, self.ack, self.nack)
        self.assertEqual(
            self.messaging.mock_calls,
            [
                call.publish_q3(end),
                call.publish_q4_q5(end),
            ],
        )
        self.assertEqual(2, self.ack.call_count)
        self.nack.assert_not_called()

    def test_publication_failure_never_acks_batch_or_end(self):
        for message in (self.batch, RoutedInputEndMessage("run", "results.run")):
            for method in ("publish_q3", "publish_q4_q5"):
                with self.subTest(message=message, method=method):
                    messaging = Mock()
                    getattr(messaging, method).side_effect = RuntimeError("unavailable")
                    ack, nack = Mock(), Mock()
                    with self.assertRaisesRegex(RuntimeError, "unavailable"):
                        WordExtractorController(messaging).process_message(
                            message, ack, nack
                        )
                    ack.assert_not_called()
                    nack.assert_called_once_with()

    def test_unexpected_message_is_rejected(self):
        with self.assertRaises(ValueError):
            self.controller.process_message(AckMessage(), self.ack, self.nack)
        self.ack.assert_not_called()
        self.nack.assert_called_once_with()
        self.assertEqual([], self.messaging.mock_calls)

    def test_delegates_start_and_stop(self):
        self.controller.start()
        self.controller.stop()
        self.messaging.start_consuming.assert_called_once_with(
            self.controller.process_message
        )
        self.messaging.stop_consuming.assert_called_once_with()
