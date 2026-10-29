import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    AckMessage,
    Q3WordPresenceBatchMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedInputEndMessage,
)
from q3.word_aggregator.controller import Q3WordAggregatorController


class TestQ3WordAggregator(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.controller = Q3WordAggregatorController(self.messaging)
        self.ack, self.nack = Mock(), Mock()

    def send(self, presences, execution="run"):
        self.controller.process_message(
            Q3WordPresenceBatchMessage(presences, execution, f"results.{execution}"),
            self.ack,
            self.nack,
        )

    def test_both_orders_batches_and_repetitions_emit_once(self):
        for first, second in (("en", "fr"), ("fr", "en")):
            with self.subTest(first=first):
                self.setUp()
                self.send([{"word": "café", "language": first}] * 2)
                self.messaging.publish.assert_not_called()
                self.send([{"word": "café", "language": second}] * 2)
                self.send([{"word": "café", "language": first}])
                self.messaging.publish.assert_called_once_with(
                    ResultMessage(3, {"word": "café"}, "run")
                )
                self.nack.assert_not_called()

    def test_isolates_words_and_executions(self):
        self.send(
            [{"word": "and", "language": "en"}, {"word": "other", "language": "fr"}],
            "a",
        )
        self.send([{"word": "and", "language": "fr"}], "b")
        self.messaging.publish.assert_not_called()
        self.send([{"word": "and", "language": "fr"}], "a")
        self.messaging.publish.assert_called_once_with(
            ResultMessage(3, {"word": "and"}, "a")
        )

    def test_end_follows_results_and_cleans_only_completed_execution(self):
        events = Mock()
        events.attach_mock(self.messaging, "messaging")
        events.attach_mock(self.ack, "ack")
        self.send(
            [
                {"word": "network", "language": "en"},
                {"word": "network", "language": "fr"},
            ],
            "a",
        )
        self.send([{"word": "network", "language": "en"}], "b")
        self.controller.process_message(
            RoutedInputEndMessage("a", "reply"), self.ack, self.nack
        )
        self.assertNotIn("a", self.controller._executions)
        self.assertIn("b", self.controller._executions)
        self.assertEqual(
            [
                call.messaging.publish(ResultMessage(3, {"word": "network"}, "a")),
                call.ack(),
                call.ack(),
                call.messaging.publish(QueryEndMessage(3, "a")),
                call.ack(),
            ],
            events.mock_calls,
        )
        self.send([{"word": "network", "language": "fr"}], "b")
        self.assertEqual(
            call.publish(ResultMessage(3, {"word": "network"}, "b")),
            self.messaging.mock_calls[-1],
        )

    def test_empty_execution_finishes(self):
        self.send([])
        self.controller.process_message(
            RoutedInputEndMessage("run", "reply"), self.ack, self.nack
        )
        self.messaging.publish.assert_called_once_with(QueryEndMessage(3, "run"))
        self.assertEqual({}, self.controller._executions)

    def test_failed_result_can_be_retried_without_losing_emission(self):
        self.send([{"word": "x", "language": "en"}])
        self.ack.reset_mock()
        self.messaging.publish.side_effect = RuntimeError("unavailable")
        with self.assertRaises(RuntimeError):
            self.send([{"word": "x", "language": "fr"}])
        self.ack.assert_not_called()
        self.nack.assert_called_once()
        self.messaging.publish.side_effect = None
        self.send([{"word": "x", "language": "fr"}])
        self.assertEqual(
            call.publish(ResultMessage(3, {"word": "x"}, "run")),
            self.messaging.mock_calls[-1],
        )

    def test_failed_end_retains_state(self):
        self.send([{"word": "x", "language": "en"}])
        self.ack.reset_mock()
        self.messaging.publish.side_effect = RuntimeError("unavailable")
        with self.assertRaises(RuntimeError):
            self.controller.process_message(
                RoutedInputEndMessage("run", "reply"), self.ack, self.nack
            )
        self.assertIn("run", self.controller._executions)
        self.ack.assert_not_called()
        self.nack.assert_called_once()

    def test_rejects_unexpected_messages(self):
        with self.assertRaises(ValueError):
            self.controller.process_message(AckMessage(), self.ack, self.nack)
        self.ack.assert_not_called()
        self.nack.assert_called_once()

    def test_ignores_other_languages_and_continues_batch(self):
        self.send([{"word": "ignored", "language": "es"}])
        self.assertEqual({}, self.controller._executions)
        self.messaging.publish.assert_not_called()
        self.send(
            [
                {"word": "x", "language": "en"},
                {"word": "x", "language": "es"},
                {"word": "x", "language": "fr"},
            ]
        )
        self.messaging.publish.assert_called_once_with(
            ResultMessage(3, {"word": "x"}, "run")
        )
        self.assertEqual(2, self.ack.call_count)
        self.nack.assert_not_called()

    def test_delegates_start_stop(self):
        self.controller.start()
        self.controller.stop()
        self.messaging.start_consuming.assert_called_once_with(
            self.controller.process_message
        )
        self.messaging.stop_consuming.assert_called_once()
