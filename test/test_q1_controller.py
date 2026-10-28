import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    Q2CandidateBatchMessage,
    QueryEndMessage,
    RoutedInputEndMessage,
)
from q1.controller import Q1Controller
from q1.filter import build_query1_results


class TestQ1Controller(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = Q1Controller(
            messaging=self.messaging,
        )

        self.batch = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/english",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                },
                {
                    "url": "https://example.com/french",
                    "language": "fr",
                    "modification_date": "2019-01-01T00:00:00",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_publishes_query1_results(self):
        expected_results = build_query1_results(self.batch)

        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

        self.assertEqual(
            self.messaging.method_calls,
            [call.publish(result) for result in expected_results],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_nothing_when_batch_has_no_english_articles(self):
        batch = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/french",
                    "language": "fr",
                    "modification_date": "2020-01-01T00:00:00",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.controller.process_message(
            batch,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_query_end_when_input_ends(self):
        self.controller.process_message(
            self.input_end,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(
            QueryEndMessage(
                query_id=1,
                execution_id="execution-123",
            )
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_nacks_when_publication_fails(self):
        error = RuntimeError("result sink unavailable")
        self.messaging.publish.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.messaging.publish.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = Q2CandidateBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Q1_CANDIDATE_BATCH or ROUTED_INPUT_END",
        ):
            self.controller.process_message(
                message,
                self.ack,
                self.nack,
            )

        self.messaging.publish.assert_not_called()
        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
