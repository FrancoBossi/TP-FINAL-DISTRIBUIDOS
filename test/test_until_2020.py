import unittest

from common.message_protocol.message import (
    Q1Q2ArticleBatchMessage,
    RoutedInputEndMessage,
)
from until_2020.filter import filter_batch, is_until_2020


class TestUntil2020Filter(unittest.TestCase):
    def test_includes_the_entire_2020_year(self):
        self.assertTrue(is_until_2020({"modification_date": "2020-12-31T23:59:59"}))

    def test_excludes_articles_from_2021(self):
        self.assertFalse(is_until_2020({"modification_date": "2021-01-01T00:00:00"}))

    def test_filters_a_batch_without_changing_metadata(self):
        message = Q1Q2ArticleBatchMessage(
            articles=[
                {"url": "old", "modification_date": "2020-01-01T00:00:00"},
                {"url": "new", "modification_date": "2021-01-01T00:00:00"},
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.assertEqual(
            filter_batch(message),
            Q1Q2ArticleBatchMessage(
                articles=[message.articles[0]],
                execution_id=message.execution_id,
                reply_queue=message.reply_queue,
            ),
        )

    def test_rejects_invalid_dates(self):
        with self.assertRaises(ValueError):
            is_until_2020({"modification_date": "not-a-date"})


class TestUntil2020Controller(unittest.TestCase):
    def setUp(self):
        from unittest.mock import Mock

        from until_2020.controller import Until2020Controller

        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()
        self.controller = Until2020Controller(self.messaging)
        self.batch = Q1Q2ArticleBatchMessage(
            articles=[{"modification_date": "2020-01-01T00:00:00"}],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_publishes_filtered_batch_to_both_queries(self):
        self.controller.process_message(self.batch, self.ack, self.nack)

        expected = filter_batch(self.batch)
        self.messaging.publish_q1.assert_called_once_with(expected)
        self.messaging.publish_q2.assert_called_once_with(expected)
        self.ack.assert_called_once()

    def test_publishes_input_end_to_both_queries(self):
        self.controller.process_message(self.input_end, self.ack, self.nack)

        self.messaging.publish_q1.assert_called_once_with(self.input_end)
        self.messaging.publish_q2.assert_called_once_with(self.input_end)
        self.ack.assert_called_once()

    def test_nacks_when_second_publication_fails(self):
        error = RuntimeError("q2 unavailable")
        self.messaging.publish_q2.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertIs(raised.exception, error)
        self.messaging.publish_q1.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()
