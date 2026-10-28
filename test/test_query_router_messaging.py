import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    Q1Q2ArticleBatchMessage,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
)
from query_router.messaging import QueryRouterMessaging


class TestQueryRouterMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.q1_q2_output_queue = Mock()
        self.text_output_queue = Mock()

        self.messaging = QueryRouterMessaging(
            input_queue=self.input_queue,
            q1_q2_output_queue=self.q1_q2_output_queue,
            text_output_queue=self.text_output_queue,
        )

    def test_publishes_each_message_to_its_output_queue(self):
        q1_q2_message = Q1Q2ArticleBatchMessage(
            articles=[{"name": "Example article"}],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        text_message = TextArticleBatchMessage(
            articles=[{"article_id": "article-1"}],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.publish_q1_q2(q1_q2_message)
        self.messaging.publish_text(text_message)

        self.q1_q2_output_queue.send.assert_called_once_with(q1_q2_message.serialize())
        self.text_output_queue.send.assert_called_once_with(text_message.serialize())

    def test_deserializes_input_and_preserves_acknowledgements(self):
        on_message = Mock()
        ack = Mock()
        nack = Mock()

        self.messaging.start_consuming(on_message)

        self.input_queue.start_consuming.assert_called_once()
        middleware_callback = self.input_queue.start_consuming.call_args.args[0]

        message = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        middleware_callback(message.serialize(), ack, nack)

        on_message.assert_called_once_with(message, ack, nack)

    def test_stops_consumption_and_closes_all_queues(self):
        self.messaging.stop_consuming()
        self.messaging.close()

        self.input_queue.stop_consuming.assert_called_once()
        self.input_queue.close.assert_called_once()
        self.q1_q2_output_queue.close.assert_called_once()
        self.text_output_queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
