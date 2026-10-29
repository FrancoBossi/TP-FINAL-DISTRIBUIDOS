import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    Q2CandidateBatchMessage,
    RoutedInputEndMessage,
)
from until_2020_filter.messaging import Until2020FilterMessaging


class TestUntil2020FilterMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.q1_output_queue = Mock()
        self.q2_output_queue = Mock()

        self.messaging = Until2020FilterMessaging(
            input_queue=self.input_queue,
            q1_output_queue=self.q1_output_queue,
            q2_output_queue=self.q2_output_queue,
        )

    def test_publishes_each_message_to_its_output_queue(self):
        q1_message = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        q2_message = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "sections": ["Biography"],
                    "image_url": None,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.publish_q1(q1_message)
        self.messaging.publish_q2(q2_message)

        self.q1_output_queue.send.assert_called_once_with(q1_message.serialize())
        self.q2_output_queue.send.assert_called_once_with(q2_message.serialize())

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
        self.q1_output_queue.close.assert_called_once()
        self.q2_output_queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
