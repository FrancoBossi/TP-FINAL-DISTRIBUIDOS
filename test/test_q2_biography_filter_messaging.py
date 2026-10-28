import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    RoutedInputEndMessage,
)
from q2.biography_filter.messaging import Q2BiographyFilterMessaging


class TestQ2BiographyFilterMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.output_queue = Mock()

        self.messaging = Q2BiographyFilterMessaging(
            input_queue=self.input_queue,
            output_queue=self.output_queue,
        )

    def test_publishes_serialized_message(self):
        message = ImageCandidateBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "image_url": "https://example.com/image.jpg",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.publish(message)

        self.output_queue.send.assert_called_once_with(message.serialize())

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

    def test_stops_consumption_and_closes_both_queues(self):
        self.messaging.stop_consuming()
        self.messaging.close()

        self.input_queue.stop_consuming.assert_called_once()
        self.input_queue.close.assert_called_once()
        self.output_queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
