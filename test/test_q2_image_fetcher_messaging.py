import unittest
from unittest.mock import Mock, patch

from common.message_protocol.message import (
    PartitionEndMessage,
    QueryPartitionEndMessage,
)
from q2.image_fetcher.messaging import ImageFetcherMessaging


class TestImageFetcherMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.output_queue = Mock()
        self.control_exchange_input = Mock()
        self.control_exchange_output = Mock()

        self.consumer_patcher = patch("q2.image_fetcher.messaging.CoordinatedConsumer")
        consumer_class = self.consumer_patcher.start()
        self.consumer = consumer_class.return_value
        self.addCleanup(self.consumer_patcher.stop)

        self.messaging = ImageFetcherMessaging(
            input_queue=self.input_queue,
            output_queue=self.output_queue,
            control_exchange_input=self.control_exchange_input,
            control_exchange_output=self.control_exchange_output,
        )

        consumer_class.assert_called_once_with(
            input_queue=self.input_queue,
            control_exchange_input=self.control_exchange_input,
        )

    def test_publishes_serialized_message(self):
        message = QueryPartitionEndMessage(
            query_id=2,
            partition_id=0,
            execution_id="execution-123",
        )

        self.messaging.publish(message)

        self.output_queue.send.assert_called_once_with(message.serialize())

    def test_broadcasts_serialized_control_message(self):
        message = PartitionEndMessage(
            stage="q2_biography_filter",
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.broadcast_control(message)

        self.control_exchange_output.send.assert_called_once_with(message.serialize())

    def test_delegates_both_callbacks_to_coordinated_consumer(self):
        on_message = Mock()
        on_control_message = Mock()

        self.messaging.start_consuming(on_message, on_control_message)

        self.consumer.start_consuming.assert_called_once_with(
            on_message,
            on_control_message,
        )

    def test_stops_and_closes_consumer_and_output_connections(self):
        self.messaging.stop_consuming()
        self.messaging.close()

        self.consumer.stop_consuming.assert_called_once()
        self.consumer.close.assert_called_once()
        self.output_queue.close.assert_called_once()
        self.control_exchange_output.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
