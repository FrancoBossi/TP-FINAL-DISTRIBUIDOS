import unittest
from unittest.mock import Mock, patch

from common.message_protocol.message import (
    PartitionEndMessage,
    TokenizedArticleBatchMessage,
)
from word_extractor.messaging import WordExtractorMessaging


class TestWordExtractorMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.q3_output_queue = Mock()
        self.q4_q5_output_queue = Mock()
        self.control_exchange_input = Mock()
        self.control_exchange_output = Mock()

        self.consumer_patcher = patch("word_extractor.messaging.CoordinatedConsumer")
        consumer_class = self.consumer_patcher.start()
        self.consumer = consumer_class.return_value
        self.addCleanup(self.consumer_patcher.stop)

        self.messaging = WordExtractorMessaging(
            input_queue=self.input_queue,
            q3_output_queue=self.q3_output_queue,
            q4_q5_output_queue=self.q4_q5_output_queue,
            control_exchange_input=self.control_exchange_input,
            control_exchange_output=self.control_exchange_output,
        )

        consumer_class.assert_called_once_with(
            input_queue=self.input_queue,
            control_exchange_input=self.control_exchange_input,
        )

    def test_publishes_each_message_to_its_output_queue(self):
        message = TokenizedArticleBatchMessage(
            articles=[
                {
                    "article_id": "article-1",
                    "url": "https://example.com/article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "words": ["network", "and"],
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.publish_q3(message)
        self.messaging.publish_q4_q5(message)

        self.q3_output_queue.send.assert_called_once_with(message.serialize())
        self.q4_q5_output_queue.send.assert_called_once_with(message.serialize())

    def test_broadcasts_serialized_control_message(self):
        message = PartitionEndMessage(
            stage="query_router",
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

    def test_stops_consumption_and_closes_all_queues(self):
        self.messaging.stop_consuming()
        self.messaging.close()

        self.consumer.stop_consuming.assert_called_once()
        self.consumer.close.assert_called_once()
        self.q3_output_queue.close.assert_called_once()
        self.q4_q5_output_queue.close.assert_called_once()
        self.control_exchange_output.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
