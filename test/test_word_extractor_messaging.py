import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    RoutedInputEndMessage,
    TokenizedArticleBatchMessage,
)
from word_extractor.messaging import WordExtractorMessaging


class TestWordExtractorMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.q3_output_queue = Mock()
        self.q4_q5_output_queue = Mock()

        self.messaging = WordExtractorMessaging(
            input_queue=self.input_queue,
            q3_output_queue=self.q3_output_queue,
            q4_q5_output_queue=self.q4_q5_output_queue,
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
        self.q3_output_queue.close.assert_called_once()
        self.q4_q5_output_queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
