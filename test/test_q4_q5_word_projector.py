import copy
import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    Q4WordBatchMessage,
    Q5ArticleWordCountBatchMessage,
    RoutedInputEndMessage,
    TokenizedArticleBatchMessage,
)
from q4_q5.word_projector.controller import Q4Q5WordProjectorController
from q4_q5.word_projector.messaging import Q4Q5WordProjectorMessaging
from q4_q5.word_projector.projector import project_batch


class TestQ4Q5WordProjector(unittest.TestCase):
    def test_projects_unique_non_stopwords_for_q4_and_q5(self):
        message = TokenizedArticleBatchMessage(
            articles=[
                {
                    "article_id": "article-a",
                    "url": "https://example.com/a",
                    "modification_date": "2020-01-01T00:00:00",
                    "has_abstract": True,
                    "words": [
                        "the",
                        "network",
                        "network",
                        "système",
                        "le",
                    ],
                },
                {
                    "article_id": "article-b",
                    "url": "https://example.com/b",
                    "modification_date": "2021-01-01T00:00:00",
                    "has_abstract": True,
                    "words": [
                        "network",
                        "data",
                        "and",
                    ],
                },
                {
                    "article_id": "article-empty",
                    "url": "https://example.com/empty",
                    "modification_date": "2019-01-01T00:00:00",
                    "has_abstract": True,
                    "words": [],
                },
                {
                    "article_id": "article-without-abstract",
                    "url": "https://example.com/none",
                    "modification_date": "2022-01-01T00:00:00",
                    "has_abstract": False,
                    "words": [],
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        original = copy.deepcopy(message)

        self.assertEqual(
            (
                Q4WordBatchMessage(
                    words=[
                        "network",
                        "système",
                        "data",
                        "network",
                    ],
                    execution_id="execution-123",
                    reply_queue="results.execution-123",
                ),
                Q5ArticleWordCountBatchMessage(
                    articles=[
                        {
                            "article_id": "article-a",
                            "url": "https://example.com/a",
                            "modification_date": "2020-01-01T00:00:00",
                            "word_count": 2,
                        },
                        {
                            "article_id": "article-b",
                            "url": "https://example.com/b",
                            "modification_date": "2021-01-01T00:00:00",
                            "word_count": 2,
                        },
                        {
                            "article_id": "article-empty",
                            "url": "https://example.com/empty",
                            "modification_date": "2019-01-01T00:00:00",
                            "word_count": 0,
                        },
                    ],
                    execution_id="execution-123",
                    reply_queue="results.execution-123",
                ),
            ),
            project_batch(message),
        )
        self.assertEqual(original, message)

    def test_controller_publishes_both_outputs_before_ack_and_forwards_end(self):
        batch = TokenizedArticleBatchMessage(
            articles=[
                {
                    "article_id": "article-a",
                    "url": "https://example.com/a",
                    "modification_date": "2021-01-01T00:00:00",
                    "has_abstract": True,
                    "words": ["the", "network", "network"],
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        expected_q4, expected_q5 = project_batch(batch)

        messaging = Mock()
        ack = Mock()
        nack = Mock()
        events = Mock()
        events.attach_mock(messaging.publish_q4, "publish_q4")
        events.attach_mock(messaging.publish_q5, "publish_q5")
        events.attach_mock(ack, "ack")

        controller = Q4Q5WordProjectorController(messaging)
        controller.process_message(batch, ack, nack)

        self.assertEqual(
            [
                call.publish_q4(expected_q4),
                call.publish_q5(expected_q5),
                call.ack(),
            ],
            events.mock_calls,
        )

        events.reset_mock()
        end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        controller.process_message(end, ack, nack)

        self.assertEqual(
            [
                call.publish_q4(end),
                call.publish_q5(end),
                call.ack(),
            ],
            events.mock_calls,
        )
        nack.assert_not_called()

    def test_messaging_serializes_outputs_and_deserializes_input(self):
        input_queue = Mock()
        q4_output_queue = Mock()
        q5_output_queue = Mock()
        messaging = Q4Q5WordProjectorMessaging(
            input_queue=input_queue,
            q4_output_queue=q4_output_queue,
            q5_output_queue=q5_output_queue,
        )

        q4_message = Q4WordBatchMessage(
            words=["network"],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        q5_message = Q5ArticleWordCountBatchMessage(
            articles=[
                {
                    "article_id": "article-a",
                    "url": "https://example.com/a",
                    "modification_date": "2021-01-01T00:00:00",
                    "word_count": 1,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        messaging.publish_q4(q4_message)
        messaging.publish_q5(q5_message)

        q4_output_queue.send.assert_called_once_with(q4_message.serialize())
        q5_output_queue.send.assert_called_once_with(q5_message.serialize())

        callback = Mock()
        ack = Mock()
        nack = Mock()
        messaging.start_consuming(callback)

        middleware_callback = input_queue.start_consuming.call_args.args[0]
        input_message = TokenizedArticleBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        middleware_callback(input_message.serialize(), ack, nack)

        callback.assert_called_once_with(input_message, ack, nack)

        messaging.stop_consuming()
        messaging.close()

        input_queue.stop_consuming.assert_called_once()
        input_queue.close.assert_called_once()
        q4_output_queue.close.assert_called_once()
        q5_output_queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
