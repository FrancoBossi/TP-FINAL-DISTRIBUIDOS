import copy
import unittest
from unittest.mock import Mock, call, patch

from common.message_protocol.message import (
    PartitionEndMessage,
    Q4WordBatchMessage,
    Q5ArticleWordCountBatchMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter
from q4_q5_word_projector.controller import Q4Q5WordProjectorController
from q4_q5_word_projector.messaging import Q4Q5WordProjectorMessaging
from q4_q5_word_projector.projector import project_batch


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

    def test_controller_partitions_q4_words_and_broadcasts_end(self):
        router = PartitionRouter("q4_word_counter.input", 3)
        message = TokenizedArticleBatchMessage(
            articles=[
                {
                    "article_id": "article-a",
                    "url": "https://example.com/a",
                    "modification_date": "2020-01-01T00:00:00",
                    "has_abstract": True,
                    "words": ["network", "data"],
                },
                {
                    "article_id": "article-b",
                    "url": "https://example.com/b",
                    "modification_date": "2021-01-01T00:00:00",
                    "has_abstract": True,
                    "words": ["network"],
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        messaging = Mock()
        ack = Mock()
        nack = Mock()
        controller = Q4Q5WordProjectorController(messaging, router, 1, 2)

        controller.process_message(message, ack, nack)

        routed_words: dict[str, list[str]] = {}
        for call_args in messaging.publish_q4_to.call_args_list:
            queue, batch = call_args.args
            routed_words.setdefault(queue, []).extend(batch.words)
        self.assertCountEqual(
            [word for words in routed_words.values() for word in words],
            ["network", "network", "data"],
        )
        for queue, words in routed_words.items():
            for word in words:
                self.assertEqual(
                    router.queue_for('["execution-123", "' + word + '"]'),
                    queue,
                )
        messaging.publish_q5.assert_called_once()
        ack.assert_called_once_with()
        nack.assert_not_called()

        messaging.reset_mock()
        ack.reset_mock()
        end_0 = PartitionEndMessage(
            "word_extractor", 0, "execution-123", "results.execution-123"
        )
        end_1 = PartitionEndMessage(
            "word_extractor", 1, "execution-123", "results.execution-123"
        )
        controller.process_message(end_0, ack, nack)
        messaging.broadcast_control.assert_called_once_with(end_0)
        messaging.reset_mock()
        controller.process_control_message(end_0)
        controller.process_control_message(end_1)

        expected_end = PartitionEndMessage(
            "q4_q5_word_projector",
            1,
            "execution-123",
            "results.execution-123",
        )
        self.assertEqual(
            [
                call(f"q4_word_counter.input.{partition}", expected_end)
                for partition in range(3)
            ],
            messaging.publish_q4_to.call_args_list,
        )
        messaging.publish_q5.assert_called_once_with(expected_end)
        nack.assert_not_called()

    def test_messaging_serializes_outputs_and_deserializes_input(self):
        input_queue = Mock()
        q4_output_queue = Mock()
        q5_output_queue = Mock()
        control_input = Mock()
        control_output = Mock()
        with patch(
            "q4_q5_word_projector.messaging.CoordinatedConsumer"
        ) as consumer_class:
            consumer = consumer_class.return_value
            messaging = Q4Q5WordProjectorMessaging(
                input_queue=input_queue,
                q4_output_queues={"q4.0": q4_output_queue},
                q5_output_queue=q5_output_queue,
                control_exchange_input=control_input,
                control_exchange_output=control_output,
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

        messaging.publish_q4_to("q4.0", q4_message)
        messaging.publish_q5(q5_message)

        q4_output_queue.send.assert_called_once_with(q4_message.serialize())
        q5_output_queue.send.assert_called_once_with(q5_message.serialize())

        callback = Mock()
        control_callback = Mock()
        messaging.start_consuming(callback, control_callback)
        consumer.start_consuming.assert_called_once_with(callback, control_callback)

        messaging.stop_consuming()
        messaging.close()

        consumer.stop_consuming.assert_called_once()
        consumer.close.assert_called_once()
        q4_output_queue.close.assert_called_once()
        q5_output_queue.close.assert_called_once()
        control_output.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
