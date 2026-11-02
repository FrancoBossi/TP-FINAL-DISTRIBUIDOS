import copy
import json
import unittest
from unittest.mock import Mock, call, patch

from common.message_protocol.message import (
    PartitionEndMessage,
    Q5ArticleWordCountBatchMessage,
    Q5CandidateBatchMessage,
    Q5HistoricalWordCountBatchMessage,
)
from common.partition_router import PartitionRouter
from q5.date_router.controller import Q5DateRouterController
from q5.date_router.messaging import Q5DateRouterMessaging
from q5.date_router.router import route_batch


class TestQ5DateRouter(unittest.TestCase):
    def test_routes_historical_counts_and_candidates(self):
        message = Q5ArticleWordCountBatchMessage(
            articles=[
                {
                    "article_id": "empty-article",
                    "url": "https://example.com/empty",
                    "modification_date": "2019-01-01T00:00:00",
                    "word_count": 0,
                },
                {
                    "article_id": "article-2020",
                    "url": "https://example.com/2020",
                    "modification_date": "2020-12-31T23:59:59",
                    "word_count": 2,
                },
                {
                    "article_id": "article-2021",
                    "url": "https://example.com/2021",
                    "modification_date": "2021-01-01T00:00:00",
                    "word_count": 3,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        original = copy.deepcopy(message)

        self.assertEqual(
            (
                Q5HistoricalWordCountBatchMessage(
                    word_counts=[0, 2],
                    execution_id="execution-123",
                    reply_queue="results.execution-123",
                ),
                Q5CandidateBatchMessage(
                    articles=[
                        {
                            "article_id": "article-2021",
                            "url": "https://example.com/2021",
                            "word_count": 3,
                        }
                    ],
                    execution_id="execution-123",
                    reply_queue="results.execution-123",
                ),
            ),
            route_batch(message),
        )
        self.assertEqual(original, message)

    def test_controller_publishes_both_branches_before_ack_and_forwards_end(self):
        message = Q5ArticleWordCountBatchMessage(
            articles=[
                {
                    "article_id": "historical",
                    "url": "https://example.com/historical",
                    "modification_date": "2020-12-31T23:59:59",
                    "word_count": 2,
                },
                {
                    "article_id": "candidate",
                    "url": "https://example.com/candidate",
                    "modification_date": "2021-01-01T00:00:00",
                    "word_count": 3,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        historical_batch, candidate_batch = route_batch(message)

        messaging = Mock()
        ack = Mock()
        nack = Mock()
        events = Mock()
        events.attach_mock(
            messaging.publish_historical,
            "publish_historical",
        )
        events.attach_mock(
            messaging.publish_candidates,
            "publish_candidates",
        )
        events.attach_mock(ack, "ack")

        controller = Q5DateRouterController(messaging)
        controller.process_message(message, ack, nack)

        self.assertEqual(
            [
                call.publish_historical(historical_batch),
                call.publish_candidates(candidate_batch),
                call.ack(),
            ],
            events.mock_calls,
        )

        end = PartitionEndMessage(
            "q4_q5_word_projector", 0, "execution-123", "results.execution-123"
        )

        controller.process_message(end, ack, nack)

        messaging.broadcast_control.assert_called_once_with(end)
        nack.assert_not_called()

    def test_controller_routes_execution_to_one_worker_and_broadcasts_end(self):
        router = PartitionRouter("q5_stats_worker.input", 3)
        message = Q5ArticleWordCountBatchMessage(
            articles=[
                {
                    "article_id": "historical",
                    "url": "https://example.com/historical",
                    "modification_date": "2020-12-31T23:59:59",
                    "word_count": 2,
                },
                {
                    "article_id": "candidate",
                    "url": "https://example.com/candidate",
                    "modification_date": "2021-01-01T00:00:00",
                    "word_count": 3,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        messaging = Mock()
        ack = Mock()
        nack = Mock()
        controller = Q5DateRouterController(messaging, router, 1, 2)

        controller.process_message(message, ack, nack)

        historical_batch, candidate_batch = route_batch(message)
        messaging.publish_historical_to.assert_called_once_with(
            router.queue_for("execution-123"), historical_batch
        )
        messaging.publish_historical.assert_not_called()
        messaging.publish_candidates.assert_called_once_with(candidate_batch)
        ack.assert_called_once_with()
        nack.assert_not_called()

        messaging.reset_mock()
        ack.reset_mock()
        end_0 = PartitionEndMessage(
            "q4_q5_word_projector", 0, "execution-123", "results.execution-123"
        )
        end_1 = PartitionEndMessage(
            "q4_q5_word_projector", 1, "execution-123", "results.execution-123"
        )
        controller.process_control_message(end_0)
        controller.process_control_message(end_1)

        expected_end = PartitionEndMessage(
            "q5_date_router", 1, "execution-123", "results.execution-123"
        )
        self.assertEqual(
            [
                call(f"q5_stats_worker.input.{partition}", expected_end)
                for partition in range(3)
            ],
            messaging.publish_historical_to.call_args_list,
        )
        messaging.publish_candidates.assert_called_once_with(expected_end)
        nack.assert_not_called()

    def test_controller_routes_each_candidate_by_execution_and_article_id(self):
        candidate_router = PartitionRouter("q5_comparator.input", 3)
        messaging = Mock()
        controller = Q5DateRouterController(
            messaging,
            candidate_router=candidate_router,
        )
        message = Q5ArticleWordCountBatchMessage(
            articles=[
                {
                    "article_id": "article-2021",
                    "url": "https://example.com/2021",
                    "modification_date": "2021-01-01T00:00:00",
                    "word_count": 3,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        controller.process_message(message, Mock(), Mock())

        key = json.dumps(
            ["execution-123", "article-2021"],
            ensure_ascii=False,
        )
        messaging.publish_candidates_to.assert_called_once_with(
            candidate_router.queue_for(key),
            Q5CandidateBatchMessage(
                [
                    {
                        "article_id": "article-2021",
                        "url": "https://example.com/2021",
                        "word_count": 3,
                    }
                ],
                "execution-123",
                "results.execution-123",
            ),
        )
        messaging.publish_candidates.assert_not_called()

    def test_messaging_serializes_outputs_and_deserializes_input(self):
        input_queue = Mock()
        historical_output_queue = Mock()
        candidate_output_queue = Mock()
        control_input = Mock()
        control_output = Mock()
        with patch("q5.date_router.messaging.CoordinatedConsumer") as consumer_class:
            consumer = consumer_class.return_value
            messaging = Q5DateRouterMessaging(
                input_queue=input_queue,
                historical_output_queue=historical_output_queue,
                candidate_output_queue=candidate_output_queue,
                control_exchange_input=control_input,
                control_exchange_output=control_output,
            )

        historical_message = Q5HistoricalWordCountBatchMessage(
            word_counts=[0, 2],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        candidate_message = Q5CandidateBatchMessage(
            articles=[
                {
                    "article_id": "candidate",
                    "url": "https://example.com/candidate",
                    "word_count": 3,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        messaging.publish_historical(historical_message)
        messaging.publish_candidates(candidate_message)

        historical_output_queue.send.assert_called_once_with(
            historical_message.serialize()
        )
        candidate_output_queue.send.assert_called_once_with(
            candidate_message.serialize()
        )

        callback = Mock()
        control_callback = Mock()
        messaging.start_consuming(callback, control_callback)
        consumer.start_consuming.assert_called_once_with(callback, control_callback)

        messaging.stop_consuming()
        messaging.close()

        consumer.stop_consuming.assert_called_once()
        consumer.close.assert_called_once()
        historical_output_queue.close.assert_called_once()
        candidate_output_queue.close.assert_called_once()
        control_output.close.assert_called_once()

    def test_second_publication_failure_is_nacked(self):
        message = Q5ArticleWordCountBatchMessage(
            articles=[
                {
                    "article_id": "historical",
                    "url": "https://example.com/historical",
                    "modification_date": "2020-01-01T00:00:00",
                    "word_count": 2,
                },
                {
                    "article_id": "candidate",
                    "url": "https://example.com/candidate",
                    "modification_date": "2021-01-01T00:00:00",
                    "word_count": 3,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        messaging = Mock()
        messaging.publish_candidates.side_effect = RuntimeError(
            "middleware unavailable"
        )
        ack = Mock()
        nack = Mock()

        controller = Q5DateRouterController(messaging)

        with self.assertRaises(RuntimeError):
            controller.process_message(message, ack, nack)

        messaging.publish_historical.assert_called_once()
        ack.assert_not_called()
        nack.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
