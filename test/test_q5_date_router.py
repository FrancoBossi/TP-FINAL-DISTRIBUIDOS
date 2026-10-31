import copy
import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    Q5ArticleWordCountBatchMessage,
    Q5CandidateBatchMessage,
    Q5HistoricalWordCountBatchMessage,
    RoutedInputEndMessage,
)
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

        events.reset_mock()
        end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        controller.process_message(end, ack, nack)

        self.assertEqual(
            [
                call.publish_historical(end),
                call.publish_candidates(end),
                call.ack(),
            ],
            events.mock_calls,
        )
        nack.assert_not_called()

    def test_messaging_serializes_outputs_and_deserializes_input(self):
        input_queue = Mock()
        historical_output_queue = Mock()
        candidate_output_queue = Mock()
        messaging = Q5DateRouterMessaging(
            input_queue=input_queue,
            historical_output_queue=historical_output_queue,
            candidate_output_queue=candidate_output_queue,
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
        ack = Mock()
        nack = Mock()
        messaging.start_consuming(callback)

        middleware_callback = input_queue.start_consuming.call_args.args[0]
        input_message = Q5ArticleWordCountBatchMessage(
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
        historical_output_queue.close.assert_called_once()
        candidate_output_queue.close.assert_called_once()

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
