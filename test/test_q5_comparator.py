import tempfile
import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    Q5AverageMessage,
    Q5CandidateBatchMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedInputEndMessage,
)
from q5.comparator.buffer import CandidateBuffer
from q5.comparator.controller import Q5ComparatorController


class TestQ5ComparatorController(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.buffer = CandidateBuffer(self.temporary_directory.name)
        self.messaging = Mock()
        self.controller = Q5ComparatorController(
            messaging=self.messaging,
            candidate_buffer=self.buffer,
        )
        self.ack = Mock()
        self.nack = Mock()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_buffers_candidates_until_average_arrives(self):
        candidates = Q5CandidateBatchMessage(
            articles=[
                {
                    "article_id": "below",
                    "url": "https://example.com/below",
                    "word_count": 2,
                },
                {
                    "article_id": "equal",
                    "url": "https://example.com/equal",
                    "word_count": 3,
                },
                {
                    "article_id": "above",
                    "url": "https://example.com/above",
                    "word_count": 5,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        average = Q5AverageMessage(
            average_word_count=3.0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.controller.process_message(candidates, self.ack, self.nack)
        self.controller.process_message(end, self.ack, self.nack)

        self.messaging.publish.assert_not_called()
        self.assertEqual(
            list(self.buffer.iterate("execution-123")),
            candidates.articles,
        )

        self.controller.process_message(average, self.ack, self.nack)

        self.assertEqual(
            self.messaging.publish.call_args_list,
            [
                call(
                    ResultMessage(
                        query_id=5,
                        record={
                            "url": "https://example.com/above",
                            "word_count": 5,
                        },
                        execution_id="execution-123",
                    )
                ),
                call(
                    QueryEndMessage(
                        query_id=5,
                        execution_id="execution-123",
                    )
                ),
            ],
        )
        self.assertEqual(
            list(self.buffer.iterate("execution-123")),
            [],
        )
        self.assertNotIn(
            "execution-123",
            self.controller._executions,
        )
        self.assertEqual(self.ack.call_count, 3)
        self.nack.assert_not_called()

    def test_compares_immediately_when_average_arrives_first(self):
        average = Q5AverageMessage(
            average_word_count=3.0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        candidates = Q5CandidateBatchMessage(
            articles=[
                {
                    "article_id": "equal",
                    "url": "https://example.com/equal",
                    "word_count": 3,
                },
                {
                    "article_id": "above",
                    "url": "https://example.com/above",
                    "word_count": 4,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.controller.process_message(average, self.ack, self.nack)
        self.controller.process_message(candidates, self.ack, self.nack)
        self.controller.process_message(end, self.ack, self.nack)

        self.assertEqual(
            self.messaging.publish.call_args_list,
            [
                call(
                    ResultMessage(
                        query_id=5,
                        record={
                            "url": "https://example.com/above",
                            "word_count": 4,
                        },
                        execution_id="execution-123",
                    )
                ),
                call(
                    QueryEndMessage(
                        query_id=5,
                        execution_id="execution-123",
                    )
                ),
            ],
        )
        self.assertEqual(
            list(self.buffer.iterate("execution-123")),
            [],
        )
        self.assertEqual(self.ack.call_count, 3)
        self.nack.assert_not_called()

    def test_keeps_buffer_when_publishing_a_buffered_result_fails(self):
        candidates = Q5CandidateBatchMessage(
            articles=[
                {
                    "article_id": "above",
                    "url": "https://example.com/above",
                    "word_count": 5,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        average = Q5AverageMessage(
            average_word_count=3.0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.controller.process_message(candidates, self.ack, self.nack)
        self.ack.reset_mock()
        self.messaging.publish.side_effect = RuntimeError("publish failed")

        with self.assertRaises(RuntimeError):
            self.controller.process_message(average, self.ack, self.nack)

        self.ack.assert_not_called()
        self.nack.assert_called_once_with()
        self.assertEqual(
            list(self.buffer.iterate("execution-123")),
            candidates.articles,
        )


if __name__ == "__main__":
    unittest.main()
