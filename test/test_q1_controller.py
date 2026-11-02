import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    PartitionEndMessage,
    Q1CandidateBatchMessage,
    Q2CandidateBatchMessage,
    QueryPartitionEndMessage,
)
from q1.controller import Q1Controller
from q1.filter import build_query1_results


class TestQ1Controller(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = Q1Controller(
            messaging=self.messaging,
            partition_id=0,
            until_2020_partition_count=3,
        )

        self.batch = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/english",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                },
                {
                    "url": "https://example.com/french",
                    "language": "fr",
                    "modification_date": "2019-01-01T00:00:00",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.partition_ends = [
            PartitionEndMessage(
                stage="until_2020_filter",
                partition_id=partition_id,
                execution_id="execution-123",
                reply_queue="results.execution-123",
            )
            for partition_id in range(3)
        ]

    def test_publishes_query1_results(self):
        expected_results = build_query1_results(self.batch)

        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

        self.assertEqual(
            self.messaging.method_calls,
            [call.publish(result) for result in expected_results],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_nothing_when_batch_has_no_english_articles(self):
        batch = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/french",
                    "language": "fr",
                    "modification_date": "2020-01-01T00:00:00",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.controller.process_message(
            batch,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_broadcasts_until_2020_partition_end(self):
        message = self.partition_ends[0]

        self.controller.process_message(message, self.ack, self.nack)

        self.messaging.broadcast_control.assert_called_once_with(message)
        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_waits_for_all_until_2020_partitions_on_control(self):
        for message in self.partition_ends[:2]:
            self.controller.process_control_message(message)

        self.messaging.publish.assert_not_called()

    def test_publishes_query_partition_end_after_control_barrier(self):
        for message in self.partition_ends:
            self.controller.process_control_message(message)

        self.messaging.publish.assert_called_once_with(
            QueryPartitionEndMessage(
                query_id=1,
                partition_id=0,
                execution_id="execution-123",
            )
        )

    def test_does_not_count_same_control_partition_twice(self):
        for message in (
            self.partition_ends[0],
            self.partition_ends[0],
            self.partition_ends[1],
        ):
            self.controller.process_control_message(message)

        self.messaging.publish.assert_not_called()

    def test_nacks_when_publication_fails(self):
        error = RuntimeError("result sink unavailable")
        self.messaging.publish.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.messaging.publish.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_nacks_when_control_broadcast_fails(self):
        error = RuntimeError("control unavailable")
        self.messaging.broadcast_control.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.partition_ends[0],
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = Q2CandidateBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Q1_CANDIDATE_BATCH or PARTITION_END",
        ):
            self.controller.process_message(
                message,
                self.ack,
                self.nack,
            )

        self.messaging.publish.assert_not_called()
        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
