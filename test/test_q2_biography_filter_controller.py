import unittest
from unittest.mock import Mock

from common.message_protocol.message import (
    PartitionEndMessage,
    Q1CandidateBatchMessage,
    Q2CandidateBatchMessage,
)
from q2.biography_filter.controller import Q2BiographyFilterController
from q2.biography_filter.filter import filter_biography_batch


class TestQ2BiographyFilterController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = Q2BiographyFilterController(
            messaging=self.messaging,
            partition_id=0,
            until_2020_partition_count=3,
        )

        self.batch = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "Example biography",
                    "url": "https://example.com/article",
                    "sections": ["Biography"],
                    "image_url": "https://example.com/image.jpg",
                }
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

    def test_filters_and_publishes_non_empty_batch(self):
        expected = filter_biography_batch(self.batch)

        self.controller.process_message(
            self.batch,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(expected)
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_does_not_publish_empty_batch(self):
        batch = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "Article without biography",
                    "url": "https://example.com/article",
                    "sections": ["History"],
                    "image_url": "https://example.com/image.jpg",
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

    def test_publishes_partition_end_after_control_barrier(self):
        for message in self.partition_ends:
            self.controller.process_control_message(message)

        self.messaging.publish.assert_called_once_with(
            PartitionEndMessage(
                stage="q2_biography_filter",
                partition_id=0,
                execution_id="execution-123",
                reply_queue="results.execution-123",
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
        error = RuntimeError("ImageFetcher unavailable")
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
        message = Q1CandidateBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Q2_CANDIDATE_BATCH or PARTITION_END",
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
