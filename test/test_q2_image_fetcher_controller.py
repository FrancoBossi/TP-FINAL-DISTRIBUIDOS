import unittest
from unittest.mock import Mock, call, patch

from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    PartitionEndMessage,
    Q1CandidateBatchMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from q2.image_fetcher.controller import ImageFetcherController


class TestImageFetcherController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = ImageFetcherController(
            messaging=self.messaging,
            partition_id=0,
            biography_filter_partition_count=3,
        )

        self.batch = ImageCandidateBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "image_url": "https://example.com/image.jpg",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.partition_ends = [
            PartitionEndMessage(
                stage="q2_biography_filter",
                partition_id=partition_id,
                execution_id="execution-123",
                reply_queue="results.execution-123",
            )
            for partition_id in range(3)
        ]

        self.result = ResultMessage(
            query_id=2,
            record={
                "name": "Example article",
                "url": "https://example.com/article",
                "image": "AQID",
            },
            execution_id="execution-123",
        )

    def test_downloads_and_publishes_query2_results(self):
        with patch(
            "q2.image_fetcher.controller.build_query2_results",
            return_value=[self.result],
        ) as build_results:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        build_results.assert_called_once_with(self.batch)
        self.assertEqual(
            self.messaging.method_calls,
            [call.publish(self.result)],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_nothing_when_no_image_was_downloaded(self):
        with patch(
            "q2.image_fetcher.controller.build_query2_results",
            return_value=[],
        ):
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_broadcasts_biography_filter_partition_end(self):
        message = self.partition_ends[0]

        self.controller.process_message(message, self.ack, self.nack)

        self.messaging.broadcast_control.assert_called_once_with(message)
        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_waits_for_all_biography_filter_partitions_on_control(self):
        for message in self.partition_ends[:2]:
            self.controller.process_control_message(message)

        self.messaging.publish.assert_not_called()

    def test_publishes_query_partition_end_after_control_barrier(self):
        for message in self.partition_ends:
            self.controller.process_control_message(message)

        self.messaging.publish.assert_called_once_with(
            QueryPartitionEndMessage(
                query_id=2,
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

    def test_keeps_execution_barriers_separate(self):
        other_execution_ends = [
            PartitionEndMessage(
                stage="q2_biography_filter",
                partition_id=partition_id,
                execution_id="execution-456",
                reply_queue="results.execution-456",
            )
            for partition_id in range(3)
        ]

        for message in self.partition_ends[:2]:
            self.controller.process_control_message(message)
        for message in other_execution_ends:
            self.controller.process_control_message(message)

        self.messaging.publish.assert_called_once_with(
            QueryPartitionEndMessage(
                query_id=2,
                partition_id=0,
                execution_id="execution-456",
            )
        )

    def test_nacks_when_publication_fails(self):
        error = RuntimeError("ResultSink unavailable")
        self.messaging.publish.side_effect = error

        with (
            patch(
                "q2.image_fetcher.controller.build_query2_results",
                return_value=[self.result],
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
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
            "IMAGE_CANDIDATE_BATCH or PARTITION_END",
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
