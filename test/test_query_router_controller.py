import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    ArticleBatchMessage,
    NormalizedArticleBatchMessage,
    PartitionEndMessage,
)
from query_router.controller import QueryRouterController
from query_router.router import route_batch


class TestQueryRouterController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = QueryRouterController(
            messaging=self.messaging,
            partition_id=1,
            preprocessor_partition_count=2,
        )

        self.batch = NormalizedArticleBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "abstract": "Example abstract.",
                    "sections": ["Biography"],
                    "image_url": None,
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.partition_end_0 = PartitionEndMessage(
            stage="preprocessor",
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.partition_end_1 = PartitionEndMessage(
            stage="preprocessor",
            partition_id=1,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_projects_and_publishes_batch_to_both_branches(self):
        expected_q1_q2, expected_text = route_batch(self.batch)

        self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1_q2(expected_q1_q2),
                call.publish_text(expected_text),
            ],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_broadcasts_preprocessor_partition_end(self):
        self.controller.process_message(
            self.partition_end_0,
            self.ack,
            self.nack,
        )

        self.messaging.broadcast_control.assert_called_once_with(self.partition_end_0)
        self.messaging.publish_q1_q2.assert_not_called()
        self.messaging.publish_text.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_waits_for_all_preprocessor_partitions_on_control(self):
        self.controller.process_control_message(self.partition_end_0)

        self.messaging.publish_q1_q2.assert_not_called()
        self.messaging.publish_text.assert_not_called()

    def test_emits_own_partition_end_after_control_barrier(self):
        self.controller.process_control_message(self.partition_end_0)
        self.controller.process_control_message(self.partition_end_1)

        expected = PartitionEndMessage(
            stage="query_router",
            partition_id=1,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1_q2(expected),
                call.publish_text(expected),
            ],
        )

    def test_does_not_count_same_control_partition_twice(self):
        self.controller.process_control_message(self.partition_end_0)
        self.controller.process_control_message(self.partition_end_0)

        self.messaging.publish_q1_q2.assert_not_called()
        self.messaging.publish_text.assert_not_called()

    def test_tracks_interleaved_control_executions_independently(self):
        other_partition_end_0 = PartitionEndMessage(
            stage="preprocessor",
            partition_id=0,
            execution_id="execution-456",
            reply_queue="results.execution-456",
        )
        other_partition_end_1 = PartitionEndMessage(
            stage="preprocessor",
            partition_id=1,
            execution_id="execution-456",
            reply_queue="results.execution-456",
        )

        for message in (
            self.partition_end_0,
            other_partition_end_0,
            self.partition_end_1,
            other_partition_end_1,
        ):
            self.controller.process_control_message(message)

        first_end = PartitionEndMessage(
            stage="query_router",
            partition_id=1,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        second_end = PartitionEndMessage(
            stage="query_router",
            partition_id=1,
            execution_id="execution-456",
            reply_queue="results.execution-456",
        )
        self.assertEqual(
            self.messaging.method_calls,
            [
                call.publish_q1_q2(first_end),
                call.publish_text(first_end),
                call.publish_q1_q2(second_end),
                call.publish_text(second_end),
            ],
        )

    def test_nacks_when_second_batch_publication_fails(self):
        error = RuntimeError("text output unavailable")
        self.messaging.publish_text.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(self.batch, self.ack, self.nack)

        self.assertIs(raised.exception, error)
        self.messaging.publish_q1_q2.assert_called_once()
        self.messaging.publish_text.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_nacks_when_control_broadcast_fails(self):
        error = RuntimeError("control unavailable")
        self.messaging.broadcast_control.side_effect = error

        with self.assertRaises(RuntimeError) as raised:
            self.controller.process_message(
                self.partition_end_0,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_data_message(self):
        message = ArticleBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "NORMALIZED_ARTICLE_BATCH or PARTITION_END",
        ):
            self.controller.process_message(message, self.ack, self.nack)

        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_partition_end_from_wrong_control_stage(self):
        message = PartitionEndMessage(
            stage="query_router",
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "expected preprocessor partition end",
        ):
            self.controller.process_control_message(message)


if __name__ == "__main__":
    unittest.main()
