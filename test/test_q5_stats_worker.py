import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    PartitionEndMessage,
    Q5HistoricalWordCountBatchMessage,
    Q5PartialStatsMessage,
    Q5StatsPartitionEndMessage,
)
from common.partition_router import PartitionRouter
from q5.stats_worker.controller import Q5StatsWorkerController
from q5.stats_worker.messaging import Q5StatsWorkerMessaging


class TestQ5StatsWorkerController(unittest.TestCase):
    def test_messaging_routes_one_execution_to_one_average_reducer(self):
        input_queue = Mock()
        output_queues = {
            f"q5_average_reducer.input.{partition}": Mock() for partition in range(3)
        }
        router = PartitionRouter("q5_average_reducer.input", 3)
        messaging = Q5StatsWorkerMessaging(
            input_queue,
            output_queues["q5_average_reducer.input.0"],
            output_queues,
            router,
        )
        message = Q5PartialStatsMessage(
            10,
            2,
            "execution-123",
            "results.execution-123",
        )

        messaging.publish(message)

        selected_queue = router.queue_for("execution-123")
        output_queues[selected_queue].send.assert_called_once_with(message.serialize())
        for queue_name, queue in output_queues.items():
            if queue_name != selected_queue:
                queue.send.assert_not_called()

    def test_accumulates_batches_and_publishes_partial_stats_before_end(self):
        messaging = Mock()
        controller = Q5StatsWorkerController(
            messaging=messaging,
            partition_id=0,
        )
        ack = Mock()
        nack = Mock()

        first_batch = Q5HistoricalWordCountBatchMessage(
            word_counts=[0, 2, 4],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        second_batch = Q5HistoricalWordCountBatchMessage(
            word_counts=[3, 5],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        end = PartitionEndMessage(
            stage="q5_date_router",
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        controller.process_message(first_batch, ack, nack)
        controller.process_message(second_batch, ack, nack)
        controller.process_message(end, ack, nack)

        self.assertEqual(
            messaging.publish.call_args_list,
            [
                call(
                    Q5PartialStatsMessage(
                        total_word_count=14,
                        abstract_count=5,
                        execution_id="execution-123",
                        reply_queue="results.execution-123",
                    )
                ),
                call(
                    Q5StatsPartitionEndMessage(
                        partition_id=0,
                        execution_id="execution-123",
                        reply_queue="results.execution-123",
                    )
                ),
            ],
        )
        self.assertEqual(ack.call_count, 3)
        nack.assert_not_called()
        self.assertNotIn("execution-123", controller._executions)

    def test_keeps_state_and_nacks_when_publication_fails(self):
        messaging = Mock()
        controller = Q5StatsWorkerController(
            messaging=messaging,
            partition_id=0,
        )
        ack = Mock()
        nack = Mock()

        batch = Q5HistoricalWordCountBatchMessage(
            word_counts=[2, 4],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        end = PartitionEndMessage(
            stage="q5_date_router",
            partition_id=0,
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        controller.process_message(batch, ack, nack)
        ack.reset_mock()
        messaging.publish.side_effect = [None, RuntimeError("publish failed")]

        with self.assertRaises(RuntimeError):
            controller.process_message(end, ack, nack)

        ack.assert_not_called()
        nack.assert_called_once_with()
        self.assertEqual(
            controller._executions["execution-123"].total_word_count,
            6,
        )
        self.assertEqual(
            controller._executions["execution-123"].abstract_count,
            2,
        )


if __name__ == "__main__":
    unittest.main()
