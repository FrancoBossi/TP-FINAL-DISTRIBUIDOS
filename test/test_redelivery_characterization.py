import unittest
from unittest.mock import Mock

from common.message_identity import build_result_message_ids
from common.message_protocol.message import (
    NormalizedArticleBatchMessage,
    PartitionEndMessage,
    Q4PartialTopBatchMessage,
    Q4WordBatchMessage,
    ResultMessage,
)
from q4.word_counter.controller import Q4WordCounterController
from query_router.controller import QueryRouterController
from result_sink.controller import ResultSinkController


class RedeliveryCharacterizationTest(unittest.TestCase):
    """Document the duplicate windows that inbox/outbox must close."""

    def test_partial_fanout_redelivery_republishes_same_logical_output(self):
        messaging = Mock()
        controller = QueryRouterController(
            messaging=messaging,
            partition_id=0,
            preprocessor_partition_count=1,
        )
        message = NormalizedArticleBatchMessage(
            articles=[
                {
                    "name": "Network",
                    "url": "https://example.com/network",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "abstract": "A network.",
                    "sections": ["Introduction"],
                    "image_url": None,
                }
            ],
            execution_id="execution-redelivery",
            reply_queue="results.execution-redelivery",
            message_id="m1:normalized-input",
        )
        first_ack, first_nack = Mock(), Mock()
        messaging.publish_text.side_effect = RuntimeError("second branch unavailable")

        with self.assertRaisesRegex(RuntimeError, "second branch unavailable"):
            controller.process_message(message, first_ack, first_nack)

        first_ack.assert_not_called()
        first_nack.assert_called_once()
        first_q1_q2_output = messaging.publish_q1_q2.call_args.args[0]

        messaging.publish_text.side_effect = None
        second_ack, second_nack = Mock(), Mock()
        controller.process_message(message, second_ack, second_nack)

        second_q1_q2_output = messaging.publish_q1_q2.call_args.args[0]
        self.assertEqual(2, messaging.publish_q1_q2.call_count)
        self.assertEqual(
            first_q1_q2_output.message_id,
            second_q1_q2_output.message_id,
        )
        second_ack.assert_called_once()
        second_nack.assert_not_called()

    def test_stateful_redelivery_is_applied_twice_before_inbox_exists(self):
        messaging = Mock()
        controller = Q4WordCounterController(
            messaging=messaging,
            partition_id=0,
            projector_partition_count=1,
        )
        batch = Q4WordBatchMessage(
            words=["network"],
            execution_id="execution-redelivery",
            reply_queue="results.execution-redelivery",
            message_id="m1:q4-input",
        )

        for _ in range(2):
            controller.process_message(batch, Mock(), Mock())

        controller.process_message(
            PartitionEndMessage(
                stage="q4_q5_word_projector",
                partition_id=0,
                execution_id=batch.execution_id,
                reply_queue=batch.reply_queue,
                message_id="m1:q4-projector-end",
            ),
            Mock(),
            Mock(),
        )

        partial = next(
            call.args[0]
            for call in messaging.publish.call_args_list
            if isinstance(call.args[0], Q4PartialTopBatchMessage)
        )
        self.assertEqual([{"word": "network", "count": 2}], partial.words)
        self.assertTrue(partial.message_id.startswith("m1:"))

    def test_result_redelivery_is_forwarded_twice_with_same_result_id(self):
        messaging = Mock()
        controller = ResultSinkController(
            messaging=messaging,
            partition_counts={query_id: 1 for query_id in range(1, 6)},
        )
        result_id, message_id = build_result_message_ids(
            execution_id="execution-redelivery",
            query_id=1,
            result_kind="result",
            identity={"url": "https://example.com/network"},
            producer_id="q1",
        )
        result = ResultMessage(
            query_id=1,
            record={"url": "https://example.com/network"},
            execution_id="execution-redelivery",
            message_id=message_id,
            result_id=result_id,
        )

        for _ in range(2):
            controller.process_message(result, Mock(), Mock())

        self.assertEqual(2, messaging.publish.call_count)
        forwarded = [call.args[0] for call in messaging.publish.call_args_list]
        self.assertEqual([result_id, result_id], [item.result_id for item in forwarded])


if __name__ == "__main__":
    unittest.main()
