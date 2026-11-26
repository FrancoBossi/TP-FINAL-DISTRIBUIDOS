import unittest
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from client.client import Client
from common.message_protocol.message import AckMessage, QueryEndMessage, ResultMessage
from gateway.handler import forward_results


class TestResults(unittest.TestCase):
    def test_gateway_rejects_wrong_execution_with_nack(self):
        execution_id = "execution-123"

        messaging = Mock()
        messaging.result_queue_name = f"results.{execution_id}"

        ack = Mock()
        nack = Mock()

        def consume(on_result, on_idle=None):
            on_result(
                QueryEndMessage(
                    query_id=1,
                    execution_id="other-execution",
                ),
                ack,
                nack,
            )

        messaging.start_consuming_results.side_effect = consume

        with self.assertRaisesRegex(ValueError, "different execution"):
            forward_results(Mock(), messaging, execution_id)

        ack.assert_not_called()
        nack.assert_called_once()
        messaging.stop_consuming_results.assert_called_once()

    def test_gateway_acks_discarded_result_after_client_disconnects(self):
        execution_id = "execution-123"

        messaging = Mock()
        messaging.result_queue_name = f"results.{execution_id}"

        ack = Mock()
        nack = Mock()

        def consume(on_result, on_idle=None):
            on_result(
                ResultMessage(
                    query_id=3,
                    record={"word": "test"},
                    execution_id=execution_id,
                ),
                ack,
                nack,
            )

        messaging.start_consuming_results.side_effect = consume

        with (
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.recv_msg", side_effect=EOFError),
        ):
            self.assertFalse(forward_results(Mock(), messaging, execution_id))

        ack.assert_called_once()
        nack.assert_not_called()
        messaging.stop_consuming_results.assert_not_called()

    def test_gateway_requires_all_five_completions(self):
        execution_id = "execution-123"

        messaging = Mock()
        messaging.result_queue_name = f"results.{execution_id}"

        ack = Mock()
        nack = Mock()

        def consume(on_result, on_idle=None):
            for query_id in range(1, 5):
                on_result(
                    QueryEndMessage(
                        query_id=query_id,
                        execution_id=execution_id,
                    ),
                    ack,
                    nack,
                )

        messaging.start_consuming_results.side_effect = consume

        with (
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.recv_msg", return_value=AckMessage()),
        ):
            with self.assertRaisesRegex(EOFError, "all queries"):
                forward_results(Mock(), messaging, execution_id)

        self.assertEqual(ack.call_count, 4)
        nack.assert_not_called()
        messaging.stop_consuming_results.assert_not_called()

    def test_client_rejects_invalid_or_late_results(self):
        execution_id = "execution-123"

        for messages in (
            [
                ResultMessage(
                    query_id=6,
                    record={},
                    execution_id=execution_id,
                )
            ],
            [
                ResultMessage(
                    query_id=1,
                    record=[],
                    execution_id=execution_id,
                )
            ],
            [
                QueryEndMessage(
                    query_id=1,
                    execution_id=execution_id,
                    result_id="query-1-end",
                ),
                ResultMessage(
                    query_id=1,
                    record={},
                    execution_id=execution_id,
                    result_id="query-1-late-result",
                ),
            ],
        ):
            with self.subTest(messages=messages), TemporaryDirectory() as output_dir:
                with patch("client.client.recv_msg", side_effect=messages):
                    with self.assertRaises(ValueError):
                        Client("unused", 0).receive_results(Mock(), output_dir)

    def test_client_acks_redelivery_without_writing_result_twice(self):
        execution_id = "execution-123"
        result = ResultMessage(
            query_id=1,
            record={"title": "A"},
            execution_id=execution_id,
            result_id="query-1-result-a",
        )
        messages = [result, result]
        messages.extend(
            QueryEndMessage(
                query_id=query_id,
                execution_id=execution_id,
                result_id=f"query-{query_id}-end",
            )
            for query_id in range(1, 6)
        )

        with TemporaryDirectory() as output_dir:
            sock = Mock()
            with (
                patch("client.client.recv_msg", side_effect=messages),
                patch("client.client.send_msg") as send,
            ):
                Client("unused", 0).receive_results(sock, output_dir)

            with open(f"{output_dir}/query1.jsonl", encoding="utf-8") as output:
                self.assertEqual(output.readlines(), ['{"title": "A"}\n'])
            self.assertEqual(send.call_count, 7)


if __name__ == "__main__":
    unittest.main()
