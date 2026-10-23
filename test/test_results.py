import unittest
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from client.client import Client
from common.message_protocol.message import AckMessage, QueryEndMessage, ResultMessage
from gateway.handler import forward_results


class TestResults(unittest.TestCase):
    def test_gateway_rejects_wrong_run_with_nack(self):
        messaging = Mock()
        messaging.result_queue_name = "results.run"

        ack = Mock()
        nack = Mock()

        def consume(on_result):
            on_result(
                QueryEndMessage(1, "other"),
                ack,
                nack,
            )

        messaging.start_consuming_results.side_effect = consume

        with self.assertRaisesRegex(ValueError, "different run"):
            forward_results(Mock(), messaging, "run")

        ack.assert_not_called()
        nack.assert_called_once()
        messaging.stop_consuming_results.assert_called_once()

    def test_gateway_does_not_ack_middleware_until_client_acks(self):
        messaging = Mock()
        messaging.result_queue_name = "results.run"

        ack = Mock()
        nack = Mock()

        def consume(on_result):
            on_result(
                ResultMessage(3, {"word": "test"}, "run"),
                ack,
                nack,
            )

        messaging.start_consuming_results.side_effect = consume

        with (
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.recv_msg", side_effect=EOFError),
        ):
            with self.assertRaises(EOFError):
                forward_results(Mock(), messaging, "run")

        ack.assert_not_called()
        nack.assert_called_once()
        messaging.stop_consuming_results.assert_called_once()

    def test_gateway_requires_all_five_completions(self):
        messaging = Mock()
        messaging.result_queue_name = "results.run"

        ack = Mock()
        nack = Mock()

        def consume(on_result):
            for query_id in range(1, 5):
                on_result(
                    QueryEndMessage(query_id, "run"),
                    ack,
                    nack,
                )

        messaging.start_consuming_results.side_effect = consume

        with (
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.recv_msg", return_value=AckMessage()),
        ):
            with self.assertRaisesRegex(EOFError, "all queries"):
                forward_results(Mock(), messaging, "run")

        self.assertEqual(ack.call_count, 4)
        nack.assert_not_called()
        messaging.stop_consuming_results.assert_not_called()

    def test_client_rejects_invalid_or_late_results(self):
        for messages in (
            [ResultMessage(6, {}, "run")],
            [ResultMessage(1, [], "run")],
            [QueryEndMessage(1, "run"), ResultMessage(1, {}, "run")],
        ):
            with self.subTest(messages=messages), TemporaryDirectory() as output_dir:
                with patch("client.client.recv_msg", side_effect=messages):
                    with self.assertRaises(ValueError):
                        Client("unused", 0).receive_results(Mock(), output_dir)


if __name__ == "__main__":
    unittest.main()
