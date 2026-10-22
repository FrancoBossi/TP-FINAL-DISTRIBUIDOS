import unittest
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from client.client import Client
from common.message_protocol.message import AckMessage, QueryEndMessage, ResultMessage
from gateway.handler import forward_results


class TestResults(unittest.TestCase):
    def test_gateway_rejects_wrong_run_without_acknowledging(self):
        broker = Mock()
        broker.results.return_value = iter([(1, QueryEndMessage(1, "other"))])
        with self.assertRaisesRegex(ValueError, "different run"):
            forward_results(Mock(), broker, "run")
        broker.acknowledge_result.assert_not_called()

    def test_gateway_does_not_ack_broker_until_client_acks(self):
        broker = Mock()
        broker.results.return_value = iter(
            [(1, ResultMessage(3, {"word": "test"}, "run"))]
        )
        with (
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.recv_msg", side_effect=EOFError),
        ):
            with self.assertRaises(EOFError):
                forward_results(Mock(), broker, "run")
        broker.acknowledge_result.assert_not_called()

    def test_gateway_requires_all_five_completions(self):
        broker = Mock()
        broker.results.return_value = iter(
            [(i, QueryEndMessage(i, "run")) for i in range(1, 5)]
        )
        with (
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.recv_msg", return_value=AckMessage()),
        ):
            with self.assertRaisesRegex(EOFError, "all queries"):
                forward_results(Mock(), broker, "run")

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
