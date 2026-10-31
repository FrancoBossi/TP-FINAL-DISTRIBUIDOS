import unittest
from unittest.mock import Mock, call, patch

from client.client import Client


class TestClientConnection(unittest.TestCase):
    @patch("client.client.time.sleep")
    @patch("client.client.socket.create_connection")
    def test_connects_without_sleeping_when_gateway_is_ready(self, connect, sleep):
        sock = Client("gateway", 5000)._connect()
        self.assertIs(sock, connect.return_value)
        connect.assert_called_once_with(("gateway", 5000), timeout=5)
        sock.settimeout.assert_called_once_with(None)
        sleep.assert_not_called()

    @patch("client.client.time.sleep")
    @patch("client.client.socket.create_connection")
    def test_recovers_from_temporary_connection_failures(self, connect, sleep):
        sock = Mock()
        connect.side_effect = [
            ConnectionRefusedError("starting"),
            TimeoutError("unreachable"),
            sock,
        ]
        self.assertIs(Client("gateway", 5000)._connect(), sock)
        self.assertEqual(connect.call_count, 3)
        self.assertEqual(sleep.call_args_list, [call(3), call(3)])
        sock.settimeout.assert_called_once_with(None)

    @patch("client.client.time.sleep")
    @patch("client.client.socket.create_connection")
    def test_exhaustion_preserves_last_error_without_an_extra_sleep(
        self, connect, sleep
    ):
        error = ConnectionRefusedError("unavailable")
        connect.side_effect = error
        with self.assertRaises(ConnectionRefusedError) as raised:
            Client("gateway", 5000)._connect()
        self.assertIs(raised.exception, error)
        self.assertEqual(connect.call_count, 10)
        self.assertEqual(sleep.call_args_list, [call(3)] * 9)

    @patch("client.client.iter_article_records", return_value=iter([]))
    @patch("client.client.Client.receive_results")
    @patch("client.client.Client.send_articles", side_effect=ConnectionResetError)
    @patch("client.client.Client._connect")
    def test_does_not_retry_an_execution_after_connection_is_established(
        self, connect, send, receive, articles
    ):
        with self.assertRaises(ConnectionResetError):
            Client("gateway", 5000).run("input.parquet")
        connect.assert_called_once()
        send.assert_called_once()
        receive.assert_not_called()
        connect.return_value.__exit__.assert_called_once()
