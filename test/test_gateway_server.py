import threading
import unittest
from unittest.mock import Mock, patch

from gateway.server import serve


class TestGatewayServer(unittest.TestCase):
    def test_accepts_next_client_while_previous_session_drains(self):
        first = Mock()
        second = Mock()
        draining = threading.Event()
        next_client = threading.Event()
        finished = threading.Event()
        server = Mock()
        server.accept.side_effect = [
            (first, "first"),
            (second, "second"),
            StopIteration,
        ]

        def handle(client, factory):
            if client is first:
                draining.set()
                next_client.wait(2)
                finished.set()
            else:
                draining.wait(2)
                next_client.set()

        with (
            patch("gateway.server.socket.socket") as socket_factory,
            patch("gateway.server.handle_client", side_effect=handle),
        ):
            socket_factory.return_value.__enter__.return_value = server
            # The accepted sockets need context manager support.
            first.__enter__ = Mock(return_value=first)
            first.__exit__ = Mock(return_value=False)
            second.__enter__ = Mock(return_value=second)
            second.__exit__ = Mock(return_value=False)
            with self.assertRaises(StopIteration):
                serve("localhost", 5000, Mock())
            self.assertTrue(next_client.wait(2))
            self.assertTrue(finished.wait(2))
