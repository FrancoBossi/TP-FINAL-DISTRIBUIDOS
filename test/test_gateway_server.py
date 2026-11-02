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

        def handle(client, factory, **kwargs):
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
                serve("localhost", 5000, Mock(), threading.Event())
            self.assertTrue(next_client.wait(2))
            self.assertTrue(finished.wait(2))


class TestClientSession(unittest.TestCase):
    def test_stop_during_messaging_creation_stops_late_consumer(self):
        from gateway.server import ClientSession

        client = Mock()
        client.__enter__ = Mock(return_value=client)
        client.__exit__ = Mock(return_value=False)
        creating = threading.Event()
        release = threading.Event()
        messaging = Mock()
        errors = []

        def factory(_execution_id):
            creating.set()
            if not release.wait(2):
                errors.append("Messaging creation was not released")
            return messaging

        def handle(_client, create, **_kwargs):
            create("execution")

        session = ClientSession(client, "client", factory, threading.Event())
        with patch("gateway.server.handle_client", side_effect=handle):
            session.start()
            try:
                self.assertTrue(creating.wait(2))
                session.stop()
                session.stop()
            finally:
                release.set()
                session.join()
        self.assertEqual(errors, [])
        messaging.stop_consuming_results.assert_called_once_with()
        client.shutdown.assert_called_once()
        self.assertFalse(session.is_alive())

    def test_failed_stop_during_creation_closes_unreturned_messaging(self):
        from gateway.server import ClientSession

        messaging = Mock()
        messaging.stop_consuming_results.side_effect = RuntimeError("stop failed")
        stopped = threading.Event()
        stopped.set()
        session = ClientSession(Mock(), "client", lambda _: messaging, stopped)
        with self.assertRaisesRegex(RuntimeError, "stop failed"):
            session._create_messaging("execution")
        messaging.close.assert_called_once_with()
