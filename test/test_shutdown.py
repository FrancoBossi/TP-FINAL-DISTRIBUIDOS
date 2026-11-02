import signal
import unittest
from unittest.mock import Mock

from common.coordinated_consumer import CoordinatedConsumer
from common.message_protocol.message import RoutedArticleMessage
from common.shutdown import shutdown_signals
from input_batcher.batcher import BatchAccumulator
from input_batcher.controller import InputBatcherController


class TestShutdown(unittest.TestCase):
    def test_signals_request_stop_once_and_restore_handlers(self):
        previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        stop = Mock()
        with shutdown_signals(stop):
            signal.raise_signal(signal.SIGTERM)
            signal.raise_signal(signal.SIGINT)
        stop.assert_called_once_with()
        for signum, handler in previous.items():
            self.assertEqual(signal.getsignal(signum), handler)

    def test_stop_before_coordinated_start_does_not_start_threads(self):
        data, control = Mock(), Mock()
        consumer = CoordinatedConsumer(data, control)
        consumer.stop_consuming()
        consumer.start_consuming(Mock(), Mock())
        data.start_consuming.assert_not_called()
        control.start_consuming.assert_not_called()

    def test_batcher_drains_all_partial_batches_without_end_messages(self):
        batcher = BatchAccumulator(4096)
        messaging = Mock()
        controller = InputBatcherController(batcher, messaging)

        def consume(callback):
            for execution in ("a", "b"):
                callback(
                    RoutedArticleMessage(
                        article={"name": execution},
                        execution_id=execution,
                        reply_queue=f"results.{execution}",
                    ),
                    Mock(),
                    Mock(),
                )
            controller.stop()

        messaging.start_consuming.side_effect = consume
        controller.start()
        self.assertEqual(messaging.publish.call_count, 2)
        self.assertEqual(batcher.current_batch, {})
        for publication in messaging.publish.call_args_list:
            self.assertEqual(len(publication.args[0].articles), 1)


if __name__ == "__main__":
    unittest.main()


class TestGatewayShutdown(unittest.TestCase):
    def test_shutdown_wakes_upload_and_joins_session(self):
        import socket
        from threading import Event
        from unittest.mock import patch

        from gateway.server import serve

        stopped = Event()
        server = Mock()
        client = Mock()
        client.__enter__ = Mock(return_value=client)
        client.__exit__ = Mock(return_value=False)
        session_started = Event()
        interrupted = Event()
        finished = Event()

        def accept():
            if not session_started.is_set():
                return client, "client"
            stopped.set()
            raise socket.timeout()

        def handle(_client, _factory, **_kwargs):
            session_started.set()
            self.assertTrue(interrupted.wait(2))
            finished.set()

        server.accept.side_effect = accept
        client.shutdown.side_effect = lambda _how: interrupted.set()
        with (
            patch("gateway.server.socket.socket") as factory,
            patch("gateway.server.handle_client", side_effect=handle),
        ):
            factory.return_value.__enter__.return_value = server
            serve("localhost", 5000, Mock(), stopped)
        self.assertTrue(finished.is_set())
        client.shutdown.assert_called_with(socket.SHUT_RDWR)
