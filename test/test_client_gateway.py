import json
import socket
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from client.client import Client
from common.message_identity import build_message_id
from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
    ExecutionCompletedMessage,
    ExecutionFailedMessage,
    InputEndMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from gateway.handler import handle_client


class FakeMessaging:
    def __init__(self):
        self.published = []
        self.terminal = []
        self.closed = False
        self.result_queue_name = "results.execution-123"
        self.acks = []
        self.nacks = []
        self.consumption_stopped = False

    def publish(self, message):
        self.published.append(message)

    def publish_terminal(self, message):
        self.terminal.append(message)

    def start_consuming_results(self, on_result, on_idle=None):
        execution_id = self.published[-1].execution_id
        messages = [
            ResultMessage(
                query_id=3,
                record={"word": "système"},
                execution_id=execution_id,
                result_id="query-3-result-0",
            ),
            *[
                QueryEndMessage(
                    query_id=query_id,
                    execution_id=execution_id,
                    result_id=f"query-{query_id}-end",
                )
                for query_id in (5, 1, 3, 2, 4)
            ],
        ]

        for message in messages:

            def ack():
                self.acks.append(message)

            def nack():
                self.nacks.append(message)

            on_result(message, ack, nack)

            if self.consumption_stopped:
                break

    def stop_consuming_results(self):
        self.consumption_stopped = True

    def close(self):
        self.closed = True


class TestClientGateway(unittest.TestCase):
    def test_upload_and_receive_all_five_queries_with_fake_messaging(self):
        article = {
            "abstract": None,
            "date_modified": "2020-01-01T00:00:00+00:00",
            "image": {
                "content_url": "https://upload.wikimedia.org/example.jpg",
                "height": 100,
                "width": 200,
            },
            "in_language": {"identifier": "fr"},
            "name": "École",
            "sections": '[{"name": "Histoire"}]',
            "url": "https://example.com",
        }

        for articles in ([article], []):
            with self.subTest(articles=articles), TemporaryDirectory() as output_dir:
                messaging = FakeMessaging()
                errors = []

                client_socket, gateway_socket = socket.socketpair()
                client_socket.settimeout(3)
                gateway_socket.settimeout(3)

                def gateway():
                    with gateway_socket:
                        try:
                            handle_client(
                                gateway_socket, lambda _: messaging, threading.Event()
                            )
                        except Exception as error:
                            errors.append(error)

                thread = threading.Thread(target=gateway, daemon=True)
                thread.start()

                try:
                    with (
                        client_socket,
                        patch(
                            "client.client.iter_article_records",
                            return_value=iter(articles),
                        ),
                        patch(
                            "client.client.socket.create_connection",
                            return_value=client_socket,
                        ),
                    ):
                        Client("gateway", 5000).run(
                            "fixture.parquet",
                            output_dir,
                        )
                finally:
                    client_socket.close()
                    thread.join(4)

                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertTrue(messaging.closed)
                self.assertTrue(messaging.consumption_stopped)
                self.assertEqual(len(messaging.acks), 6)
                self.assertEqual(messaging.nacks, [])

                for query_id in range(1, 6):
                    lines = (
                        (Path(output_dir) / f"query{query_id}.jsonl")
                        .read_text()
                        .splitlines()
                    )
                    self.assertEqual(
                        [json.loads(line) for line in lines],
                        [{"word": "système"}] if query_id == 3 else [],
                    )

                self.assertEqual(
                    len(messaging.published),
                    len(articles) + 1,
                )

                end = messaging.published[-1]
                self.assertIsInstance(end, RoutedInputEndMessage)
                self.assertTrue(end.execution_id)
                self.assertEqual(
                    end.reply_queue,
                    messaging.result_queue_name,
                )
                self.assertEqual(
                    end.message_id,
                    build_message_id(
                        execution_id=end.execution_id,
                        producer_id="gateway",
                        operation="input_end",
                        identity={"article_count": len(articles)},
                    ),
                )

                if articles:
                    sent = messaging.published[0]
                    self.assertIsInstance(sent, RoutedArticleMessage)
                    self.assertEqual(
                        sent.execution_id,
                        end.execution_id,
                    )
                    self.assertEqual(
                        sent.reply_queue,
                        messaging.result_queue_name,
                    )
                    self.assertEqual(sent.article, article)
                    self.assertEqual(
                        sent.message_id,
                        build_message_id(
                            execution_id=sent.execution_id,
                            producer_id="gateway",
                            operation="article",
                            identity={"sequence": 0},
                        ),
                    )

    def test_disconnect_during_upload_ends_input_without_waiting_for_results(self):
        messaging = FakeMessaging()
        with (
            patch("gateway.handler.recv_msg", side_effect=EOFError("disconnected")),
            patch("gateway.handler.send_msg") as send,
        ):
            handle_client(Mock(), lambda _: messaging, threading.Event())

        self.assertEqual(len(messaging.published), 1)
        self.assertIsInstance(messaging.published[0], RoutedInputEndMessage)
        self.assertEqual(
            messaging.published[0].message_id,
            build_message_id(
                execution_id=messaging.published[0].execution_id,
                producer_id="gateway",
                operation="input_end",
                identity={"article_count": 0},
            ),
        )
        self.assertEqual(messaging.acks, [])
        self.assertEqual(messaging.nacks, [])
        self.assertFalse(messaging.consumption_stopped)
        self.assertTrue(messaging.closed)
        send.assert_not_called()

    def test_lost_article_ack_preserves_published_count_in_input_end(self):
        messaging = FakeMessaging()
        article = {"name": "Published before disconnect"}
        with (
            patch(
                "gateway.handler.recv_msg",
                return_value=ArticleMessage(article=article),
            ),
            patch("gateway.handler.send_msg", side_effect=BrokenPipeError),
        ):
            handle_client(Mock(), lambda _: messaging, threading.Event())

        self.assertEqual(len(messaging.published), 2)
        published_article, input_end = messaging.published
        self.assertIsInstance(published_article, RoutedArticleMessage)
        self.assertIsInstance(input_end, RoutedInputEndMessage)
        self.assertEqual(
            input_end.message_id,
            build_message_id(
                execution_id=input_end.execution_id,
                producer_id="gateway",
                operation="input_end",
                identity={"article_count": 1},
            ),
        )

    def test_deadline_expires_while_waiting_for_first_result(self):
        messaging = FakeMessaging()
        messaging.start_consuming_results = Mock(
            side_effect=lambda _, on_idle: on_idle()
        )
        client_socket = Mock()
        with (
            patch("gateway.handler.monotonic", return_value=100),
            patch("common.message_protocol.external.monotonic", return_value=111),
            patch("gateway.handler.recv_msg", return_value=InputEndMessage()),
            patch("gateway.handler.send_msg") as send,
        ):
            with self.assertRaisesRegex(TimeoutError, "deadline exceeded"):
                handle_client(
                    client_socket,
                    lambda _: messaging,
                    threading.Event(),
                    timeout_seconds=10,
                )

        self.assertIsInstance(send.call_args.args[1], ErrorMessage)
        self.assertEqual(messaging.terminal[0].cause, "timeout")
        self.assertTrue(messaging.closed)

    def test_gateway_detects_disconnect_before_first_result(self):
        messaging = FakeMessaging()
        messaging.start_consuming_results = Mock(
            side_effect=lambda _, on_idle: on_idle()
        )
        client_socket = Mock()
        client_socket.recv.return_value = b""
        with (
            patch("gateway.handler.recv_msg", return_value=InputEndMessage()),
            patch("gateway.handler.send_msg"),
            patch("gateway.handler.select", return_value=([client_socket], [], [])),
        ):
            handle_client(client_socket, lambda _: messaging, threading.Event())

        self.assertTrue(messaging.consumption_stopped)
        self.assertIsInstance(messaging.terminal[0], ExecutionFailedMessage)
        self.assertEqual(messaging.terminal[0].cause, "client_disconnected")
        self.assertTrue(messaging.closed)

    def test_gateway_reports_completion_after_client_acks(self):
        messaging = FakeMessaging()
        with (
            patch(
                "gateway.handler.recv_msg",
                side_effect=[InputEndMessage(), *[AckMessage() for _ in range(6)]],
            ),
            patch("gateway.handler.send_msg"),
        ):
            handle_client(Mock(), lambda _: messaging, threading.Event())

        self.assertEqual(len(messaging.terminal), 1)
        self.assertIsInstance(messaging.terminal[0], ExecutionCompletedMessage)
        self.assertEqual(messaging.terminal[0].cause, "all_queries_acknowledged")

    def test_disconnect_during_results_discards_remaining_results(self):
        messaging = FakeMessaging()
        with (
            patch("gateway.handler.recv_msg", return_value=InputEndMessage()),
            patch(
                "gateway.handler.send_msg", side_effect=[None, BrokenPipeError]
            ) as send,
        ):
            handle_client(Mock(), lambda _: messaging, threading.Event())
        self.assertEqual(len(messaging.published), 1)
        self.assertEqual(len(messaging.acks), 6)
        self.assertEqual(messaging.nacks, [])
        self.assertEqual(send.call_count, 2)
        self.assertIsInstance(messaging.terminal[0], ExecutionFailedMessage)
        self.assertEqual(messaging.terminal[0].cause, "client_disconnected")
        self.assertTrue(messaging.closed)

    def test_disconnect_during_end_ack_does_not_duplicate_end(self):
        messaging = FakeMessaging()
        with (
            patch("gateway.handler.recv_msg", return_value=InputEndMessage()),
            patch("gateway.handler.send_msg", side_effect=BrokenPipeError),
        ):
            handle_client(Mock(), lambda _: messaging, threading.Event())
        self.assertEqual(len(messaging.published), 1)
        self.assertEqual(len(messaging.acks), 6)
        self.assertTrue(messaging.closed)

    def test_client_rejects_wrong_upload_ack(self):
        with patch("client.client.recv_msg", return_value=InputEndMessage()):
            with self.assertRaisesRegex(ValueError, "ACK"):
                Client("unused", 0).send_articles(Mock(), [])

    def test_client_surfaces_gateway_error(self):
        with patch(
            "client.client.recv_msg", return_value=ErrorMessage("publish failed")
        ):
            with self.assertRaisesRegex(RuntimeError, "publish failed"):
                Client("unused", 0).send_articles(Mock(), [])

    def test_gateway_does_not_ack_failed_publication(self):
        messaging = Mock()
        messaging.publish.side_effect = RuntimeError("middleware unavailable")

        with (
            patch("gateway.handler.recv_msg", return_value=InputEndMessage()),
            patch("gateway.handler.send_msg") as send,
        ):
            with self.assertRaises(RuntimeError):
                handle_client(Mock(), lambda _: messaging, threading.Event())

        send.assert_called_once()
        self.assertIsInstance(send.call_args.args[1], ErrorMessage)
        messaging.close.assert_called_once()

    def test_gateway_rejects_unexpected_input(self):
        messaging = Mock()

        for message in (AckMessage(), ArticleMessage([])):
            with (
                self.subTest(message=message),
                patch("gateway.handler.recv_msg", return_value=message),
                patch("gateway.handler.send_msg"),
            ):
                with self.assertRaises(ValueError):
                    handle_client(Mock(), lambda _: messaging, threading.Event())

        messaging.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
