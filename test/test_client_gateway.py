import json
import socket
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from client.client import Client
from common.article import Article
from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
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
        self.closed = False
        self.result_queue_name = "results.run"
        self.acks = []
        self.nacks = []
        self.consumption_stopped = False

    def publish(self, message):
        self.published.append(message)

    def start_consuming_results(self, on_result):
        run_id = self.published[-1].run_id
        messages = [
            ResultMessage(3, {"word": "système"}, run_id),
            *[QueryEndMessage(query_id, run_id) for query_id in (5, 1, 3, 2, 4)],
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
        article = Article(
            "École",
            "https://example.com",
            "fr",
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            None,
            (),
            None,
        )

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
                            handle_client(gateway_socket, lambda _: messaging)
                        except Exception as error:
                            errors.append(error)

                thread = threading.Thread(target=gateway, daemon=True)
                thread.start()

                try:
                    with (
                        client_socket,
                        patch(
                            "client.client.iter_articles",
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
                self.assertTrue(end.run_id)
                self.assertEqual(
                    end.reply_queue,
                    messaging.result_queue_name,
                )

                if articles:
                    sent = messaging.published[0]
                    self.assertIsInstance(sent, RoutedArticleMessage)
                    self.assertEqual(sent.run_id, end.run_id)
                    self.assertEqual(
                        sent.reply_queue,
                        messaging.result_queue_name,
                    )
                    self.assertEqual(
                        sent.article["modification_date"],
                        "2020-01-01T00:00:00+00:00",
                    )
                    self.assertIsNone(sent.article["abstract"])
                    self.assertEqual(sent.article["sections"], [])

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
                handle_client(Mock(), lambda _: messaging)

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
                    handle_client(Mock(), lambda _: messaging)

        messaging.publish.assert_not_called()

    def test_gateway_closes_messaging_on_disconnect(self):
        messaging = Mock()

        with (
            patch("gateway.handler.recv_msg", side_effect=EOFError("disconnected")),
            patch("gateway.handler.send_msg", side_effect=BrokenPipeError),
        ):
            with self.assertRaises(EOFError):
                handle_client(Mock(), lambda _: messaging)

        messaging.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
