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


class FakeBroker:
    def __init__(self):
        self.published = []
        self.closed = False
        self.reply_queue = "results.run"
        self.acks = []

    def publish(self, message):
        self.published.append(message)

    def results(self):
        run_id = self.published[-1].run_id
        yield 1, ResultMessage(3, {"word": "système"}, run_id)
        for query_id in (5, 1, 3, 2, 4):
            yield query_id + 1, QueryEndMessage(query_id, run_id)

    def acknowledge_result(self, delivery_tag):
        self.acks.append(delivery_tag)

    def close(self):
        self.closed = True


class TestClientGateway(unittest.TestCase):
    def test_upload_and_receive_all_five_queries_with_fake_broker(self):
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
                broker, errors = FakeBroker(), []
                client_socket, gateway_socket = socket.socketpair()
                client_socket.settimeout(3)
                gateway_socket.settimeout(3)

                def gateway():
                    with gateway_socket:
                        try:
                            handle_client(gateway_socket, lambda _: broker)
                        except Exception as error:
                            errors.append(error)

                thread = threading.Thread(target=gateway, daemon=True)
                thread.start()
                try:
                    with (
                        client_socket,
                        patch(
                            "client.client.iter_articles", return_value=iter(articles)
                        ),
                        patch(
                            "client.client.socket.create_connection",
                            return_value=client_socket,
                        ),
                    ):
                        Client("gateway", 5000).run("fixture.parquet", output_dir)
                finally:
                    client_socket.close()
                    thread.join(4)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertTrue(broker.closed)
                self.assertEqual(len(broker.acks), 6)
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
                self.assertEqual(len(broker.published), len(articles) + 1)
                end = broker.published[-1]
                self.assertIsInstance(end, RoutedInputEndMessage)
                self.assertTrue(end.run_id)
                if articles:
                    sent = broker.published[0]
                    self.assertIsInstance(sent, RoutedArticleMessage)
                    self.assertEqual(sent.run_id, end.run_id)
                    self.assertEqual(
                        sent.article["modification_date"], "2020-01-01T00:00:00+00:00"
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
        broker = Mock()
        broker.publish.side_effect = RuntimeError("broker unavailable")
        with (
            patch("gateway.handler.recv_msg", return_value=InputEndMessage()),
            patch("gateway.handler.send_msg") as send,
        ):
            with self.assertRaises(RuntimeError):
                handle_client(Mock(), lambda _: broker)
        send.assert_called_once()
        self.assertIsInstance(send.call_args.args[1], ErrorMessage)
        broker.close.assert_called_once()

    def test_gateway_rejects_unexpected_input(self):
        broker = Mock()
        for message in (AckMessage(), ArticleMessage([])):
            with (
                self.subTest(message=message),
                patch("gateway.handler.recv_msg", return_value=message),
                patch("gateway.handler.send_msg"),
            ):
                with self.assertRaises(ValueError):
                    handle_client(Mock(), lambda _: broker)
        broker.publish.assert_not_called()

    def test_gateway_closes_broker_on_disconnect(self):
        broker = Mock()
        with (
            patch("gateway.handler.recv_msg", side_effect=EOFError("disconnected")),
            patch("gateway.handler.send_msg", side_effect=BrokenPipeError),
        ):
            with self.assertRaises(EOFError):
                handle_client(Mock(), lambda _: broker)
        broker.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
