import struct
import unittest
from unittest.mock import Mock

from common.message_protocol.external import MAX_MESSAGE_SIZE, recv_msg, send_msg
from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
    InputEndMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedArticleMessage,
)


class TestExternalProtocol(unittest.TestCase):
    def test_external_messages_round_trip(self):
        messages = [
            ArticleMessage(
                article={
                    "name": "École",
                    "abstract": None,
                    "date_modified": "2020-01-01T00:00:00",
                    "image": {
                        "content_url": "https://example.com/image.jpg",
                        "width": 200,
                        "height": 100,
                    },
                    "in_language": {"identifier": "fr"},
                    "sections": '[{"name": "Biographie"}]',
                    "url": "https://example.com/article",
                }
            ),
            InputEndMessage(),
            AckMessage(),
            ErrorMessage(message="middleware unavailable"),
            ResultMessage(
                query_id=2,
                execution_id="execution-123",
                record={
                    "name": "École",
                    "image": b"\x00\xff",
                },
            ),
            QueryEndMessage(
                query_id=2,
                execution_id="execution-123",
            ),
        ]

        for message in messages:
            with self.subTest(message=message):
                sender = Mock()
                send_msg(sender, message)
                frame = sender.sendall.call_args.args[0]

                receiver = Mock()
                receiver.recv.side_effect = [frame[:4], frame[4:]]

                self.assertEqual(recv_msg(receiver), message)

    def test_ack_has_binary_frame(self):
        sender = Mock()
        send_msg(sender, AckMessage())

        frame = sender.sendall.call_args.args[0]

        self.assertEqual(frame, struct.pack("!I", 1) + b"\x03")

    def test_fragmented_utf8_message(self):
        message = ArticleMessage(article={"name": "système"})
        sender = Mock()
        send_msg(sender, message)
        frame = sender.sendall.call_args.args[0]

        self.assertEqual(
            struct.unpack("!I", frame[:4])[0],
            len(frame[4:]),
        )

        receiver = Mock()
        receiver.recv.side_effect = [bytes([byte]) for byte in frame]

        self.assertEqual(recv_msg(receiver), message)

    def test_disconnect_in_header_or_body(self):
        cases = [
            [b"\x00", b""],
            [struct.pack("!I", 10), b"\x03", b""],
        ]

        for chunks in cases:
            with self.subTest(chunks=chunks):
                sock = Mock()
                sock.recv.side_effect = chunks

                with self.assertRaises(EOFError):
                    recv_msg(sock)

    def test_rejects_invalid_frame_lengths(self):
        for size in (0, MAX_MESSAGE_SIZE + 1):
            with self.subTest(size=size):
                sock = Mock()
                sock.recv.return_value = struct.pack("!I", size)

                with self.assertRaises(ValueError):
                    recv_msg(sock)

                self.assertEqual(sock.recv.call_count, 1)

    def test_rejects_invalid_binary_messages(self):
        invalid_bodies = [
            b"\xff",
            b"\x03\x00",
            b"\x01\x00",
        ]

        for body in invalid_bodies:
            with self.subTest(body=body):
                sock = Mock()
                sock.recv.side_effect = [
                    struct.pack("!I", len(body)),
                    body,
                ]

                with self.assertRaises(ValueError):
                    recv_msg(sock)

    def test_rejects_internal_messages(self):
        message = RoutedArticleMessage(
            article={"name": "École"},
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaises(TypeError):
            send_msg(Mock(), message)


if __name__ == "__main__":
    unittest.main()
