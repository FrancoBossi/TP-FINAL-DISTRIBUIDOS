import socket
import struct
import unittest

from common.control_wire import (
    MAX_FRAME_SIZE,
    ProtocolError,
    recv_frame,
    send_frame,
)
from common.message_protocol.external import encode_value


class TestControlWire(unittest.TestCase):
    def setUp(self):
        self.left, self.right = socket.socketpair()
        self.addCleanup(self.left.close)
        self.addCleanup(self.right.close)

    def test_round_trips_nested_control_messages(self):
        message = {
            "type": "PONG",
            "name": "preprocessor-0",
            "nonce": 7,
            "nested": {"epoch": 3, "ok": True, "detail": None, "ratio": 0.5},
            "targets": ["a", "b"],
        }

        send_frame(self.left, message)

        self.assertEqual(recv_frame(self.right), message)

    def test_rejects_message_without_string_type(self):
        with self.assertRaises(ProtocolError):
            send_frame(self.left, {"nonce": 1})

        with self.assertRaises(ProtocolError):
            send_frame(self.left, {"type": 3})

    def test_rejects_oversized_message_before_sending(self):
        with self.assertRaises(ProtocolError):
            send_frame(self.left, {"type": "X", "blob": "a" * MAX_FRAME_SIZE})

    def test_rejects_declared_size_outside_limits(self):
        for size in (0, MAX_FRAME_SIZE + 1):
            with self.subTest(size=size):
                self.left.sendall(struct.pack("!I", size))
                with self.assertRaises(ProtocolError):
                    recv_frame(self.right)

    def test_rejects_malformed_body(self):
        body = b"\xff\xff"
        self.left.sendall(struct.pack("!I", len(body)) + body)

        with self.assertRaises(ProtocolError):
            recv_frame(self.right)

    def test_rejects_trailing_bytes(self):
        body = encode_value({"type": "PING"}) + b"x"
        self.left.sendall(struct.pack("!I", len(body)) + body)

        with self.assertRaises(ProtocolError):
            recv_frame(self.right)

    def test_rejects_values_that_are_not_typed_dictionaries(self):
        for value in ([1, 2], "PING", {"nonce": 1}, {"type": 4}):
            with self.subTest(value=value):
                body = encode_value(value)
                self.left.sendall(struct.pack("!I", len(body)) + body)
                with self.assertRaises(ProtocolError):
                    recv_frame(self.right)

    def test_truncated_frame_reports_closed_connection(self):
        body = encode_value({"type": "PING"})
        self.left.sendall(struct.pack("!I", len(body)) + body[:-1])
        self.left.close()

        with self.assertRaises(EOFError):
            recv_frame(self.right)


if __name__ == "__main__":
    unittest.main()
