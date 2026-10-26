import struct
import unittest
from unittest.mock import Mock

from common.message_protocol.external import MAX_MESSAGE_SIZE, recv_msg, send_msg
from common.message_protocol.message import AckMessage, ArticleMessage


class TestExternalProtocol(unittest.TestCase):
    def test_message_type_is_binary(self):
        sender = Mock()
        send_msg(sender, AckMessage())
        frame = sender.sendall.call_args.args[0]
        # El primer byte del cuerpo contiene el codigo binario 3 para ACK.
        self.assertEqual(frame[4:5], b"\x03")
        receiver = Mock()
        receiver.recv.side_effect = [frame[:4], frame[4:]]
        self.assertEqual(recv_msg(receiver), AckMessage())

    def test_fragmented_utf8_message(self):
        message = ArticleMessage(article={"name": "système"})
        sender = Mock()
        send_msg(sender, message)
        frame = sender.sendall.call_args.args[0]
        self.assertEqual(struct.unpack("!I", frame[:4])[0], len(frame[4:]))
        receiver = Mock()
        receiver.recv.side_effect = [bytes([byte]) for byte in frame]
        self.assertEqual(recv_msg(receiver), message)

    def test_disconnect_in_header_or_body(self):
        for chunks in ([b"\x00", b""], [struct.pack("!I", 10), b"{}", b""]):
            with self.subTest(chunks=chunks):
                sock = Mock()
                sock.recv.side_effect = chunks
                with self.assertRaises(EOFError):
                    recv_msg(sock)

    def test_rejects_invalid_frame_lengths(self):
        for size in (0, MAX_MESSAGE_SIZE + 1):
            sock = Mock()
            sock.recv.return_value = struct.pack("!I", size)
            with self.assertRaises(ValueError):
                recv_msg(sock)
            self.assertEqual(sock.recv.call_count, 1)

    def test_rejects_non_message_binary_payloads(self):
        # Se rechazan tipos desconocidos y datos sobrantes en el cuerpo binario.
        for body in (b"\xff", b"\x03\x00"):
            sock = Mock()
            sock.recv.side_effect = [struct.pack("!I", len(body)), body]
            with self.assertRaises(ValueError):
                recv_msg(sock)


if __name__ == "__main__":
    unittest.main()
