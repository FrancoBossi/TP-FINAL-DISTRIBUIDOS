import struct
from collections.abc import Set
from socket import socket

from common.message_protocol.message import Message, QueryEndMessage, ResultMessage

MAX_MESSAGE_SIZE: int = 64 * 1024 * 1024
QUERY_IDS = frozenset(range(1, 6))


def _recv_exact(sock: socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("Connection closed before the complete message arrived")
        data.extend(chunk)
    return bytes(data)


def send_msg(sock: socket, message: Message) -> None:
    body = message.serialize()
    if not 0 < len(body) <= MAX_MESSAGE_SIZE:
        raise ValueError("Message exceeds the permitted size")
    sock.sendall(struct.pack("!I", len(body)) + body)


def recv_msg(sock: socket) -> Message:
    size = struct.unpack("!I", _recv_exact(sock, 4))[0]
    if not 0 < size <= MAX_MESSAGE_SIZE:
        raise ValueError("Invalid message size")
    return Message.deserialize(_recv_exact(sock, size))


def validate_result(message: Message, completed: Set[int]) -> int:
    if not isinstance(message, (ResultMessage, QueryEndMessage)):
        raise ValueError("Expected RESULT or QUERY_END")
    query_id = message.query_id
    if type(query_id) is not int or query_id not in QUERY_IDS:
        raise ValueError("Invalid query_id")
    if query_id in completed:
        raise ValueError("Received a message after QUERY_END")
    if isinstance(message, ResultMessage) and not isinstance(message.record, dict):
        raise ValueError("RESULT requires a record object")
    return query_id
