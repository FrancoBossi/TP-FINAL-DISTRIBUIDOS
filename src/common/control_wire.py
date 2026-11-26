"""Framing for the control plane (health checks and FailureManager traffic).

Each frame is a four byte length in network order followed by one dictionary
encoded with the project's own binary codec. The dictionary always carries a
string ``type``. No external library takes part in the wire format.
"""

import struct
from socket import socket
from typing import Any

from common.message_protocol.external import decode_value, encode_value

MAX_FRAME_SIZE: int = 64 * 1024


class ProtocolError(Exception):
    """A peer sent a frame that is not a valid control message."""


def _recv_exact(sock: socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("Connection closed before the complete frame arrived")
        data.extend(chunk)
    return bytes(data)


def send_frame(sock: socket, message: dict[str, Any]) -> None:
    if not isinstance(message.get("type"), str):
        raise ProtocolError("Control message requires a string type")

    body = encode_value(message)

    if not 0 < len(body) <= MAX_FRAME_SIZE:
        raise ProtocolError("Control message exceeds the permitted size")

    sock.sendall(struct.pack("!I", len(body)) + body)


def recv_frame(sock: socket) -> dict[str, Any]:
    size = struct.unpack("!I", _recv_exact(sock, 4))[0]

    if not 0 < size <= MAX_FRAME_SIZE:
        raise ProtocolError("Invalid control frame size")

    try:
        message = decode_value(_recv_exact(sock, size))
    except (ValueError, UnicodeDecodeError) as error:
        raise ProtocolError(f"Malformed control frame: {error}") from error

    if not isinstance(message, dict) or not isinstance(message.get("type"), str):
        raise ProtocolError("Control frame must be a dictionary with a string type")

    return message
