import math
import struct
from collections.abc import Set
from io import BytesIO
from socket import socket
from typing import Any

from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
    InputEndMessage,
    Message,
    QueryEndMessage,
    ResultMessage,
)

MAX_MESSAGE_SIZE: int = 64 * 1024 * 1024
QUERY_IDS = frozenset(range(1, 6))
_MESSAGE_ARTICLE = 1
_MESSAGE_INPUT_END = 2
_MESSAGE_ACK = 3
_MESSAGE_ERROR = 4
_MESSAGE_RESULT = 5
_MESSAGE_QUERY_END = 6
_VALUE_NONE = 0
_VALUE_FALSE = 1
_VALUE_TRUE = 2
_VALUE_INTEGER = 3
_VALUE_FLOAT = 4
_VALUE_STRING = 5
_VALUE_BYTES = 6
_VALUE_LIST = 7
_VALUE_DICT = 8

_MAX_UINT32 = (1 << 32) - 1


def _encode_length(length: int) -> bytes:
    if not 0 <= length <= _MAX_UINT32:
        raise ValueError("Value length exceeds the permitted size")

    return struct.pack("!I", length)


def _encode_value(value: Any) -> bytes:
    if value is None:
        return bytes([_VALUE_NONE])

    if value is False:
        return bytes([_VALUE_FALSE])

    if value is True:
        return bytes([_VALUE_TRUE])

    if type(value) is int:
        try:
            encoded_integer = struct.pack("!q", value)
        except struct.error as error:
            raise ValueError("Integer is outside the signed 64-bit range") from error

        return bytes([_VALUE_INTEGER]) + encoded_integer

    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Float must be finite")

        return bytes([_VALUE_FLOAT]) + struct.pack("!d", value)

    if isinstance(value, str):
        encoded_string = value.encode("utf-8")

        return (
            bytes([_VALUE_STRING])
            + _encode_length(len(encoded_string))
            + encoded_string
        )

    if isinstance(value, bytes):
        return bytes([_VALUE_BYTES]) + _encode_length(len(value)) + value

    if isinstance(value, list):
        encoded_items = [_encode_value(item) for item in value]

        return (
            bytes([_VALUE_LIST])
            + _encode_length(len(encoded_items))
            + b"".join(encoded_items)
        )

    if isinstance(value, dict):
        encoded_items = []

        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Dictionary keys must be strings")

            encoded_items.append(_encode_value(key))
            encoded_items.append(_encode_value(item))

        return (
            bytes([_VALUE_DICT]) + _encode_length(len(value)) + b"".join(encoded_items)
        )

    raise TypeError(f"Unsupported value type: {type(value).__name__}")


def _read_exact(stream: BytesIO, size: int) -> bytes:
    data = stream.read(size)

    if len(data) != size:
        raise ValueError("Binary value ended unexpectedly")

    return data


def _decode_length(stream: BytesIO) -> int:
    return struct.unpack("!I", _read_exact(stream, 4))[0]


def _decode_value(stream: BytesIO) -> Any:
    value_type = _read_exact(stream, 1)[0]

    if value_type == _VALUE_NONE:
        return None

    if value_type == _VALUE_FALSE:
        return False

    if value_type == _VALUE_TRUE:
        return True

    if value_type == _VALUE_INTEGER:
        return struct.unpack("!q", _read_exact(stream, 8))[0]

    if value_type == _VALUE_FLOAT:
        value = struct.unpack("!d", _read_exact(stream, 8))[0]

        if not math.isfinite(value):
            raise ValueError("Float must be finite")

        return value

    if value_type == _VALUE_STRING:
        length = _decode_length(stream)
        return _read_exact(stream, length).decode("utf-8")

    if value_type == _VALUE_BYTES:
        length = _decode_length(stream)
        return _read_exact(stream, length)

    if value_type == _VALUE_LIST:
        item_count = _decode_length(stream)
        return [_decode_value(stream) for _ in range(item_count)]

    if value_type == _VALUE_DICT:
        item_count = _decode_length(stream)
        decoded = {}

        for _ in range(item_count):
            key = _decode_value(stream)

            if not isinstance(key, str):
                raise ValueError("Dictionary key must be a string")

            decoded[key] = _decode_value(stream)

        return decoded

    raise ValueError(f"Unknown binary value type: {value_type}")


def _encode_message(message: Message) -> bytes:
    if type(message) is ArticleMessage:
        return bytes([_MESSAGE_ARTICLE]) + _encode_value(message.article)

    if type(message) is InputEndMessage:
        return bytes([_MESSAGE_INPUT_END])

    if type(message) is AckMessage:
        return bytes([_MESSAGE_ACK])

    if type(message) is ErrorMessage:
        return bytes([_MESSAGE_ERROR]) + _encode_value(message.message)

    if type(message) is ResultMessage:
        return (
            bytes([_MESSAGE_RESULT])
            + _encode_value(message.query_id)
            + _encode_value(message.execution_id)
            + _encode_value(message.record)
        )

    if type(message) is QueryEndMessage:
        return (
            bytes([_MESSAGE_QUERY_END])
            + _encode_value(message.query_id)
            + _encode_value(message.execution_id)
        )

    raise TypeError(
        f"Message is not part of the external protocol: {type(message).__name__}"
    )


def _decode_message(body: bytes) -> Message:
    stream = BytesIO(body)
    message_type = _read_exact(stream, 1)[0]

    if message_type == _MESSAGE_ARTICLE:
        article = _decode_value(stream)

        if not isinstance(article, dict):
            raise ValueError("ARTICLE requires a dictionary")

        message = ArticleMessage(article=article)

    elif message_type == _MESSAGE_INPUT_END:
        message = InputEndMessage()

    elif message_type == _MESSAGE_ACK:
        message = AckMessage()

    elif message_type == _MESSAGE_ERROR:
        error_message = _decode_value(stream)

        if not isinstance(error_message, str):
            raise ValueError("ERROR requires a string")

        message = ErrorMessage(message=error_message)

    elif message_type == _MESSAGE_RESULT:
        query_id = _decode_value(stream)
        execution_id = _decode_value(stream)
        record = _decode_value(stream)

        if type(query_id) is not int:
            raise ValueError("RESULT requires an integer query_id")

        if not isinstance(execution_id, str):
            raise ValueError("RESULT requires a string execution_id")

        if not isinstance(record, dict):
            raise ValueError("RESULT requires a dictionary record")

        message = ResultMessage(
            query_id=query_id,
            execution_id=execution_id,
            record=record,
        )

    elif message_type == _MESSAGE_QUERY_END:
        query_id = _decode_value(stream)
        execution_id = _decode_value(stream)

        if type(query_id) is not int:
            raise ValueError("QUERY_END requires an integer query_id")

        if not isinstance(execution_id, str):
            raise ValueError("QUERY_END requires a string execution_id")

        message = QueryEndMessage(
            query_id=query_id,
            execution_id=execution_id,
        )

    else:
        raise ValueError(f"Unknown external message type: {message_type}")

    if stream.read(1):
        raise ValueError("External message contains unexpected trailing bytes")

    return message


def _recv_exact(sock: socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("Connection closed before the complete message arrived")
        data.extend(chunk)
    return bytes(data)


def send_msg(sock: socket, message: Message) -> None:
    body = _encode_message(message)

    if not 0 < len(body) <= MAX_MESSAGE_SIZE:
        raise ValueError("Message exceeds the permitted size")

    sock.sendall(struct.pack("!I", len(body)) + body)


def recv_msg(sock: socket) -> Message:
    size = struct.unpack("!I", _recv_exact(sock, 4))[0]

    if not 0 < size <= MAX_MESSAGE_SIZE:
        raise ValueError("Invalid message size")

    return _decode_message(_recv_exact(sock, size))


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
