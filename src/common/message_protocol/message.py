import struct
from dataclasses import dataclass
from math import isfinite
from typing import Any, ClassVar

from common.message_protocol.message_type import MessageType

MAX_COLLECTION_ITEMS = 1_000_000
MAX_STRING_BYTES = 64 * 1024 * 1024

# Cada tipo de mensaje tiene un codigo fijo para no transmitir nombres JSON.
TYPE_CODES = {
    MessageType.ARTICLE: 1,
    MessageType.INPUT_END: 2,
    MessageType.ACK: 3,
    MessageType.ERROR: 4,
    MessageType.RESULT: 5,
    MessageType.QUERY_END: 6,
}
CODES_TO_TYPES = {code: message_type for message_type, code in TYPE_CODES.items()}

NONE = 0
FALSE = 1
TRUE = 2
INT = 3
FLOAT = 4
STRING = 5
BYTES = 6
LIST = 7
DICT = 8


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.position = 0

    def read(self, size: int) -> bytes:
        end = self.position + size
        if size < 0 or end > len(self.data):
            raise ValueError("Truncated binary message")
        value = self.data[self.position:end]
        self.position = end
        return value

    def unpack(self, format: str) -> Any:
        size = struct.calcsize(format)
        return struct.unpack(format, self.read(size))[0]


def pack_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > MAX_STRING_BYTES:
        raise ValueError("String exceeds the permitted size")
    return struct.pack("!I", len(encoded)) + encoded


def encode_value(value: Any) -> bytes:
    # Cada valor comienza con un byte de tipo y luego sus datos codificados.
    if value is None:
        return bytes([NONE])
    if value is False:
        return bytes([FALSE])
    if value is True:
        return bytes([TRUE])
    if isinstance(value, int):
        return bytes([INT]) + struct.pack("!q", value)
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError("Non-finite float values are not supported")
        return bytes([FLOAT]) + struct.pack("!d", value)
    if isinstance(value, str):
        return bytes([STRING]) + pack_string(value)
    if isinstance(value, bytes):
        if len(value) > MAX_STRING_BYTES:
            raise ValueError("Binary value exceeds the permitted size")
        return bytes([BYTES]) + struct.pack("!I", len(value)) + value
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ValueError("Collection exceeds the permitted item count")
        return (
            bytes([LIST])
            + struct.pack("!I", len(value))
            + b"".join(encode_value(item) for item in value)
        )
    if isinstance(value, dict):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ValueError("Collection exceeds the permitted item count")
        if not all(isinstance(key, str) for key in value):
            raise ValueError("Dictionary keys must be strings")
        encoded = bytearray(bytes([DICT]) + struct.pack("!I", len(value)))
        for key in sorted(value):
            encoded.extend(pack_string(key))
            encoded.extend(encode_value(value[key]))
        return bytes(encoded)
    raise TypeError(f"Unsupported value type: {type(value).__name__}")


def decode_value(reader: _Reader, depth: int = 0) -> Any:
    # El decoder lee el mismo formato que encode_value para reconstruir el valor.
    if depth > 100:
        raise ValueError("Message nesting is too deep")
    value_type = reader.unpack("!B")
    if value_type == NONE:
        return None
    if value_type == FALSE:
        return False
    if value_type == TRUE:
        return True
    if value_type == INT:
        return reader.unpack("!q")
    if value_type == FLOAT:
        return reader.unpack("!d")
    if value_type in (STRING, BYTES):
        size = reader.unpack("!I")
        if size > MAX_STRING_BYTES:
            raise ValueError("Value exceeds the permitted size")
        raw = reader.read(size)
        if value_type == BYTES:
            return raw
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("Invalid UTF-8 string") from error
    if value_type in (LIST, DICT):
        count = reader.unpack("!I")
        if count > MAX_COLLECTION_ITEMS:
            raise ValueError("Collection exceeds the permitted item count")
        if value_type == LIST:
            return [decode_value(reader, depth + 1) for _ in range(count)]
        result = {}
        for _ in range(count):
            key = read_raw_string(reader)
            if key in result:
                raise ValueError("Duplicate dictionary key")
            result[key] = decode_value(reader, depth + 1)
        return result
    raise ValueError("Unknown binary value type")


def encode_fields(message: "Message") -> bytes:
    if isinstance(message, (ArticleMessage, RoutedArticleMessage)):
        fields = [encode_value(message.article)]
        if isinstance(message, RoutedArticleMessage):
            fields.extend(
                (encode_value(message.execution_id), encode_value(message.reply_queue))
            )
        return b"".join(fields)
    if isinstance(message, RoutedInputEndMessage):
        return encode_value(message.execution_id) + encode_value(message.reply_queue)
    if isinstance(message, ErrorMessage):
        return encode_value(message.message)
    if isinstance(message, ResultMessage):
        return (
            struct.pack("!B", message.query_id)
            + encode_value(message.record)
            + encode_value(message.execution_id)
        )
    if isinstance(message, QueryEndMessage):
        return struct.pack("!B", message.query_id) + encode_value(message.execution_id)
    return b""


def read_string(reader: _Reader) -> str:
    value = decode_value(reader)
    if not isinstance(value, str):
        raise ValueError("Expected a string field")
    return value


def read_raw_string(reader: _Reader) -> str:
    size = reader.unpack("!I")
    if size > MAX_STRING_BYTES:
        raise ValueError("String exceeds the permitted size")
    try:
        return reader.read(size).decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("Invalid UTF-8 string") from error


class Message:
    type: ClassVar[MessageType]

    def serialize(self) -> bytes:
        # El primer byte identifica el tipo de mensaje.
        return bytes([TYPE_CODES[self.type]]) + encode_fields(self)

    @staticmethod
    def deserialize(body: bytes) -> "Message":
        reader = _Reader(body)
        try:
            message_type = CODES_TO_TYPES[reader.unpack("!B")]
        except KeyError as error:
            raise ValueError("Unknown message type") from error

        try:
            if message_type == MessageType.ARTICLE:
                article = decode_value(reader)
                if not isinstance(article, dict):
                    raise ValueError("ARTICLE requires an article object")
                if reader.position < len(body):
                    message = RoutedArticleMessage(
                        article=article,
                        execution_id=read_string(reader),
                        reply_queue=read_string(reader),
                    )
                else:
                    message = ArticleMessage(article=article)
            elif message_type == MessageType.INPUT_END:
                if reader.position < len(body):
                    message = RoutedInputEndMessage(
                        execution_id=read_string(reader),
                        reply_queue=read_string(reader),
                    )
                else:
                    message = InputEndMessage()
            elif message_type == MessageType.ACK:
                message = AckMessage()
            elif message_type == MessageType.ERROR:
                message = ErrorMessage(message=read_string(reader))
            elif message_type == MessageType.RESULT:
                query_id = reader.unpack("!B")
                record = decode_value(reader)
                if not isinstance(record, dict):
                    raise ValueError("RESULT requires a record object")
                message = ResultMessage(
                    query_id=query_id,
                    record=record,
                    execution_id=read_string(reader),
                )
            else:
                message = QueryEndMessage(
                    query_id=reader.unpack("!B"),
                    execution_id=read_string(reader),
                )
        except (IndexError, struct.error) as error:
            raise ValueError("Invalid binary message fields") from error

        if reader.position != len(body):
            raise ValueError("Unexpected trailing binary message data")
        return message


@dataclass(frozen=True)
class ArticleMessage(Message):
    article: dict[str, Any]
    type: ClassVar[MessageType] = MessageType.ARTICLE


@dataclass(frozen=True)
class RoutedArticleMessage(ArticleMessage):
    execution_id: str
    reply_queue: str


@dataclass(frozen=True)
class InputEndMessage(Message):
    type: ClassVar[MessageType] = MessageType.INPUT_END


@dataclass(frozen=True)
class RoutedInputEndMessage(InputEndMessage):
    execution_id: str
    reply_queue: str


@dataclass(frozen=True)
class AckMessage(Message):
    type: ClassVar[MessageType] = MessageType.ACK


@dataclass(frozen=True)
class ErrorMessage(Message):
    message: str
    type: ClassVar[MessageType] = MessageType.ERROR


@dataclass(frozen=True)
class ResultMessage(Message):
    query_id: int
    record: dict[str, Any]
    execution_id: str
    type: ClassVar[MessageType] = MessageType.RESULT


@dataclass(frozen=True)
class QueryEndMessage(Message):
    query_id: int
    execution_id: str
    type: ClassVar[MessageType] = MessageType.QUERY_END
