import json
from dataclasses import asdict, dataclass
from typing import Any, ClassVar

from common.message_protocol.message_type import MessageType


class Message:
    type: ClassVar[MessageType]

    def serialize(self) -> bytes:
        fields = {"type": self.type, **asdict(self)}
        return json.dumps(fields, ensure_ascii=False, allow_nan=False).encode("utf-8")

    @staticmethod
    def deserialize(body: bytes) -> "Message":
        fields = json.loads(body)

        if not isinstance(fields, dict) or not isinstance(fields.get("type"), str):
            raise ValueError("Expected a message object with a string type")

        kind = MessageType(fields.pop("type"))
        message_class = MESSAGE_CLASSES[kind]

        if "execution_id" in fields:
            message_class = ROUTED_CLASSES.get(kind, message_class)

        try:
            return message_class(**fields)
        except TypeError as error:
            raise ValueError(f"Invalid fields for {kind}: {error}") from error


@dataclass(frozen=True)
class ArticleMessage(Message):
    article: dict[str, Any]
    type: ClassVar[MessageType] = MessageType.ARTICLE


@dataclass(frozen=True)
class RoutedArticleMessage(ArticleMessage):
    execution_id: str
    reply_queue: str


@dataclass(frozen=True)
class ArticleBatchMessage(Message):
    articles: list[dict[str, Any]]
    execution_id: str
    reply_queue: str
    type: ClassVar[MessageType] = MessageType.ARTICLE_BATCH


@dataclass(frozen=True)
class NormalizedArticleBatchMessage(Message):
    articles: list[dict[str, Any]]
    execution_id: str
    reply_queue: str
    type: ClassVar[MessageType] = MessageType.NORMALIZED_ARTICLE_BATCH


@dataclass(frozen=True)
class Q1Q2ArticleBatchMessage(Message):
    articles: list[dict[str, Any]]
    execution_id: str
    reply_queue: str
    type: ClassVar[MessageType] = MessageType.Q1_Q2_ARTICLE_BATCH


@dataclass(frozen=True)
class TextArticleBatchMessage(Message):
    articles: list[dict[str, Any]]
    execution_id: str
    reply_queue: str
    type: ClassVar[MessageType] = MessageType.TEXT_ARTICLE_BATCH


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


MESSAGE_CLASSES: dict[MessageType, type[Message]] = {
    MessageType.ARTICLE: ArticleMessage,
    MessageType.ARTICLE_BATCH: ArticleBatchMessage,
    MessageType.NORMALIZED_ARTICLE_BATCH: NormalizedArticleBatchMessage,
    MessageType.INPUT_END: InputEndMessage,
    MessageType.ACK: AckMessage,
    MessageType.ERROR: ErrorMessage,
    MessageType.RESULT: ResultMessage,
    MessageType.QUERY_END: QueryEndMessage,
    MessageType.Q1_Q2_ARTICLE_BATCH: Q1Q2ArticleBatchMessage,
    MessageType.TEXT_ARTICLE_BATCH: TextArticleBatchMessage,
}

ROUTED_CLASSES: dict[MessageType, type[Message]] = {
    MessageType.ARTICLE: RoutedArticleMessage,
    MessageType.INPUT_END: RoutedInputEndMessage,
}
