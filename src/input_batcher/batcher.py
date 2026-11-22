import json
import logging
from collections.abc import Mapping
from typing import Any, Optional

from common.message_identity import build_message_id
from common.message_protocol.message import (
    ArticleBatchMessage,
    Message,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)

logger = logging.getLogger(__name__)

SEPARATOR_LENGTH = 2  # ", " in JSON encoding


class BatchAccumulator:
    def __init__(self, batch_size: int, partition_id: int = 0):

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        if partition_id < 0:
            raise ValueError("partition_id must be nonnegative")

        self.batch_size = batch_size
        self.partition_id = partition_id
        self.current_batch: dict[str, ArticleBatchMessage] = {}
        self._batch_sizes: dict[str, int] = {}
        self._envelope_sizes: dict[str, int] = {}
        self._source_message_ids: dict[str, list[str]] = {}

    def add_article(
        self, message: RoutedArticleMessage
    ) -> Optional[ArticleBatchMessage]:

        current = self.current_batch.get(message.execution_id)

        if current is not None and current.reply_queue != message.reply_queue:
            raise ValueError(
                f"Execution {message.execution_id} has multiple reply queues: "
                f"{current.reply_queue} and {message.reply_queue}"
            )

        article_size, envelope_size = self._measure_article(message)

        single_size = envelope_size + article_size
        if single_size > self.batch_size:
            logger.warning(
                "Article batch is %d bytes, exceeding target %d; "
                "keeping article in a singleton batch (execution %s)",
                single_size,
                self.batch_size,
                message.execution_id,
            )
            self._start_batch(message, single_size, envelope_size)
            return current

        if current is not None:
            candidate_size = (
                self._batch_sizes[message.execution_id]
                + SEPARATOR_LENGTH
                + article_size
            )
            if candidate_size <= self.batch_size:
                source_message_ids = self._source_message_ids[message.execution_id]
                source_message_ids.append(message.message_id)
                self.current_batch[message.execution_id] = self._build_batch(
                    message,
                    [*current.articles, message.article],
                    source_message_ids,
                )
                self._batch_sizes[message.execution_id] = candidate_size
                return None

        self._start_batch(message, single_size, envelope_size)
        return current

    def _measure_article(self, message: RoutedArticleMessage) -> tuple[int, int]:
        # Match Message.serialize's JSON encoding, including UTF-8 and the
        # default comma-space separator, without re-encoding earlier articles.
        article_size = len(
            json.dumps(message.article, ensure_ascii=False, allow_nan=False).encode(
                "utf-8"
            )
        )
        if message.execution_id not in self.current_batch:
            envelope_size = len(
                ArticleBatchMessage(
                    articles=[],
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                    message_id=self._build_message_id(message.execution_id, []),
                ).serialize()
            )
        else:
            envelope_size = self._envelope_sizes[message.execution_id]

        return article_size, envelope_size

    def _start_batch(
        self,
        message: RoutedArticleMessage,
        single_size: int,
        envelope_size: int,
    ) -> None:
        source_message_ids = [message.message_id]
        self._source_message_ids[message.execution_id] = source_message_ids
        self.current_batch[message.execution_id] = self._build_batch(
            message,
            [message.article],
            source_message_ids,
        )
        self._batch_sizes[message.execution_id] = single_size
        self._envelope_sizes[message.execution_id] = envelope_size

    def flush(self, message: RoutedInputEndMessage) -> Optional[ArticleBatchMessage]:

        current = self.current_batch.get(message.execution_id)

        if current is None:
            return None

        if current.reply_queue != message.reply_queue:
            raise ValueError(
                f"Execution {message.execution_id} has multiple reply queues: "
                f"{current.reply_queue} and {message.reply_queue}"
            )

        del self._batch_sizes[message.execution_id]
        del self._envelope_sizes[message.execution_id]
        del self._source_message_ids[message.execution_id]
        return self.current_batch.pop(message.execution_id)

    def export_state(self, execution_id: str) -> dict[str, Any]:
        current = self.current_batch.get(execution_id)
        if current is None:
            return {}
        return {
            "batch": current.serialize().decode("utf-8"),
            "batch_size": self._batch_sizes[execution_id],
            "envelope_size": self._envelope_sizes[execution_id],
            "source_message_ids": list(self._source_message_ids[execution_id]),
        }

    def restore_state(
        self,
        execution_id: str,
        state: Mapping[str, Any],
    ) -> None:
        if not isinstance(state, Mapping):
            raise ValueError("InputBatcher state must be a mapping")
        if not state:
            self.current_batch.pop(execution_id, None)
            self._batch_sizes.pop(execution_id, None)
            self._envelope_sizes.pop(execution_id, None)
            self._source_message_ids.pop(execution_id, None)
            return
        if set(state) != {
            "batch",
            "batch_size",
            "envelope_size",
            "source_message_ids",
        }:
            raise ValueError("Invalid InputBatcher state fields")

        encoded_batch = state["batch"]
        batch_size = state["batch_size"]
        envelope_size = state["envelope_size"]
        source_message_ids = state["source_message_ids"]
        if not isinstance(encoded_batch, str):
            raise ValueError("InputBatcher batch must be serialized JSON")
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("InputBatcher batch size must be positive")
        if type(envelope_size) is not int or envelope_size <= 0:
            raise ValueError("InputBatcher envelope size must be positive")
        if not isinstance(source_message_ids, list) or any(
            not isinstance(message_id, str) or not message_id
            for message_id in source_message_ids
        ):
            raise ValueError("InputBatcher source IDs must be nonempty strings")

        decoded = Message.deserialize(encoded_batch.encode("utf-8"))
        if not isinstance(decoded, ArticleBatchMessage):
            raise ValueError("InputBatcher state must contain an article batch")
        if decoded.execution_id != execution_id:
            raise ValueError("InputBatcher state belongs to another execution")
        if len(decoded.articles) != len(source_message_ids):
            raise ValueError("InputBatcher articles and source IDs do not match")
        expected_message_id = self._build_message_id(
            execution_id,
            source_message_ids,
        )
        if decoded.message_id != expected_message_id:
            raise ValueError("InputBatcher batch identity is inconsistent")
        if len(decoded.serialize()) != batch_size:
            raise ValueError("InputBatcher batch size is inconsistent")

        expected_envelope_size = len(
            ArticleBatchMessage(
                articles=[],
                execution_id=execution_id,
                reply_queue=decoded.reply_queue,
                message_id=self._build_message_id(execution_id, []),
            ).serialize()
        )
        if envelope_size != expected_envelope_size:
            raise ValueError("InputBatcher envelope size is inconsistent")

        self.current_batch[execution_id] = decoded
        self._batch_sizes[execution_id] = batch_size
        self._envelope_sizes[execution_id] = envelope_size
        self._source_message_ids[execution_id] = list(source_message_ids)

    def _build_batch(
        self,
        message: RoutedArticleMessage,
        articles: list[dict],
        source_message_ids: list[str],
    ) -> ArticleBatchMessage:
        return ArticleBatchMessage(
            articles=articles,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
            message_id=self._build_message_id(
                message.execution_id,
                source_message_ids,
            ),
        )

    def _build_message_id(
        self,
        execution_id: str,
        source_message_ids: list[str],
    ) -> str:
        return build_message_id(
            execution_id=execution_id,
            producer_id=f"input_batcher:{self.partition_id}",
            operation="article_batch",
            identity={"source_message_ids": source_message_ids},
        )
