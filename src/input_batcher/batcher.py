import json
import logging
from typing import Optional

from common.message_protocol.message import (
    ArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)

logger = logging.getLogger(__name__)

SEPARATOR_LENGTH = 2  # ", " in JSON encoding


class BatchAccumulator:
    def __init__(self, batch_size: int):

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        self.batch_size = batch_size
        self.current_batch: dict[str, ArticleBatchMessage] = {}
        self._batch_sizes: dict[str, int] = {}
        self._envelope_sizes: dict[str, int] = {}

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
                current.articles.append(message.article)
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
        self.current_batch[message.execution_id] = ArticleBatchMessage(
            articles=[message.article],
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
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
        return self.current_batch.pop(message.execution_id)
