from typing import Optional

from common.message_protocol.message import (
    ArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)


class BatchAccumulator:
    def __init__(self, batch_size: int):

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        self.batch_size = batch_size
        self.current_batch: dict[str, ArticleBatchMessage] = {}

    def add_article(
        self, message: RoutedArticleMessage
    ) -> Optional[ArticleBatchMessage]:

        current = self.current_batch.get(message.execution_id)

        if current is not None and current.reply_queue != message.reply_queue:
            raise ValueError(
                f"Execution {message.execution_id} has multiple reply queues: "
                f"{current.reply_queue} and {message.reply_queue}"
            )

        articles = [message.article]

        if current is not None:
            articles = [*current.articles, message.article]

        candidate = ArticleBatchMessage(
            articles=articles,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
        )

        if len(candidate.serialize()) <= self.batch_size:
            self.current_batch[message.execution_id] = candidate
            return None

        single_article_batch = ArticleBatchMessage(
            articles=[message.article],
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
        )

        if len(single_article_batch.serialize()) > self.batch_size:
            raise ValueError(
                f"Single article exceeds batch size limit of {self.batch_size} bytes"
            )

        self.current_batch[message.execution_id] = single_article_batch
        return current

    def flush(self, message: RoutedInputEndMessage) -> Optional[ArticleBatchMessage]:

        current = self.current_batch.get(message.execution_id)

        if current is None:
            return None

        if current.reply_queue != message.reply_queue:
            raise ValueError(
                f"Execution {message.execution_id} has multiple reply queues: "
                f"{current.reply_queue} and {message.reply_queue}"
            )

        return self.current_batch.pop(message.execution_id)
