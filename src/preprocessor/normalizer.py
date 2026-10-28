from common.article import (
    article_from_raw_record,
    article_to_normalized_record,
)
from common.message_protocol.message import (
    ArticleBatchMessage,
    NormalizedArticleBatchMessage,
)


def normalize_batch(
    message: ArticleBatchMessage,
) -> NormalizedArticleBatchMessage:
    normalized_articles = [
        article_to_normalized_record(article_from_raw_record(record))
        for record in message.articles
    ]

    return NormalizedArticleBatchMessage(
        articles=normalized_articles,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
    )
