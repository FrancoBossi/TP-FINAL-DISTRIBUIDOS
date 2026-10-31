from common.message_protocol.message import (
    Q4WordBatchMessage,
    Q5ArticleWordCountBatchMessage,
    TokenizedArticleBatchMessage,
)
from common.stop_words import STOP_WORDS


def project_batch(
    message: TokenizedArticleBatchMessage,
) -> tuple[Q4WordBatchMessage, Q5ArticleWordCountBatchMessage]:
    q4_words = []
    q5_articles = []

    for article in message.articles:
        if not article["has_abstract"]:
            continue

        unique_words = sorted(set(article["words"]) - STOP_WORDS)

        q4_words.extend(unique_words)
        q5_articles.append(
            {
                "article_id": article["article_id"],
                "url": article["url"],
                "modification_date": article["modification_date"],
                "word_count": len(unique_words),
            }
        )

    return (
        Q4WordBatchMessage(
            words=q4_words,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
        ),
        Q5ArticleWordCountBatchMessage(
            articles=q5_articles,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
        ),
    )
