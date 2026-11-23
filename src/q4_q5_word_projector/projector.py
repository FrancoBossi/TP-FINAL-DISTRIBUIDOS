from common.message_identity import derive_message_id
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
            message_id=derive_message_id(
                execution_id=message.execution_id,
                input_message_id=message.message_id,
                producer_id="q4_q5_word_projector",
                operation="q4_word_batch",
            ),
        ),
        Q5ArticleWordCountBatchMessage(
            articles=q5_articles,
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
            message_id=derive_message_id(
                execution_id=message.execution_id,
                input_message_id=message.message_id,
                producer_id="q4_q5_word_projector",
                operation="q5_article_word_count_batch",
            ),
        ),
    )
