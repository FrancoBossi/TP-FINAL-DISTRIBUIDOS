from common.message_protocol.message import (
    TextArticleBatchMessage,
    TokenizedArticleBatchMessage,
)
from common.text import extract_words


def tokenize_batch(message: TextArticleBatchMessage) -> TokenizedArticleBatchMessage:
    return TokenizedArticleBatchMessage(
        articles=[
            {
                "article_id": article["article_id"],
                "url": article["url"],
                "language": article["language"],
                "modification_date": article["modification_date"],
                "words": list(extract_words(article["abstract"])),
            }
            for article in message.articles
        ],
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
    )
