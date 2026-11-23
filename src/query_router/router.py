from common.message_identity import derive_message_id
from common.message_protocol.message import (
    NormalizedArticleBatchMessage,
    Q1Q2ArticleBatchMessage,
    TextArticleBatchMessage,
)


def route_batch(
    message: NormalizedArticleBatchMessage,
) -> tuple[
    Q1Q2ArticleBatchMessage,
    TextArticleBatchMessage,
]:
    q1_q2_articles = [
        {
            "name": article["name"],
            "url": article["url"],
            "language": article["language"],
            "modification_date": article["modification_date"],
            "sections": article["sections"],
            "image_url": article["image_url"],
        }
        for article in message.articles
    ]

    text_articles = [
        {
            "article_id": article["url"],
            "url": article["url"],
            "language": article["language"],
            "modification_date": article["modification_date"],
            "abstract": article["abstract"],
        }
        for article in message.articles
    ]

    q1_q2_batch = Q1Q2ArticleBatchMessage(
        articles=q1_q2_articles,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
        message_id=derive_message_id(
            execution_id=message.execution_id,
            input_message_id=message.message_id,
            producer_id="query_router",
            operation="q1_q2_article_batch",
        ),
    )
    text_batch = TextArticleBatchMessage(
        articles=text_articles,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
        message_id=derive_message_id(
            execution_id=message.execution_id,
            input_message_id=message.message_id,
            producer_id="query_router",
            operation="text_article_batch",
        ),
    )

    return (
        q1_q2_batch,
        text_batch,
    )
