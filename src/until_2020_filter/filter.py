from datetime import datetime

from common.message_identity import derive_message_id
from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    Q1Q2ArticleBatchMessage,
    Q2CandidateBatchMessage,
)


def filter_batch(
    message: Q1Q2ArticleBatchMessage,
) -> tuple[
    Q1CandidateBatchMessage,
    Q2CandidateBatchMessage,
]:
    articles_until_2020 = [
        article
        for article in message.articles
        if datetime.fromisoformat(article["modification_date"]).year <= 2020
    ]

    q1_articles = [
        {
            "url": article["url"],
            "language": article["language"],
            "modification_date": article["modification_date"],
        }
        for article in articles_until_2020
    ]

    q2_articles = [
        {
            "name": article["name"],
            "url": article["url"],
            "sections": article["sections"],
            "image_url": article["image_url"],
        }
        for article in articles_until_2020
    ]

    q1_batch = Q1CandidateBatchMessage(
        articles=q1_articles,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
        message_id=derive_message_id(
            execution_id=message.execution_id,
            input_message_id=message.message_id,
            producer_id="until_2020_filter",
            operation="q1_candidate_batch",
        ),
    )
    q2_batch = Q2CandidateBatchMessage(
        articles=q2_articles,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
        message_id=derive_message_id(
            execution_id=message.execution_id,
            input_message_id=message.message_id,
            producer_id="until_2020_filter",
            operation="q2_candidate_batch",
        ),
    )

    return (
        q1_batch,
        q2_batch,
    )
