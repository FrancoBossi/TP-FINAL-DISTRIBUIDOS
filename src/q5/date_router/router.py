from datetime import datetime

from common.message_identity import derive_message_id
from common.message_protocol.message import (
    Q5ArticleWordCountBatchMessage,
    Q5CandidateBatchMessage,
    Q5HistoricalWordCountBatchMessage,
)


def route_batch(
    message: Q5ArticleWordCountBatchMessage,
) -> tuple[
    Q5HistoricalWordCountBatchMessage,
    Q5CandidateBatchMessage,
]:
    historical_word_counts = []
    candidate_articles = []

    for article in message.articles:
        modification_year = datetime.fromisoformat(article["modification_date"]).year

        if modification_year <= 2020:
            historical_word_counts.append(article["word_count"])
        else:
            candidate_articles.append(
                {
                    "article_id": article["article_id"],
                    "url": article["url"],
                    "word_count": article["word_count"],
                }
            )

    historical_batch = Q5HistoricalWordCountBatchMessage(
        word_counts=historical_word_counts,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
        message_id=derive_message_id(
            execution_id=message.execution_id,
            input_message_id=message.message_id,
            producer_id="q5_date_router",
            operation="historical_word_count_batch",
        ),
    )
    candidate_batch = Q5CandidateBatchMessage(
        articles=candidate_articles,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
        message_id=derive_message_id(
            execution_id=message.execution_id,
            input_message_id=message.message_id,
            producer_id="q5_date_router",
            operation="candidate_batch",
        ),
    )

    return (
        historical_batch,
        candidate_batch,
    )
