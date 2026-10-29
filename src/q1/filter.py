from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    ResultMessage,
)


def build_query1_results(
    message: Q1CandidateBatchMessage,
) -> list[ResultMessage]:
    return [
        ResultMessage(
            query_id=1,
            record={
                "url": article["url"],
                "modification_date": article["modification_date"],
            },
            execution_id=message.execution_id,
        )
        for article in message.articles
        if article["language"] == "en"
    ]
