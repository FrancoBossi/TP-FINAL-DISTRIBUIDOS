from common.message_identity import build_result_message_ids
from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    ResultMessage,
)


def build_query1_results(
    message: Q1CandidateBatchMessage,
) -> list[ResultMessage]:
    results = []
    for article in message.articles:
        if article["language"] != "en":
            continue
        result_id, message_id = build_result_message_ids(
            execution_id=message.execution_id,
            query_id=1,
            result_kind="result",
            identity={"url": article["url"]},
            producer_id="q1",
        )
        results.append(
            ResultMessage(
                query_id=1,
                record={
                    "url": article["url"],
                    "modification_date": article["modification_date"],
                },
                execution_id=message.execution_id,
                message_id=message_id,
                result_id=result_id,
            )
        )
    return results
