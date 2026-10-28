from datetime import datetime

from common.message_protocol.message import Q1Q2ArticleBatchMessage

#filtramos los articulos con la fecha hasta 2020
def is_until_2020(article: dict[str, object]) -> bool:
    value = article.get("modification_date")
    if not isinstance(value, str):
        raise ValueError("modification_date must be an ISO date string")

    try:
        modification_date = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("modification_date must be an ISO date string") from error

    return modification_date.year <= 2020

#recive un batch completo de articulos
def filter_batch(
    message: Q1Q2ArticleBatchMessage,
) -> Q1Q2ArticleBatchMessage:
    return Q1Q2ArticleBatchMessage(
        articles=[article for article in message.articles if is_until_2020(article)],
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
    )
