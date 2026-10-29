import json
from collections.abc import Iterator

from common.message_protocol.message import (
    Q3WordPresenceBatchMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter


def project_batch(
    message: TokenizedArticleBatchMessage,
    router: PartitionRouter,
) -> dict[str, Q3WordPresenceBatchMessage]:
    batches: dict[str, Q3WordPresenceBatchMessage] = {}
    for presence in _iter_presences(message):
        queue = _queue_for_word(router, message.execution_id, presence["word"])
        _append_presence(batches, queue, presence, message)
    return batches


def _iter_presences(message: TokenizedArticleBatchMessage) -> Iterator[dict[str, str]]:
    for article in message.articles:
        language = article["language"]
        if language not in ("en", "fr"):
            continue
        for word in sorted(set(article["words"])):
            yield {"word": word, "language": language}


def _queue_for_word(router: PartitionRouter, execution_id: str, word: str) -> str:
    key = json.dumps([execution_id, word], ensure_ascii=False)
    return router.queue_for(key)


def _append_presence(
    batches: dict[str, Q3WordPresenceBatchMessage],
    queue: str,
    presence: dict[str, str],
    message: TokenizedArticleBatchMessage,
) -> None:
    if queue not in batches:
        batches[queue] = Q3WordPresenceBatchMessage(
            presences=[],
            execution_id=message.execution_id,
            reply_queue=message.reply_queue,
        )
    batches[queue].presences.append(presence)
