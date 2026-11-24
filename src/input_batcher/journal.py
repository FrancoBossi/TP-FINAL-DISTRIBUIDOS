"""Keep the uncheckpointed portion of an InputBatcher execution."""

import os
from collections.abc import Iterator
from pathlib import Path

from common.message_protocol.message import Message, RoutedArticleMessage
from common.persistence import PersistenceCorruptionError


class ArticleJournal:
    def __init__(self, directory: Path, generation: int) -> None:
        self.path = directory / f"pending-{generation}.jsonl"
        self.message_ids: set[str] = set()

    def append(self, message: RoutedArticleMessage) -> None:
        record = message.serialize() + b"\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("ab", buffering=0) as journal:
            offset = journal.tell()
            try:
                if journal.write(record) != len(record):
                    raise OSError("incomplete article journal write")
                os.fsync(journal.fileno())
                if offset == 0:
                    descriptor = os.open(self.path.parent, os.O_RDONLY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
            except OSError:
                journal.truncate(offset)
                os.fsync(journal.fileno())
                raise
        self.message_ids.add(message.message_id)

    def recover(self, execution_id: str) -> Iterator[RoutedArticleMessage]:
        if not self.path.exists():
            return
        with self.path.open("r+b") as journal:
            while record := journal.readline():
                if not record.endswith(b"\n"):
                    journal.truncate(journal.tell() - len(record))
                    break
                try:
                    message = Message.deserialize(record)
                except (ValueError, KeyError) as error:
                    raise PersistenceCorruptionError(
                        "Invalid article journal record"
                    ) from error
                if (
                    not isinstance(message, RoutedArticleMessage)
                    or message.execution_id != execution_id
                    or message.message_id in self.message_ids
                ):
                    raise PersistenceCorruptionError("Invalid article journal entry")
                self.message_ids.add(message.message_id)
                yield message
