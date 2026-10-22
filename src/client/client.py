import json
import logging
import socket
from collections.abc import Iterable
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from typing import TextIO

from common.article import Article
from common.message_protocol.external import (
    QUERY_IDS,
    recv_msg,
    send_msg,
    validate_result,
)
from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
    InputEndMessage,
    Message,
    QueryEndMessage,
    ResultMessage,
)
from dataset.parquet_reader import iter_articles

LOG_BATCH_SIZE = 1000
logger = logging.getLogger(__name__)


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    def _receive(self, sock: socket.socket) -> Message:
        message = recv_msg(sock)
        if isinstance(message, ErrorMessage):
            raise RuntimeError(message.message)
        return message

    def _send_article(self, sock: socket.socket, article: Article) -> None:
        record = asdict(article)
        record["modification_date"] = article.modification_date.isoformat()
        send_msg(sock, ArticleMessage(article=record))
        if not isinstance(self._receive(sock), AckMessage):
            raise ValueError("Expected ACK for ARTICLE")

    def _send_end_of_input(self, sock: socket.socket) -> None:
        send_msg(sock, InputEndMessage())
        if not isinstance(self._receive(sock), AckMessage):
            raise ValueError("Expected ACK for INPUT_END")

    def send_articles(self, sock: socket.socket, articles: Iterable[Article]) -> None:
        sent = 0
        logger.info("Started uploading articles")
        for article in articles:
            self._send_article(sock, article)
            sent += 1
            logger.debug("Sent article %d: %s", sent, article.name)
            if sent % LOG_BATCH_SIZE == 0:
                logger.info("Upload progress: %d articles acknowledged", sent)
        self._send_end_of_input(sock)
        logger.info("Upload complete: %d articles acknowledged", sent)

    def _write_result(
        self, message: ResultMessage, output_file: TextIO, count: int
    ) -> int:
        line = json.dumps(message.record, ensure_ascii=False, allow_nan=False)
        output_file.write(line + "\n")
        count += 1
        if count % LOG_BATCH_SIZE == 0:
            logger.info("Query %d: %d results written", message.query_id, count)
        return count

    def receive_results(self, sock: socket.socket, output_dir: str | Path) -> None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        completed: set[int] = set()
        counts = {query_id: 0 for query_id in QUERY_IDS}
        logger.info("Waiting for all five queries; output: %s", output_dir)
        with ExitStack() as stack:
            files = {
                query_id: stack.enter_context(
                    (output_dir / f"query{query_id}.jsonl").open("w", encoding="utf-8")
                )
                for query_id in sorted(QUERY_IDS)
            }
            while completed != QUERY_IDS:
                message = self._receive(sock)
                query_id = validate_result(message, completed)
                match message:
                    case ResultMessage():
                        counts[query_id] = self._write_result(
                            message, files[query_id], counts[query_id]
                        )
                    case QueryEndMessage():
                        files[query_id].flush()
                        completed.add(query_id)
                        logger.info(
                            "Query %d complete: %d results (%d/5 queries)",
                            query_id,
                            counts[query_id],
                            len(completed),
                        )
                send_msg(sock, AckMessage())
        logger.info(
            "All queries complete: %d results written to %s",
            sum(counts.values()),
            output_dir,
        )

    def run(
        self, input_file: str | Path, output_dir: str | Path = "output/distributed"
    ) -> None:
        logger.info("Connecting to gateway at %s:%d", self.host, self.port)
        with socket.create_connection((self.host, self.port)) as sock:
            logger.info("Connected; reading articles from %s", input_file)
            self.send_articles(sock, iter_articles(input_file))
            self.receive_results(sock, output_dir)
