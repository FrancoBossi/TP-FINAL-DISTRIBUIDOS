import json
import logging
import os
import socket
import time
from collections.abc import Iterable
from contextlib import ExitStack
from pathlib import Path
from typing import Any, TextIO

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
from dataset.parquet_reader import iter_article_records

LOG_BATCH_SIZE = 1000
CONNECTION_ATTEMPTS = 10
CONNECTION_RETRY_DELAY = 3
CONNECTION_TIMEOUT = 5
logger = logging.getLogger(__name__)


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    def _try_connect(self, attempt: int) -> socket.socket | None:
        try:
            sock = socket.create_connection(
                (self.host, self.port), timeout=CONNECTION_TIMEOUT
            )
        except OSError as error:
            if attempt == CONNECTION_ATTEMPTS:
                raise
            logger.warning(
                "Gateway connection attempt %d/%d failed: %s; retrying in %ds",
                attempt,
                CONNECTION_ATTEMPTS,
                error,
                CONNECTION_RETRY_DELAY,
            )
            time.sleep(CONNECTION_RETRY_DELAY)
        else:
            try:
                sock.settimeout(None)
            except Exception:
                sock.close()
                raise
            return sock

    def _connect(self) -> socket.socket:
        for attempt in range(1, CONNECTION_ATTEMPTS + 1):
            sock = self._try_connect(attempt)
            if sock is not None:
                return sock

    def _receive(self, sock: socket.socket) -> Message:
        message = recv_msg(sock)
        if isinstance(message, ErrorMessage):
            raise RuntimeError(message.message)
        return message

    def _send_article(
        self,
        sock: socket.socket,
        article: dict[str, Any],
    ) -> None:

        send_msg(sock, ArticleMessage(article=article))

        if not isinstance(self._receive(sock), AckMessage):
            raise ValueError("Expected ACK for ARTICLE")

    def _send_end_of_input(self, sock: socket.socket) -> None:
        send_msg(sock, InputEndMessage())
        if not isinstance(self._receive(sock), AckMessage):
            raise ValueError("Expected ACK for INPUT_END")

    def send_articles(
        self,
        sock: socket.socket,
        articles: Iterable[dict[str, Any]],
    ) -> None:

        sent = 0
        logger.info("Started uploading articles")

        for article in articles:
            self._send_article(sock, article)
            sent += 1
            logger.debug("Sent article %d: %s", sent, article["name"])

            if sent % LOG_BATCH_SIZE == 0:
                logger.info("Upload progress: %d articles acknowledged", sent)

        self._send_end_of_input(sock)
        logger.info("Upload complete: %d articles acknowledged", sent)

    def _write_result(
        self, message: ResultMessage, output_file: TextIO, count: int
    ) -> int:
        line = json.dumps(message.record, ensure_ascii=False, allow_nan=False)
        output_file.write(line + "\n")
        output_file.flush()
        os.fsync(output_file.fileno())
        count += 1
        if count % LOG_BATCH_SIZE == 0:
            logger.info("Query %d: %d results written", message.query_id, count)
        return count

    def receive_results(
        self,
        sock: socket.socket,
        output_dir: str | Path,
        *,
        started_at: float | None = None,
    ) -> None:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        completed: set[int] = set()
        seen_result_ids: set[str] = set()
        counts = {query_id: 0 for query_id in QUERY_IDS}
        logger.info("Waiting for all five queries; output: %s", output_dir)
        with ExitStack() as stack:
            files = {
                query_id: stack.enter_context(
                    (output_dir / f"query{query_id}.jsonl").open("w", encoding="utf-8")
                )
                for query_id in sorted(QUERY_IDS)
            }
            result_id_ledger = stack.enter_context(
                (output_dir / ".result_ids.jsonl").open("w", encoding="utf-8")
            )
            while completed != QUERY_IDS:
                message = self._receive(sock)
                if isinstance(message, (ResultMessage, QueryEndMessage)):
                    if message.result_id in seen_result_ids:
                        send_msg(sock, AckMessage())
                        continue
                query_id = validate_result(message, completed)
                match message:
                    case ResultMessage():
                        counts[query_id] = self._write_result(
                            message, files[query_id], counts[query_id]
                        )
                    case QueryEndMessage():
                        files[query_id].flush()
                        os.fsync(files[query_id].fileno())
                        completed.add(query_id)
                        logger.info(
                            "Query %d complete: %d results (%d/5 queries)",
                            query_id,
                            counts[query_id],
                            len(completed),
                        )
                seen_result_ids.add(message.result_id)
                result_id_ledger.write(message.result_id + "\n")
                result_id_ledger.flush()
                os.fsync(result_id_ledger.fileno())
                if started_at is not None and isinstance(message, ResultMessage):
                    logger.info(
                        "First result persisted after %.6f seconds",
                        time.monotonic() - started_at,
                    )
                    started_at = None
                send_msg(sock, AckMessage())
        logger.info(
            "All queries complete: %d results written to %s",
            sum(counts.values()),
            output_dir,
        )

    def run(
        self, input_file: str | Path, output_dir: str | Path = "output/distributed"
    ) -> None:
        started_at = time.monotonic()
        logger.info("Connecting to gateway at %s:%d", self.host, self.port)
        with self._connect() as sock:
            logger.info("Connected; reading articles from %s", input_file)
            self.send_articles(sock, iter_article_records(input_file))
            self.receive_results(sock, output_dir, started_at=started_at)
