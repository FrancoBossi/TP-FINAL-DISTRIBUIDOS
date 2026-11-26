import logging
from dataclasses import dataclass
from pathlib import Path

from common.execution_cleanup import ExecutionCleanup
from common.message_protocol.message import (
    ArticleBatchMessage,
    ExecutionCleanupMessage,
    Message,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from input_batcher.batcher import BatchAccumulator
from input_batcher.journal import ArticleJournal
from input_batcher.messaging import Ack, InputBatcherMessaging, Nack

logger = logging.getLogger(__name__)


@dataclass
class _Execution:
    checkpoint: DurableExecutionStore
    generation: int
    journal: ArticleJournal


class InputBatcherController:
    def __init__(
        self,
        batcher: BatchAccumulator,
        messaging: InputBatcherMessaging,
        persistence_layout: PersistenceLayout,
        output_destination: str,
    ) -> None:
        if not output_destination:
            raise ValueError("output_destination must be nonempty")
        self._batcher = batcher
        self._messaging = messaging
        self._persistence_layout = persistence_layout
        self._output_destination = output_destination
        self._cleanup = ExecutionCleanup(persistence_layout, retention_seconds=3600)
        self._executions: dict[str, _Execution] = {}

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        if not isinstance(
            message,
            (RoutedArticleMessage, RoutedInputEndMessage, ExecutionCleanupMessage),
        ):
            nack()
            raise ValueError("InputBatcher expected ROUTED_ARTICLE or ROUTED_INPUT_END")

        try:
            if isinstance(message, ExecutionCleanupMessage):
                self._cleanup.cleanup(message.execution_id, self._cleanup_execution)
                ack()
                return
            if self._cleanup.is_closed(message.execution_id):
                ack()
                return
            execution = self._execution_for(message.execution_id)
            if (
                message.message_id not in execution.journal.message_ids
                and not execution.checkpoint.has_applied(message.message_id)
            ):
                self._apply(message, execution)
            self._publish_pending(execution.checkpoint)
        except Exception:
            nack()
            raise
        ack()

    def _apply(
        self,
        message: RoutedArticleMessage | RoutedInputEndMessage,
        execution: _Execution,
    ) -> None:
        execution_id = message.execution_id
        try:
            if isinstance(message, RoutedArticleMessage):
                batch = self._batcher.add_article(message)
                if batch is None:
                    execution.journal.append(message)
                    return
                outputs: list[Message] = [batch]
            else:
                batch = self._batcher.flush(message)
                outputs = ([batch] if batch is not None else []) + [message]

            generation = execution.generation + 1
            checkpoint_state = self._batcher.export_state(execution_id)
            checkpoint_state["journal_generation"] = generation
            execution.checkpoint.apply(
                message.message_id,
                checkpoint_state,
                [
                    PendingOutput.from_message(self._output_destination, output)
                    for output in outputs
                ],
            )
        except Exception:
            # Reload on the next delivery. A failed write may have reached disk.
            self._clear_execution(execution_id)
            raise

        previous = execution.journal.path
        execution.generation = generation
        execution.journal = ArticleJournal(previous.parent, generation)
        self._remove_obsolete_journal(previous)

    def _execution_for(self, execution_id: str) -> _Execution:
        execution = self._executions.get(execution_id)
        if execution is not None:
            return execution

        path = self._persistence_layout.state_path(execution_id)
        checkpoint = DurableExecutionStore(AtomicJsonStore(path))
        state = checkpoint.state
        generation = state.pop("journal_generation", 0)
        if type(generation) is not int or generation < 0:
            raise PersistenceCorruptionError("Invalid article journal generation")
        journal = ArticleJournal(path.parent, generation)
        self._batcher.restore_state(execution_id, state)
        for message in journal.recover(execution_id):
            self._batcher.add_article(message, replay=True)
        for obsolete in path.parent.glob("pending-*.jsonl"):
            if obsolete != journal.path:
                self._remove_obsolete_journal(obsolete)
        execution = _Execution(checkpoint, generation, journal)
        self._executions[execution_id] = execution
        return execution

    def _log_journal_size(self, path: Path) -> None:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return
        except OSError:
            logger.warning("Could not measure journal %s", path, exc_info=True)
            return
        logger.info(
            "Journal bytes execution=%s partition=%d file=%s bytes=%d",
            path.parent.name,
            self._persistence_layout.partition_id,
            path.name,
            size,
        )

    def _remove_obsolete_journal(self, path: Path) -> None:
        self._log_journal_size(path)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.exception("Could not remove checkpointed journal %s", path)

    def _publish_pending(self, store: DurableExecutionStore) -> None:
        for output in store.pending_outputs:
            if output.destination != self._output_destination:
                raise PersistenceCorruptionError(
                    "InputBatcher outbox contains an unexpected destination"
                )
            message = output.deserialize_message()
            if not isinstance(message, (ArticleBatchMessage, RoutedInputEndMessage)):
                raise PersistenceCorruptionError(
                    "InputBatcher outbox contains an invalid message"
                )
            self._messaging.publish(message)
            store.confirm(output)

    def _cleanup_execution(self, execution_id: str) -> None:
        directory = self._persistence_layout.state_path(execution_id).parent
        for journal in directory.glob("pending-*.jsonl"):
            self._log_journal_size(journal)
        self._clear_execution(execution_id)

    def _clear_execution(self, execution_id: str) -> None:
        self._executions.pop(execution_id, None)
        self._batcher.restore_state(execution_id, {})

    def _recover(self) -> None:
        self._cleanup.recover(self._cleanup_execution)
        executions = set(self._persistence_layout.execution_ids())
        executions.update(
            path.parent.name
            for path in self._persistence_layout.executions_path.glob(
                "*/pending-0.jsonl"
            )
        )
        for execution_id in sorted(executions):
            if self._cleanup.is_closed(execution_id):
                continue
            execution = self._execution_for(execution_id)
            self._publish_pending(execution.checkpoint)

    def start(self) -> None:
        self._recover()
        self._messaging.start_consuming(
            self.process_message,
            **({"on_idle": self._cleanup.sweep} if self._cleanup is not None else {}),
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
