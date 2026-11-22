import logging

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
from input_batcher.messaging import Ack, InputBatcherMessaging, Nack

logger = logging.getLogger(__name__)


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
        self._stores: dict[str, DurableExecutionStore] = {}

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
                self._cleanup.cleanup(message.execution_id, self._clear_execution)
                ack()
                return
            if self._cleanup.is_closed(message.execution_id):
                ack()
                return
            store = self._store_for(message.execution_id)
        except Exception:
            nack()
            raise
        if store.has_applied(message.message_id):
            try:
                self._batcher.restore_state(message.execution_id, store.state)
                self._publish_pending(store)
            except Exception:
                nack()
                raise
            ack()
            self._compact_after_ack(
                store,
                message.execution_id,
                message.message_id,
            )
            return

        previous_state = self._batcher.export_state(message.execution_id)
        try:
            match message:
                case RoutedArticleMessage():
                    batch = self._batcher.add_article(message)
                    outputs: list[Message] = [] if batch is None else [batch]

                case RoutedInputEndMessage():
                    batch = self._batcher.flush(message)
                    outputs = ([batch] if batch is not None else []) + [message]

            pending = [
                PendingOutput.from_message(self._output_destination, output)
                for output in outputs
            ]
            applied = store.apply(
                message.message_id,
                self._batcher.export_state(message.execution_id),
                pending,
            )
            if not applied:
                self._batcher.restore_state(message.execution_id, store.state)
        except Exception:
            self._batcher.restore_state(message.execution_id, previous_state)
            nack()
            raise

        try:
            self._publish_pending(store)
        except Exception:
            nack()
            raise

        ack()
        self._compact_after_ack(
            store,
            message.execution_id,
            message.message_id,
        )

    def _store_for(self, execution_id: str) -> DurableExecutionStore:
        store = self._stores.get(execution_id)
        if store is None:
            store = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
            self._stores[execution_id] = store
        return store

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

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)
        self._batcher.restore_state(execution_id, {})

    def _recover(self) -> None:
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._persistence_layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            self._batcher.restore_state(execution_id, store.state)
            self._publish_pending(store)

    def _compact_after_ack(
        self,
        store: DurableExecutionStore,
        execution_id: str,
        last_message_id: str,
    ) -> None:
        state = self._batcher.export_state(execution_id)
        retained = list(state.get("source_message_ids", []))
        if last_message_id not in retained:
            retained.append(last_message_id)
        try:
            store.compact_inbox(retained)
        except Exception:
            logger.exception(
                "Could not compact durable inbox for execution %s",
                execution_id,
            )

    def start(self) -> None:
        self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
