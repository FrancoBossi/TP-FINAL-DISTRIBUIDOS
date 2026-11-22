from common.coordinated_consumer import Ack, Nack
from common.execution_cleanup import ExecutionCleanup
from common.message_identity import build_control_message_id
from common.message_protocol.message import (
    ExecutionCleanupMessage,
    Message,
    NormalizedArticleBatchMessage,
    PartitionEndMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from query_router.messaging import QueryRouterMessaging
from query_router.router import route_batch


class QueryRouterController:
    def __init__(
        self,
        messaging: QueryRouterMessaging,
        partition_id: int,
        preprocessor_partition_count: int,
        persistence_layout: PersistenceLayout,
        q1_q2_destination: str,
        text_destination: str,
    ) -> None:
        if preprocessor_partition_count <= 0:
            raise ValueError("Preprocessor partition count must be positive")
        if not q1_q2_destination or not text_destination:
            raise ValueError("QueryRouter output destinations must be nonempty")
        if q1_q2_destination == text_destination:
            raise ValueError("QueryRouter output destinations must be different")

        self._messaging = messaging
        self._partition_id = partition_id
        self._preprocessor_partition_count = preprocessor_partition_count
        self._persistence_layout = persistence_layout
        self._q1_q2_destination = q1_q2_destination
        self._text_destination = text_destination
        self._finished_preprocessor_partitions: dict[str, set[int]] = {}
        self._cleanup = ExecutionCleanup(persistence_layout, retention_seconds=3600)
        self._stores: dict[str, DurableExecutionStore] = {}

    def _validate_preprocessor_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "preprocessor":
            raise ValueError(
                f"QueryRouter expected preprocessor partition end, got {message.stage}"
            )

        if not 0 <= message.partition_id < self._preprocessor_partition_count:
            raise ValueError(
                f"Invalid preprocessor partition id: {message.partition_id}"
            )

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            if isinstance(
                message, (NormalizedArticleBatchMessage, PartitionEndMessage)
            ) and self._cleanup.is_closed(message.execution_id):
                ack()
                return
            match message:
                case NormalizedArticleBatchMessage():
                    q1_q2_batch, text_batch = route_batch(message)

                    self._messaging.publish_q1_q2(q1_q2_batch)
                    self._messaging.publish_text(text_batch)

                case PartitionEndMessage():
                    self._validate_preprocessor_partition_end(message)
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "QueryRouter expected NORMALIZED_ARTICLE_BATCH or PARTITION_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def process_control_message(
        self,
        message: Message,
    ) -> None:
        if isinstance(message, ExecutionCleanupMessage):
            self._cleanup.cleanup(message.execution_id, self._clear_execution)
            return
        if isinstance(message, PartitionEndMessage) and self._cleanup.is_closed(
            message.execution_id
        ):
            return
        match message:
            case PartitionEndMessage():
                self._validate_preprocessor_partition_end(message)
                self._apply_control(message)

            case _:
                raise ValueError(
                    "QueryRouter expected PARTITION_END from control exchange"
                )

    def _apply_control(self, message: PartitionEndMessage) -> None:
        store = self._store_for(message.execution_id)
        finished, reply_queue, completed = self._decode_state(store.state)
        if store.has_applied(message.message_id):
            self._restore_memory(message.execution_id, finished, completed)
            self._publish_pending(store)
            return
        if completed:
            raise PersistenceCorruptionError(
                "QueryRouter completed barrier is missing a control inbox entry"
            )
        if reply_queue is not None and reply_queue != message.reply_queue:
            raise ValueError("QueryRouter control messages disagree on reply_queue")

        finished.add(message.partition_id)
        completed = len(finished) == self._preprocessor_partition_count
        state = {
            "finished_partitions": sorted(finished),
            "reply_queue": message.reply_queue,
            "completed": completed,
        }
        outputs = []
        if completed:
            partition_end = PartitionEndMessage(
                stage="query_router",
                partition_id=self._partition_id,
                execution_id=message.execution_id,
                reply_queue=message.reply_queue,
                message_id=build_control_message_id(
                    execution_id=message.execution_id,
                    stage="query_router",
                    partition_id=self._partition_id,
                ),
            )
            outputs = [
                PendingOutput.from_message(self._q1_q2_destination, partition_end),
                PendingOutput.from_message(self._text_destination, partition_end),
            ]

        store.apply(message.message_id, state, outputs)
        self._restore_memory(message.execution_id, finished, completed)
        self._publish_pending(store)

    def _store_for(self, execution_id: str) -> DurableExecutionStore:
        store = self._stores.get(execution_id)
        if store is None:
            store = DurableExecutionStore(
                AtomicJsonStore(self._persistence_layout.state_path(execution_id))
            )
            self._stores[execution_id] = store
        return store

    def _decode_state(
        self,
        state: dict,
    ) -> tuple[set[int], str | None, bool]:
        if not state:
            return set(), None, False
        if set(state) != {"finished_partitions", "reply_queue", "completed"}:
            raise PersistenceCorruptionError("invalid QueryRouter barrier state")
        finished = state["finished_partitions"]
        reply_queue = state["reply_queue"]
        completed = state["completed"]
        if (
            not isinstance(finished, list)
            or any(type(partition) is not int for partition in finished)
            or finished != sorted(set(finished))
            or any(
                not 0 <= partition < self._preprocessor_partition_count
                for partition in finished
            )
            or not isinstance(reply_queue, str)
            or not reply_queue
            or type(completed) is not bool
            or completed != (len(finished) == self._preprocessor_partition_count)
        ):
            raise PersistenceCorruptionError("invalid QueryRouter barrier state")
        return set(finished), reply_queue, completed

    def _restore_memory(
        self,
        execution_id: str,
        finished: set[int],
        completed: bool,
    ) -> None:
        if completed:
            self._finished_preprocessor_partitions.pop(execution_id, None)
        else:
            self._finished_preprocessor_partitions[execution_id] = set(finished)

    def _publish_pending(self, store: DurableExecutionStore) -> None:
        for output in store.pending_outputs:
            message = output.deserialize_message()
            if not isinstance(message, PartitionEndMessage):
                raise PersistenceCorruptionError(
                    "QueryRouter control outbox contains an invalid message"
                )
            if output.destination == self._q1_q2_destination:
                self._messaging.publish_q1_q2(message)
            elif output.destination == self._text_destination:
                self._messaging.publish_text(message)
            else:
                raise PersistenceCorruptionError(
                    "QueryRouter control outbox contains an unexpected destination"
                )
            store.confirm(output)

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)
        self._finished_preprocessor_partitions.pop(execution_id, None)

    def _recover(self) -> None:
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._persistence_layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            finished, _reply_queue, completed = self._decode_state(store.state)
            self._restore_memory(execution_id, finished, completed)
            self._publish_pending(store)

    def start(self) -> None:
        self._recover()
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
            **({"on_idle": self._cleanup.sweep} if self._cleanup is not None else {}),
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
