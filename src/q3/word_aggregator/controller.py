from dataclasses import dataclass
from typing import Any

from common.execution_cleanup import ExecutionCleanup
from common.message_identity import (
    build_control_message_id,
    build_result_message_ids,
)
from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q3WordPresenceBatchMessage,
    QueryEndMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)
from q3.word_aggregator.messaging import Ack, Nack


@dataclass
class WordPresence:
    seen_en: bool = False
    seen_fr: bool = False
    emitted: bool = False


class Q3WordAggregatorController:
    def __init__(
        self,
        messaging,
        partition_id: int = 0,
        partition_count: int = 1,
        projector_partition_count: int = 1,
        persistence_layout: PersistenceLayout | None = None,
        output_destination: str = "result_sink.input",
    ) -> None:
        if partition_count <= 0 or not 0 <= partition_id < partition_count:
            raise ValueError("invalid Q3 partition configuration")
        if projector_partition_count <= 0:
            raise ValueError("Q3WordProjector partition count must be positive")
        self._messaging = messaging
        self._partition_id = partition_id
        self._partition_count = partition_count
        self._projector_partition_count = projector_partition_count
        self._executions: dict[str, dict[str, WordPresence]] = {}
        self._finished_projector_partitions: dict[str, set[int]] = {}
        self._persistence_layout = persistence_layout
        self._cleanup = (
            ExecutionCleanup(persistence_layout, retention_seconds=3600)
            if persistence_layout is not None
            else None
        )
        self._output_destination = output_destination
        self._stores: dict[str, DurableExecutionStore] = {}

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        if self._cleanup is not None:
            try:
                handled = self._cleanup.handle_message(message, self._clear_execution)
            except Exception:
                nack()
                raise
            if handled:
                ack()
                return
            self._process_durable(message, ack, nack)
            return
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

    def _process_durable(self, message: Message, ack: Ack, nack: Nack) -> None:
        if not isinstance(message, (Q3WordPresenceBatchMessage, PartitionEndMessage)):
            nack()
            raise ValueError(
                "Q3WordAggregator expected Q3_WORD_PRESENCE_BATCH or PARTITION_END"
            )
        if isinstance(message, PartitionEndMessage):
            self._validate_projector_partition_end(message)
        try:
            store = self._store_for(message.execution_id)
            if store.has_applied(message.message_id):
                self._publish_pending(store)
                ack()
                return
            state = self._decode_state(store.state)
            if state["completed"]:
                raise PersistenceCorruptionError(
                    "Q3WordAggregator received new input after completion"
                )
            outputs = self._apply_to_state(message, state)
            store.apply(
                message.message_id,
                state,
                [
                    PendingOutput.from_message(self._output_destination, output)
                    for output in outputs
                ],
            )
            self._publish_pending(store)
        except Exception:
            nack()
            raise
        ack()

    def _apply_to_state(self, message: Message, state: dict[str, Any]) -> list[Message]:
        outputs: list[Message] = []
        if isinstance(message, Q3WordPresenceBatchMessage):
            words = state["words"]
            for presence in message.presences:
                language = presence["language"]
                if language not in ("en", "fr"):
                    continue
                word = presence["word"]
                presence_state = words.setdefault(
                    word, {"seen_en": False, "seen_fr": False, "emitted": False}
                )
                presence_state[f"seen_{language}"] = True
                if (
                    presence_state["seen_en"]
                    and presence_state["seen_fr"]
                    and not presence_state["emitted"]
                ):
                    result_id, message_id = build_result_message_ids(
                        execution_id=message.execution_id,
                        query_id=3,
                        result_kind="result",
                        identity={"word": word},
                        producer_id="q3_word_aggregator",
                    )
                    outputs.append(
                        ResultMessage(
                            query_id=3,
                            record={"word": word},
                            execution_id=message.execution_id,
                            message_id=message_id,
                            result_id=result_id,
                        )
                    )
                    presence_state["emitted"] = True
            return outputs

        state["finished_partitions"] = sorted(
            {*state["finished_partitions"], message.partition_id}
        )
        if len(state["finished_partitions"]) < self._projector_partition_count:
            return outputs
        state["completed"] = True
        if self._partition_count == 1:
            result_id, message_id = build_result_message_ids(
                execution_id=message.execution_id,
                query_id=3,
                result_kind="end",
                identity={"end": True},
                producer_id="q3_word_aggregator",
            )
            outputs.append(
                QueryEndMessage(3, message.execution_id, message_id, result_id)
            )
        else:
            outputs.append(
                QueryPartitionEndMessage(
                    query_id=3,
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    message_id=build_control_message_id(
                        execution_id=message.execution_id,
                        stage="q3_word_aggregator",
                        partition_id=self._partition_id,
                        operation="query_partition_end",
                    ),
                )
            )
        return outputs

    def _decode_state(self, state: dict[str, Any]) -> dict[str, Any]:
        if not state:
            return {"words": {}, "finished_partitions": [], "completed": False}
        if set(state) != {"words", "finished_partitions", "completed"}:
            raise PersistenceCorruptionError("invalid Q3WordAggregator state")
        words = state["words"]
        finished = state["finished_partitions"]
        completed = state["completed"]
        valid_presence = {"seen_en", "seen_fr", "emitted"}
        if (
            not isinstance(words, dict)
            or any(
                not isinstance(word, str)
                or not word
                or not isinstance(value, dict)
                or set(value) != valid_presence
                or any(type(flag) is not bool for flag in value.values())
                for word, value in words.items()
            )
            or not isinstance(finished, list)
            or any(type(item) is not int for item in finished)
            or finished != sorted(set(finished))
            or any(not 0 <= item < self._projector_partition_count for item in finished)
            or type(completed) is not bool
            or completed != (len(finished) == self._projector_partition_count)
        ):
            raise PersistenceCorruptionError("invalid Q3WordAggregator state")
        return state

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
                raise PersistenceCorruptionError("invalid Q3WordAggregator destination")
            message = output.deserialize_message()
            if not isinstance(
                message, (ResultMessage, QueryEndMessage, QueryPartitionEndMessage)
            ):
                raise PersistenceCorruptionError("invalid Q3WordAggregator outbox")
            self._messaging.publish(message)
            store.confirm(output)

    def _clear_execution(self, execution_id: str) -> None:
        self._stores.pop(execution_id, None)
        self._executions.pop(execution_id, None)
        self._finished_projector_partitions.pop(execution_id, None)

    def _recover(self) -> None:
        self._cleanup.recover(self._clear_execution)
        for execution_id in self._persistence_layout.execution_ids():
            if self._cleanup.is_closed(execution_id):
                continue
            store = self._store_for(execution_id)
            self._decode_state(store.state)
            self._publish_pending(store)

    def _handle_message(self, message: Message) -> None:
        match message:
            case Q3WordPresenceBatchMessage():
                for presence in message.presences:
                    self._handle_presence(message.execution_id, presence)
            case PartitionEndMessage():
                self._validate_projector_partition_end(message)
                finished_partitions = self._finished_projector_partitions.setdefault(
                    message.execution_id,
                    set(),
                )
                finished_partitions.add(message.partition_id)

                if len(finished_partitions) < self._projector_partition_count:
                    return

                if self._partition_count == 1:
                    result_id, message_id = build_result_message_ids(
                        execution_id=message.execution_id,
                        query_id=3,
                        result_kind="end",
                        identity={"end": True},
                        producer_id="q3_word_aggregator",
                    )
                    self._messaging.publish(
                        QueryEndMessage(
                            query_id=3,
                            execution_id=message.execution_id,
                            message_id=message_id,
                            result_id=result_id,
                        )
                    )
                else:
                    self._messaging.publish(
                        QueryPartitionEndMessage(
                            query_id=3,
                            partition_id=self._partition_id,
                            execution_id=message.execution_id,
                            message_id=build_control_message_id(
                                execution_id=message.execution_id,
                                stage="q3_word_aggregator",
                                partition_id=self._partition_id,
                                operation="query_partition_end",
                            ),
                        )
                    )
                self._executions.pop(message.execution_id, None)
                self._finished_projector_partitions.pop(message.execution_id)
            case _:
                raise ValueError(
                    "Q3WordAggregator expected Q3_WORD_PRESENCE_BATCH or PARTITION_END"
                )

    def _validate_projector_partition_end(
        self,
        message: PartitionEndMessage,
    ) -> None:
        if message.stage != "q3_word_projector":
            raise ValueError(
                "Q3WordAggregator expected q3_word_projector partition end, "
                f"got {message.stage}"
            )

        if not 0 <= message.partition_id < self._projector_partition_count:
            raise ValueError(
                f"Invalid Q3WordProjector partition id: {message.partition_id}"
            )

    def _handle_presence(self, execution_id: str, presence: dict[str, str]) -> None:
        word = presence["word"]
        language = presence["language"]
        if language not in ("en", "fr"):
            return
        words = self._executions.setdefault(execution_id, {})
        state = words.setdefault(word, WordPresence())
        if language == "en":
            state.seen_en = True
        else:
            state.seen_fr = True
        if state.seen_en and state.seen_fr and not state.emitted:
            result_id, message_id = build_result_message_ids(
                execution_id=execution_id,
                query_id=3,
                result_kind="result",
                identity={"word": word},
                producer_id="q3_word_aggregator",
            )
            self._messaging.publish(
                ResultMessage(
                    query_id=3,
                    record={"word": word},
                    execution_id=execution_id,
                    message_id=message_id,
                    result_id=result_id,
                )
            )
            state.emitted = True

    def start(self) -> None:
        if self._persistence_layout is not None:
            self._recover()
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
