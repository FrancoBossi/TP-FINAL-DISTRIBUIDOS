from dataclasses import dataclass

from common.message_protocol.message import (
    Message,
    PartitionEndMessage,
    Q3WordPresenceBatchMessage,
    QueryEndMessage,
    QueryPartitionEndMessage,
    ResultMessage,
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

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

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
                    self._messaging.publish(
                        QueryEndMessage(query_id=3, execution_id=message.execution_id)
                    )
                else:
                    self._messaging.publish(
                        QueryPartitionEndMessage(
                            query_id=3,
                            partition_id=self._partition_id,
                            execution_id=message.execution_id,
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
            self._messaging.publish(
                ResultMessage(
                    query_id=3,
                    record={"word": word},
                    execution_id=execution_id,
                )
            )
            state.emitted = True

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
