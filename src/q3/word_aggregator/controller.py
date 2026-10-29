from dataclasses import dataclass

from common.message_protocol.message import (
    Message,
    Q3WordPresenceBatchMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedInputEndMessage,
)
from q3.word_aggregator.messaging import Ack, Nack, Q3WordAggregatorMessaging


@dataclass
class WordPresence:
    seen_en: bool = False
    seen_fr: bool = False
    emitted: bool = False


class Q3WordAggregatorController:
    def __init__(self, messaging: Q3WordAggregatorMessaging) -> None:
        self._messaging = messaging
        self._executions: dict[str, dict[str, WordPresence]] = {}

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
            case RoutedInputEndMessage():
                self._messaging.publish(
                    QueryEndMessage(query_id=3, execution_id=message.execution_id)
                )
                self._executions.pop(message.execution_id, None)
            case _:
                raise ValueError(
                    "Q3WordAggregator expected Q3_WORD_PRESENCE_BATCH or ROUTED_INPUT_END"
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
