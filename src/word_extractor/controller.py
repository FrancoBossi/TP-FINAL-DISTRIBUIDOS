from common.message_protocol.message import (
    Message,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
)
from word_extractor.extractor import tokenize_batch
from word_extractor.messaging import Ack, Nack, WordExtractorMessaging


class WordExtractorController:
    def __init__(self, messaging: WordExtractorMessaging) -> None:
        self._messaging = messaging

    def _handle_message(self, message: Message) -> None:
        match message:
            case TextArticleBatchMessage():
                batch = tokenize_batch(message)
                if batch.articles:
                    self._messaging.publish_q3(batch)
                    self._messaging.publish_q4_q5(batch)
            case RoutedInputEndMessage():
                self._messaging.publish_q3(message)
                self._messaging.publish_q4_q5(message)
            case _:
                raise ValueError(
                    "WordExtractor expected TEXT_ARTICLE_BATCH or ROUTED_INPUT_END"
                )

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
