from common.message_protocol.message import (
    ArticleBatchMessage,
    Message,
    RoutedInputEndMessage,
)
from preprocessor.messaging import Ack, Nack, PreprocessorMessaging
from preprocessor.normalizer import normalize_batch


class PreprocessorController:
    def __init__(
        self,
        messaging: PreprocessorMessaging,
    ) -> None:
        self._messaging = messaging

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case ArticleBatchMessage():
                    normalized_batch = normalize_batch(message)
                    self._messaging.publish(normalized_batch)

                case RoutedInputEndMessage():
                    self._messaging.publish(message)

                case _:
                    raise ValueError(
                        "Preprocessor expected ARTICLE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
