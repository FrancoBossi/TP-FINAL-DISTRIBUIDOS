from common.message_protocol.message import (
    Message,
    RoutedInputEndMessage,
    TokenizedArticleBatchMessage,
)
from q4_q5.word_projector.messaging import (
    Ack,
    Nack,
    Q4Q5WordProjectorMessaging,
)
from q4_q5.word_projector.projector import project_batch


class Q4Q5WordProjectorController:
    def __init__(self, messaging: Q4Q5WordProjectorMessaging) -> None:
        self._messaging = messaging

    def _handle_message(self, message: Message) -> None:
        match message:
            case TokenizedArticleBatchMessage():
                q4_batch, q5_batch = project_batch(message)

                if q4_batch.words:
                    self._messaging.publish_q4(q4_batch)

                if q5_batch.articles:
                    self._messaging.publish_q5(q5_batch)

            case RoutedInputEndMessage():
                self._messaging.publish_q4(message)
                self._messaging.publish_q5(message)

            case _:
                raise ValueError(
                    "Q4Q5WordProjector expected "
                    "TOKENIZED_ARTICLE_BATCH or ROUTED_INPUT_END"
                )

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
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
