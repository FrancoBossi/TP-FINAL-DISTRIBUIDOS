from common.message_protocol.message import (
    Message,
    RoutedInputEndMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter
from q3.word_projector.messaging import Ack, Nack, Q3WordProjectorMessaging
from q3.word_projector.projector import project_batch


class Q3WordProjectorController:
    def __init__(self, messaging: Q3WordProjectorMessaging, router: PartitionRouter):
        self._messaging = messaging
        self._router = router

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            self._handle_message(message)
        except Exception:
            nack()
            raise
        ack()

    def _handle_message(self, message: Message) -> None:
        match message:
            case TokenizedArticleBatchMessage():
                for queue, batch in project_batch(message, self._router).items():
                    self._messaging.publish(queue, batch)
            case RoutedInputEndMessage():
                for partition in range(self._router.partition_count):
                    queue = f"{self._router.queue_prefix}.{partition}"
                    self._messaging.publish(queue, message)
            case _:
                raise ValueError(
                    "Q3WordProjector expected TOKENIZED_ARTICLE_BATCH or ROUTED_INPUT_END"
                )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
