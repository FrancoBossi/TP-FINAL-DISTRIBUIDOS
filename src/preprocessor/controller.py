from common.coordinated_consumer import Ack, Nack
from common.message_protocol.message import (
    ArticleBatchMessage,
    Message,
    PartitionEndMessage,
    RoutedInputEndMessage,
)
from preprocessor.messaging import PreprocessorMessaging
from preprocessor.normalizer import normalize_batch


class PreprocessorController:
    def __init__(
        self,
        messaging: PreprocessorMessaging,
        partition_id: int,
    ) -> None:
        self._messaging = messaging
        self._partition_id = partition_id

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
                    self._messaging.broadcast_control(message)

                case _:
                    raise ValueError(
                        "Preprocessor expected ARTICLE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def process_control_message(
        self,
        message: Message,
    ) -> None:
        match message:
            case RoutedInputEndMessage():
                partition_end = PartitionEndMessage(
                    stage="preprocessor",
                    partition_id=self._partition_id,
                    execution_id=message.execution_id,
                    reply_queue=message.reply_queue,
                )
                self._messaging.publish(partition_end)

            case _:
                raise ValueError(
                    "Preprocessor expected ROUTED_INPUT_END from control exchange"
                )

    def start(self) -> None:
        self._messaging.start_consuming(
            self.process_message,
            self.process_control_message,
        )

    def stop(self) -> None:
        self._messaging.stop_consuming()
