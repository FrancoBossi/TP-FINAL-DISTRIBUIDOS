from common.message_protocol.message import (
    Message,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from input_batcher.batcher import BatchAccumulator
from input_batcher.messaging import Ack, InputBatcherMessaging, Nack


class InputBatcherController:
    def __init__(
        self,
        batcher: BatchAccumulator,
        messaging: InputBatcherMessaging,
    ) -> None:
        self._batcher = batcher
        self._messaging = messaging

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case RoutedArticleMessage():
                    batch = self._batcher.add_article(message)

                    if batch is not None:
                        self._messaging.publish(batch)

                case RoutedInputEndMessage():
                    batch = self._batcher.flush(message)

                    if batch is not None:
                        self._messaging.publish(batch)

                    self._messaging.publish(message)

                case _:
                    raise ValueError(
                        "InputBatcher expected ROUTED_ARTICLE or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def _publish_remaining_batches(self) -> None:
        for batch in list(self._batcher.current_batch.values()):
            self._messaging.publish(batch)
            self._batcher.flush(
                RoutedInputEndMessage(
                    execution_id=batch.execution_id,
                    reply_queue=batch.reply_queue,
                )
            )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)
        # These articles were already acknowledged. Publish their partial
        # batches without manufacturing an end-of-input for the execution.
        self._publish_remaining_batches()

    def stop(self) -> None:
        self._messaging.stop_consuming()
