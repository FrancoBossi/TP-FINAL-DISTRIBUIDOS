from common.message_protocol.message import (
    Message,
    Q4WordBatchMessage,
    RoutedInputEndMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter
from q4_q5.word_projector.messaging import (
    Ack,
    Nack,
    Q4Q5WordProjectorMessaging,
)
from q4_q5.word_projector.projector import project_batch


class Q4Q5WordProjectorController:
    def __init__(
        self,
        messaging: Q4Q5WordProjectorMessaging,
        router: PartitionRouter | None = None,
    ) -> None:
        self._messaging = messaging
        self._router = router

    def _handle_message(self, message: Message) -> None:
        match message:
            case TokenizedArticleBatchMessage():
                q4_batch, q5_batch = project_batch(message)

                if q4_batch.words:
                    if self._router is None:
                        self._messaging.publish_q4(q4_batch)
                    else:
                        by_queue: dict[str, list[str]] = {}
                        for word in q4_batch.words:
                            queue = self._router.queue_for(
                                f"{q4_batch.execution_id}:{word}"
                            )
                            by_queue.setdefault(queue, []).append(word)
                        for queue, words in by_queue.items():
                            self._messaging.publish_q4_to(
                                queue,
                                Q4WordBatchMessage(
                                    words=words,
                                    execution_id=q4_batch.execution_id,
                                    reply_queue=q4_batch.reply_queue,
                                ),
                            )

                if q5_batch.articles:
                    self._messaging.publish_q5(q5_batch)

            case RoutedInputEndMessage():
                if self._router is None:
                    self._messaging.publish_q4(message)
                else:
                    for partition in range(self._router.partition_count):
                        self._messaging.publish_q4_to(
                            f"{self._router.queue_prefix}.{partition}",
                            message,
                        )
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
