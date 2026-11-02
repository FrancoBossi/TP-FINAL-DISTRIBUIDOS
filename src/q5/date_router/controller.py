from common.message_protocol.message import (
    Message,
    Q5ArticleWordCountBatchMessage,
    RoutedInputEndMessage,
)
from common.partition_router import PartitionRouter
from q5.date_router.messaging import (
    Ack,
    Nack,
    Q5DateRouterMessaging,
)
from q5.date_router.router import route_batch


class Q5DateRouterController:
    def __init__(
        self,
        messaging: Q5DateRouterMessaging,
        historical_router: PartitionRouter | None = None,
    ) -> None:
        self._messaging = messaging
        self._historical_router = historical_router

    def process_message(
        self,
        message: Message,
        ack: Ack,
        nack: Nack,
    ) -> None:
        try:
            match message:
                case Q5ArticleWordCountBatchMessage():
                    historical_batch, candidate_batch = route_batch(message)

                    if historical_batch.word_counts:
                        self._publish_historical(historical_batch)

                    if candidate_batch.articles:
                        self._messaging.publish_candidates(candidate_batch)

                case RoutedInputEndMessage():
                    if self._historical_router is None:
                        self._messaging.publish_historical(message)
                    else:
                        for partition in range(self._historical_router.partition_count):
                            self._messaging.publish_historical_to(
                                f"{self._historical_router.queue_prefix}.{partition}",
                                message,
                            )
                    self._messaging.publish_candidates(message)

                case _:
                    raise ValueError(
                        "Q5DateRouter expected "
                        "Q5_ARTICLE_WORD_COUNT_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def _publish_historical(self, message: Message) -> None:
        if self._historical_router is None:
            self._messaging.publish_historical(message)
        else:
            self._messaging.publish_historical_to(
                self._historical_router.queue_for(message.execution_id), message
            )

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
