from common.message_protocol.message import (
    Message,
    Q5ArticleWordCountBatchMessage,
    RoutedInputEndMessage,
)
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
                case Q5ArticleWordCountBatchMessage():
                    historical_batch, candidate_batch = route_batch(message)

                    if historical_batch.word_counts:
                        self._messaging.publish_historical(historical_batch)

                    if candidate_batch.articles:
                        self._messaging.publish_candidates(candidate_batch)

                case RoutedInputEndMessage():
                    self._messaging.publish_historical(message)
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

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
