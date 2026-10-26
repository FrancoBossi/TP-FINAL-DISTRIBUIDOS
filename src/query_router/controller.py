from common.message_protocol.message import (
    Message,
    NormalizedArticleBatchMessage,
    RoutedInputEndMessage,
)
from query_router.messaging import (
    Ack,
    Nack,
    QueryRouterMessaging,
)
from query_router.router import route_batch


class QueryRouterController:
    def __init__(
        self,
        messaging: QueryRouterMessaging,
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
                case NormalizedArticleBatchMessage():
                    q1_q2_batch, text_batch = route_batch(message)

                    self._messaging.publish_q1_q2(q1_q2_batch)
                    self._messaging.publish_text(text_batch)

                case RoutedInputEndMessage():
                    self._messaging.publish_q1_q2(message)
                    self._messaging.publish_text(message)

                case _:
                    raise ValueError(
                        "QueryRouter expected "
                        "NORMALIZED_ARTICLE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
