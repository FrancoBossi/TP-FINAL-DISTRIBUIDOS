from common.message_protocol.message import (
    Message,
    Q1Q2ArticleBatchMessage,
    RoutedInputEndMessage,
)
from until_2020_filter.filter import filter_batch
from until_2020_filter.messaging import (
    Ack,
    Nack,
    Until2020FilterMessaging,
)


class Until2020FilterController:
    def __init__(
        self,
        messaging: Until2020FilterMessaging,
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
                case Q1Q2ArticleBatchMessage():
                    q1_batch, q2_batch = filter_batch(message)

                    if q1_batch.articles:
                        self._messaging.publish_q1(q1_batch)

                    if q2_batch.articles:
                        self._messaging.publish_q2(q2_batch)

                case RoutedInputEndMessage():
                    self._messaging.publish_q1(message)
                    self._messaging.publish_q2(message)

                case _:
                    raise ValueError(
                        "Until2020Filter expected "
                        "Q1_Q2_ARTICLE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
