from common.message_protocol.message import (
    Message,
    Q1Q2ArticleBatchMessage,
    RoutedInputEndMessage,
)
from until_2020.filter import filter_batch
from until_2020.messaging import Ack, Nack, Until2020Messaging


class Until2020Controller:
    def __init__(self, messaging: Until2020Messaging) -> None:
        self._messaging = messaging

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            match message:
                case Q1Q2ArticleBatchMessage():
                    filtered_batch = filter_batch(message)
                    self._messaging.publish_q1(filtered_batch)
                    self._messaging.publish_q2(filtered_batch)

                case RoutedInputEndMessage():
                    self._messaging.publish_q1(message)
                    self._messaging.publish_q2(message)

                case _:
                    raise ValueError(
                        "Until2020 expected "
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
