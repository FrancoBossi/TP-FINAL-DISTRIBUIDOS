from common.message_protocol.message import (
    Message,
    Q2CandidateBatchMessage,
    RoutedInputEndMessage,
)
from q2.biography_filter.filter import filter_biography_batch
from q2.biography_filter.messaging import (
    Ack,
    Nack,
    Q2BiographyFilterMessaging,
)


class Q2BiographyFilterController:
    def __init__(
        self,
        messaging: Q2BiographyFilterMessaging,
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
                case Q2CandidateBatchMessage():
                    image_candidates = filter_biography_batch(message)

                    if image_candidates.articles:
                        self._messaging.publish(image_candidates)

                case RoutedInputEndMessage():
                    self._messaging.publish(message)

                case _:
                    raise ValueError(
                        "Q2BiographyFilter expected "
                        "Q2_CANDIDATE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
