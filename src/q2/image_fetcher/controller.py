from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    Message,
    QueryEndMessage,
    RoutedInputEndMessage,
)
from q2.image_fetcher.messaging import (
    Ack,
    ImageFetcherMessaging,
    Nack,
)
from q2.image_fetcher.processor import build_query2_results


class ImageFetcherController:
    def __init__(
        self,
        messaging: ImageFetcherMessaging,
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
                case ImageCandidateBatchMessage():
                    results = build_query2_results(message)

                    for result in results:
                        self._messaging.publish(result)

                case RoutedInputEndMessage():
                    query_end = QueryEndMessage(
                        query_id=2,
                        execution_id=message.execution_id,
                    )
                    self._messaging.publish(query_end)

                case _:
                    raise ValueError(
                        "ImageFetcher expected "
                        "IMAGE_CANDIDATE_BATCH or ROUTED_INPUT_END"
                    )
        except Exception:
            nack()
            raise

        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
