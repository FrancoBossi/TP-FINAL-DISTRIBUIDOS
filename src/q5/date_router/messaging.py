from collections.abc import Callable

from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareQueue

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[
    [Message, Ack, Nack],
    None,
]


class Q5DateRouterMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        historical_output_queue: MessageMiddlewareQueue,
        candidate_output_queue: MessageMiddlewareQueue,
    ) -> None:
        self._input_queue = input_queue
        self._historical_output_queue = historical_output_queue
        self._candidate_output_queue = candidate_output_queue

    def publish_historical(self, message: Message) -> None:
        self._historical_output_queue.send(message.serialize())

    def publish_candidates(self, message: Message) -> None:
        self._candidate_output_queue.send(message.serialize())

    def start_consuming(self, on_message: MessageCallback) -> None:
        def deserialize_message(
            body: bytes,
            ack: Ack,
            nack: Nack,
        ) -> None:
            message = Message.deserialize(body)
            on_message(message, ack, nack)

        self._input_queue.start_consuming(deserialize_message)

    def stop_consuming(self) -> None:
        self._input_queue.stop_consuming()

    def close(self) -> None:
        try:
            self._input_queue.close()
        finally:
            try:
                self._historical_output_queue.close()
            finally:
                self._candidate_output_queue.close()
