from collections.abc import Callable

from common.message_protocol.message import Message, QueryEndMessage, ResultMessage
from common.middleware.middleware import (
    MessageMiddlewarePublisher,
    MessageMiddlewareQueue,
)

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[[Message, Ack, Nack], None]


class ResultSinkMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        publisher: MessageMiddlewarePublisher,
    ) -> None:
        self._input_queue = input_queue
        self._publisher = publisher

    def publish(self, message: ResultMessage | QueryEndMessage) -> None:
        self._publisher.send_to(f"results.{message.execution_id}", message.serialize())

    def start_consuming(self, on_message: MessageCallback) -> None:
        def deserialize_message(body: bytes, ack: Ack, nack: Nack) -> None:
            try:
                message = Message.deserialize(body)
            except Exception:
                nack()
                raise
            on_message(message, ack, nack)

        self._input_queue.start_consuming(deserialize_message)

    def stop_consuming(self) -> None:
        self._input_queue.stop_consuming()

    def close(self) -> None:
        try:
            self._input_queue.close()
        finally:
            self._publisher.close()
