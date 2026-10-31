from collections.abc import Callable

from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareQueue

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[[Message, Ack, Nack], None]


class Q4WordCounterMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        output_queue: MessageMiddlewareQueue,
    ) -> None:
        self._input_queue = input_queue
        self._output_queue = output_queue

    def publish(self, message: Message) -> None:
        self._output_queue.send(message.serialize())

    def start_consuming(self, on_message: MessageCallback) -> None:
        def deserialize(body: bytes, ack: Ack, nack: Nack) -> None:
            on_message(Message.deserialize(body), ack, nack)

        self._input_queue.start_consuming(deserialize)

    def stop_consuming(self) -> None:
        self._input_queue.stop_consuming()

    def close(self) -> None:
        try:
            self._input_queue.close()
        finally:
            self._output_queue.close()
