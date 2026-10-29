from collections.abc import Callable

from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareQueue

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[
    [Message, Ack, Nack],
    None,
]


class Q3WordProjectorMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        output_queues: dict[str, MessageMiddlewareQueue],
    ) -> None:
        self._input_queue = input_queue
        self._output_queues = output_queues

    def publish(self, queue: str, message: Message) -> None:
        self._output_queues[queue].send(message.serialize())

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
            for queue in self._output_queues.values():
                queue.close()
