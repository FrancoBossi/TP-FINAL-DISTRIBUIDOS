from collections.abc import Callable

from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareQueue

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[
    [Message, Ack, Nack],
    None,
]


class WordExtractorMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        q3_output_queue: MessageMiddlewareQueue,
        q4_q5_output_queue: MessageMiddlewareQueue,
    ) -> None:
        self._input_queue = input_queue
        self._q3_output_queue = q3_output_queue
        self._q4_q5_output_queue = q4_q5_output_queue

    def publish_q3(self, message: Message) -> None:
        self._q3_output_queue.send(message.serialize())

    def publish_q4_q5(self, message: Message) -> None:
        self._q4_q5_output_queue.send(message.serialize())

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
                self._q3_output_queue.close()
            finally:
                self._q4_q5_output_queue.close()
