from collections.abc import Callable

from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareQueue

Acknowledgement = Callable[[], None]
ResultCallback = Callable[[Message, Acknowledgement, Acknowledgement], None]


class GatewayMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        result_queue: MessageMiddlewareQueue,
        result_queue_name: str,
    ) -> None:
        self._input_queue = input_queue
        self._result_queue = result_queue
        self.result_queue_name = result_queue_name

    def publish(self, message: Message) -> None:
        self._input_queue.send(message.serialize())

    def start_consuming_results(self, on_result: ResultCallback) -> None:
        def deserialize_result(
            body: bytes,
            ack: Acknowledgement,
            nack: Acknowledgement,
        ) -> None:
            message = Message.deserialize(body)
            on_result(message, ack, nack)

        self._result_queue.start_consuming(deserialize_result)

    def stop_consuming_results(self) -> None:
        self._result_queue.stop_consuming()

    def close(self) -> None:
        try:
            self._input_queue.close()
        finally:
            self._result_queue.close()
