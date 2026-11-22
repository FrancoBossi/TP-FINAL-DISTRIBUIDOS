from collections.abc import Callable
from threading import Lock

from common.message_protocol.message import ExecutionStartedMessage, Message
from common.middleware.middleware import MessageMiddlewareQueue

Acknowledgement = Callable[[], None]
ResultCallback = Callable[[Message, Acknowledgement, Acknowledgement], None]


class GatewayMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        result_queue: MessageMiddlewareQueue,
        result_queue_name: str,
        lifecycle_queue: MessageMiddlewareQueue | None = None,
    ) -> None:
        self._lifecycle_lock = Lock()
        self._closed = False
        self._input_queue = input_queue
        self._lifecycle_queue = lifecycle_queue
        self._result_queue = result_queue
        self.result_queue_name = result_queue_name

    def publish(self, message: Message) -> None:
        self._input_queue.send(message.serialize())

    def publish_started(self, message: ExecutionStartedMessage) -> None:
        if self._lifecycle_queue is None:
            raise RuntimeError("execution registration requires a lifecycle queue")
        self._lifecycle_queue.send(message.serialize())

    def publish_terminal(self, message: Message) -> None:
        if self._lifecycle_queue is not None:
            self._lifecycle_queue.send(message.serialize())

    def start_consuming_results(
        self, on_result: ResultCallback, on_idle: Callable[[], None] | None = None
    ) -> None:
        def deserialize_result(
            body: bytes,
            ack: Acknowledgement,
            nack: Acknowledgement,
        ) -> None:
            message = Message.deserialize(body)
            on_result(message, ack, nack)

        self._result_queue.start_consuming(deserialize_result, on_idle=on_idle)

    def stop_consuming_results(self) -> None:
        with self._lifecycle_lock:
            if not self._closed:
                self._result_queue.stop_consuming()

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._input_queue.close()
            finally:
                try:
                    self._result_queue.close()
                finally:
                    if self._lifecycle_queue is not None:
                        self._lifecycle_queue.close()
