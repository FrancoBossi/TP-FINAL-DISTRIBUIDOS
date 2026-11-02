from collections.abc import Callable

from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareQueue
from common.partition_router import PartitionRouter

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[
    [Message, Ack, Nack],
    None,
]


class Q5StatsWorkerMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        output_queue: MessageMiddlewareQueue,
        output_queues: dict[str, MessageMiddlewareQueue] | None = None,
        router: PartitionRouter | None = None,
    ) -> None:
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._output_queues = output_queues or {}
        self._router = router

    def publish(self, message: Message) -> None:
        queue = self._output_queue
        if self._router is not None:
            queue = self._output_queues[self._router.queue_for(message.execution_id)]
        queue.send(message.serialize())

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
                if queue is not self._output_queue:
                    queue.close()
            self._output_queue.close()
