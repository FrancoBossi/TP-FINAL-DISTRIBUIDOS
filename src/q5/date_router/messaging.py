from common.coordinated_consumer import (
    ControlMessageCallback,
    CoordinatedConsumer,
    MessageCallback,
)
from common.message_protocol.message import Message
from common.middleware.middleware import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)


class Q5DateRouterMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        historical_output_queue: MessageMiddlewareQueue,
        candidate_output_queue: MessageMiddlewareQueue,
        control_exchange_input: MessageMiddlewareExchange,
        control_exchange_output: MessageMiddlewareExchange,
        historical_output_queues: dict[str, MessageMiddlewareQueue] | None = None,
        candidate_output_queues: dict[str, MessageMiddlewareQueue] | None = None,
    ) -> None:
        self._consumer = CoordinatedConsumer(input_queue, control_exchange_input)
        self._historical_output_queue = historical_output_queue
        self._historical_output_queues = historical_output_queues or {}
        self._candidate_output_queue = candidate_output_queue
        self._candidate_output_queues = candidate_output_queues or {}
        self._control_exchange_output = control_exchange_output

    def publish_historical(self, message: Message) -> None:
        self._historical_output_queue.send(message.serialize())

    def publish_historical_to(self, queue: str, message: Message) -> None:
        self._historical_output_queues.get(queue, self._historical_output_queue).send(
            message.serialize()
        )

    def publish_candidates(self, message: Message) -> None:
        self._candidate_output_queue.send(message.serialize())

    def publish_candidates_to(self, queue: str, message: Message) -> None:
        self._candidate_output_queues.get(queue, self._candidate_output_queue).send(
            message.serialize()
        )

    def broadcast_control(self, message: Message) -> None:
        self._control_exchange_output.send(message.serialize())

    def start_consuming(
        self,
        on_message: MessageCallback,
        on_control_message: ControlMessageCallback,
    ) -> None:
        self._consumer.start_consuming(on_message, on_control_message)

    def stop_consuming(self) -> None:
        self._consumer.stop_consuming()

    def close(self) -> None:
        try:
            self._consumer.close()
        finally:
            try:
                self._historical_output_queue.close()
            finally:
                try:
                    for queue in self._historical_output_queues.values():
                        if queue is not self._historical_output_queue:
                            queue.close()
                    self._candidate_output_queue.close()
                    for queue in self._candidate_output_queues.values():
                        if queue is not self._candidate_output_queue:
                            queue.close()
                finally:
                    self._control_exchange_output.close()
