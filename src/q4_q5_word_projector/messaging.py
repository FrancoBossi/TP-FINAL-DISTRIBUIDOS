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


class Q4Q5WordProjectorMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        q4_output_queues: dict[str, MessageMiddlewareQueue],
        q5_output_queue: MessageMiddlewareQueue,
        control_exchange_input: MessageMiddlewareExchange,
        control_exchange_output: MessageMiddlewareExchange,
    ) -> None:
        self._consumer = CoordinatedConsumer(input_queue, control_exchange_input)
        self._q4_output_queues = q4_output_queues
        self._q5_output_queue = q5_output_queue
        self._control_exchange_output = control_exchange_output

    def publish_q4_to(self, queue: str, message: Message) -> None:
        self._q4_output_queues[queue].send(message.serialize())

    def publish_q5(self, message: Message) -> None:
        self._q5_output_queue.send(message.serialize())

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
                for queue in self._q4_output_queues.values():
                    queue.close()
            finally:
                try:
                    self._q5_output_queue.close()
                finally:
                    self._control_exchange_output.close()
