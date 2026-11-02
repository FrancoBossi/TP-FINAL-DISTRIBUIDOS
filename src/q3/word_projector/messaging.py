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


class Q3WordProjectorMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        output_queues: dict[str, MessageMiddlewareQueue],
        control_exchange_input: MessageMiddlewareExchange,
        control_exchange_output: MessageMiddlewareExchange,
    ) -> None:
        self._consumer = CoordinatedConsumer(
            input_queue=input_queue,
            control_exchange_input=control_exchange_input,
        )
        self._output_queues = output_queues
        self._control_exchange_output = control_exchange_output

    def publish(self, queue: str, message: Message) -> None:
        self._output_queues[queue].send(message.serialize())

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
                for queue in self._output_queues.values():
                    queue.close()
            finally:
                self._control_exchange_output.close()
