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


class WordExtractorMessaging:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        q3_output_queue: MessageMiddlewareQueue,
        q4_q5_output_queue: MessageMiddlewareQueue,
        control_exchange_input: MessageMiddlewareExchange,
        control_exchange_output: MessageMiddlewareExchange,
    ) -> None:
        self._consumer = CoordinatedConsumer(
            input_queue=input_queue,
            control_exchange_input=control_exchange_input,
        )
        self._q3_output_queue = q3_output_queue
        self._q4_q5_output_queue = q4_q5_output_queue
        self._control_exchange_output = control_exchange_output

    def publish_q3(self, message: Message) -> None:
        self._q3_output_queue.send(message.serialize())

    def publish_q4_q5(self, message: Message) -> None:
        self._q4_q5_output_queue.send(message.serialize())

    def broadcast_control(self, message: Message) -> None:
        self._control_exchange_output.send(message.serialize())

    def start_consuming(
        self,
        on_message: MessageCallback,
        on_control_message: ControlMessageCallback,
    ) -> None:
        self._consumer.start_consuming(
            on_message,
            on_control_message,
        )

    def stop_consuming(self) -> None:
        self._consumer.stop_consuming()

    def close(self) -> None:
        try:
            self._consumer.close()
        finally:
            try:
                self._q3_output_queue.close()
            finally:
                try:
                    self._q4_q5_output_queue.close()
                finally:
                    self._control_exchange_output.close()
