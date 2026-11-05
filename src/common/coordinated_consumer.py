from collections.abc import Callable
from queue import Empty, Queue
from threading import Thread

from common.message_protocol.message import Message
from common.middleware.middleware import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)

Ack = Callable[[], None]
Nack = Callable[..., None]
MessageCallback = Callable[
    [Message, Ack, Nack],
    None,
]
ControlMessageCallback = Callable[[Message], None]


class CoordinatedConsumer:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        control_exchange_input: MessageMiddlewareExchange,
    ) -> None:
        self._input_queue = input_queue
        self._control_exchange_input = control_exchange_input
        self._pending_control_messages: Queue[Message] = Queue()
        self._stopped = False
        self._control_error: Exception | None = None

    def _consume_control_messages(self) -> None:
        def enqueue_control_message(
            body: bytes,
            ack: Ack,
            nack: Nack,
        ) -> None:
            try:
                message = Message.deserialize(body)
                self._pending_control_messages.put(message)
            except Exception:
                nack()
                raise

            ack()

        try:
            self._control_exchange_input.start_consuming(enqueue_control_message)
        except Exception as error:
            self._control_error = error

    def start_consuming(
        self,
        on_message: MessageCallback,
        on_control_message: ControlMessageCallback,
    ) -> None:
        def deserialize_message(
            body: bytes,
            ack: Ack,
            nack: Nack,
        ) -> None:
            message = Message.deserialize(body)
            on_message(message, ack, nack)

        if self._stopped:
            return
        self._control_error = None

        control_thread = Thread(
            target=self._consume_control_messages,
            name="control-consumer",
        )
        control_thread.start()

        def process_pending_control() -> None:
            if self._control_error is not None:
                raise self._control_error
            pending = []
            while not self._stopped:
                try:
                    pending.append(self._pending_control_messages.get_nowait())
                except Empty:
                    break
            if not pending:
                return
            # Snapshot controls BEFORE the broker fence: controls arriving during
            # the fence belong to the next pass and may precede newer deliveries.
            self._input_queue.drain_pending()
            for message in pending:
                if self._stopped:
                    break
                on_control_message(message)

        try:
            self._input_queue.start_consuming(
                deserialize_message, on_idle=process_pending_control
            )
        finally:
            self._control_exchange_input.stop_consuming()
            control_thread.join()

    def stop_consuming(self) -> None:
        self._stopped = True

        try:
            self._input_queue.stop_consuming()
        finally:
            self._control_exchange_input.stop_consuming()

    def close(self) -> None:
        try:
            self._input_queue.close()
        finally:
            self._control_exchange_input.close()
