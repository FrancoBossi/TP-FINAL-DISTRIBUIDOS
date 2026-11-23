from collections.abc import Callable
from dataclasses import dataclass, field
from queue import Empty, Queue
from threading import Event, Thread

from common.health import LIVENESS
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


@dataclass
class _PendingControl:
    message: Message
    completed: Event = field(default_factory=Event)
    error: Exception | None = None


class CoordinatedConsumer:
    def __init__(
        self,
        input_queue: MessageMiddlewareQueue,
        control_exchange_input: MessageMiddlewareExchange,
    ) -> None:
        self._input_queue = input_queue
        self._control_exchange_input = control_exchange_input
        self._pending_control_messages: Queue[_PendingControl] = Queue()
        self._stopped = False
        self._control_shutdown = Event()
        self._control_error: Exception | None = None

    def _consume_control_messages(self) -> None:
        def enqueue_control_message(
            body: bytes,
            ack: Ack,
            nack: Nack,
        ) -> None:
            try:
                message = Message.deserialize(body)
                pending = _PendingControl(message)
                self._pending_control_messages.put(pending)
            except Exception:
                nack()
                raise

            while not pending.completed.wait(timeout=0.1):
                if self._control_shutdown.is_set():
                    nack(requeue=True)
                    return

            if pending.error is not None:
                nack(requeue=True)
                return
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
        self._control_shutdown.clear()
        self._control_error = None

        control_thread = Thread(
            target=self._consume_control_messages,
            name="control-consumer",
        )
        control_thread.start()
        # A dead control thread leaves barriers unresolved even if DATA flows.
        LIVENESS.add_check(
            "control-consumer",
            lambda: control_thread.is_alive() and self._control_error is None,
        )

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
            for pending_control in pending:
                if self._stopped:
                    break
                try:
                    on_control_message(pending_control.message)
                except Exception as error:
                    pending_control.error = error
                    pending_control.completed.set()
                    raise
                pending_control.completed.set()

        try:
            self._input_queue.start_consuming(
                deserialize_message, on_idle=process_pending_control
            )
        finally:
            LIVENESS.remove_check("control-consumer")
            self._control_exchange_input.stop_consuming()
            control_thread.join()

    def stop_consuming(self) -> None:
        self._stopped = True
        self._control_shutdown.set()

        try:
            self._input_queue.stop_consuming()
        finally:
            self._control_exchange_input.stop_consuming()

    def close(self) -> None:
        try:
            self._input_queue.close()
        finally:
            self._control_exchange_input.close()
