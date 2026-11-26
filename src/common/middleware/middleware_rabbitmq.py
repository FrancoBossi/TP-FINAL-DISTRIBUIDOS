from contextlib import contextmanager, nullcontext
from functools import partial
from threading import Event

import pika

from common.health import LIVENESS, TICK_INTERVAL_SECONDS
from common.middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewarePublisher,
    MessageMiddlewareQueue,
    MessageMiddlewareUnroutableError,
)


@contextmanager
def _pika_error_to_middleware_error(
    operation,
    error_type,
    connection_error=MessageMiddlewareDisconnectedError,
):
    try:
        yield
    except pika.exceptions.UnroutableError as error:
        raise MessageMiddlewareUnroutableError(
            f"Unroutable message during {operation} in RabbitMQ: {error}"
        ) from error
    except pika.exceptions.AMQPConnectionError as error:
        raise connection_error(
            f"Error connecting to RabbitMQ during {operation}: {error}"
        ) from error
    except pika.exceptions.AMQPError as error:
        raise error_type(f"Error during {operation} in RabbitMQ: {error}") from error


class _MessageMiddlewareRabbitMQBase:
    # Only the data consumer reports progress to the health endpoint. The control
    # thread of a CoordinatedConsumer must not mask a blocked data loop.
    _reports_liveness = False

    def __init__(self, host: str) -> None:
        self._stop_requested = Event()
        self._health_check_name = f"rabbitmq-{id(self)}"
        with _pika_error_to_middleware_error(
            "initialization",
            MessageMiddlewareMessageError,
        ):
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    # BlockingConnection does not service heartbeats on idle publisher connections.
                    heartbeat=0,
                    # Health checks do not guarantee every startup connection succeeds.
                    connection_attempts=10,
                    retry_delay=3,
                    socket_timeout=5,
                    stack_timeout=10,
                    blocked_connection_timeout=10,
                )
            )
            try:
                self._channel = self._connection.channel()
                self._channel.confirm_delivery()
                LIVENESS.add_check(
                    self._health_check_name, self._rabbitmq_connection_is_open
                )
            except Exception:
                self.close()
                raise

    def _check_connection(self) -> None:
        if self._connection.is_closed:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ is closed")

        if self._channel.is_closed:
            raise MessageMiddlewareMessageError("Channel to RabbitMQ is closed")

    def _rabbitmq_connection_is_open(self) -> bool:
        return self._connection.is_open and self._channel.is_open

    def _handler_scope(self):
        return LIVENESS.busy() if self._reports_liveness else nullcontext()

    def start_consuming(self, on_message_callback, on_idle=None) -> None:
        self._check_connection()
        if self._stop_requested.is_set():
            self._stop_requested.clear()
            return

        def callback(channel, method, _properties, body) -> None:
            ack = partial(
                channel.basic_ack,
                delivery_tag=method.delivery_tag,
            )
            nack = partial(
                channel.basic_nack,
                delivery_tag=method.delivery_tag,
            )
            if self._stop_requested.is_set():
                nack(requeue=True)
                return
            with self._handler_scope():
                on_message_callback(body, ack, nack)

        with _pika_error_to_middleware_error(
            "consuming messages",
            MessageMiddlewareMessageError,
        ):
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            while not self._stop_requested.is_set():
                self._connection.process_data_events(time_limit=TICK_INTERVAL_SECONDS)
                if self._reports_liveness:
                    LIVENESS.tick()
                if self._stop_requested.is_set():
                    break
                if on_idle is not None:
                    with self._handler_scope():
                        on_idle()

            if self._stop_requested.is_set():
                self._stop_requested.clear()

    def drain_pending(self) -> None:
        """Fence assigned deliveries, then dispatch them on their owner thread."""
        self._channel.queue_declare(queue=self._queue_name, passive=True)
        self._connection.process_data_events(time_limit=0)

    def stop_consuming(self) -> None:
        self._stop_requested.set()
        if self._connection.is_closed:
            return

        with _pika_error_to_middleware_error(
            "stopping consumption",
            MessageMiddlewareMessageError,
        ):
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)

    def close(self) -> None:
        LIVENESS.remove_check(self._health_check_name)
        if self._connection.is_closed:
            return

        with _pika_error_to_middleware_error(
            "closing connection",
            MessageMiddlewareCloseError,
            MessageMiddlewareCloseError,
        ):
            self._connection.close()


class MessagePublisherRabbitMQ(
    _MessageMiddlewareRabbitMQBase,
    MessageMiddlewarePublisher,
):
    """Publish to existing queues without declaring Gateway's exclusive queues."""

    def send_to(self, destination: str, message: bytes) -> None:
        self._check_connection()

        with _pika_error_to_middleware_error(
            "publishing result",
            MessageMiddlewareMessageError,
        ):
            self._channel.basic_publish(
                exchange="",
                routing_key=destination,
                body=message,
                mandatory=True,
            )


class MessageMiddlewareExchangeRabbitMQ(
    _MessageMiddlewareRabbitMQBase,
    MessageMiddlewareExchange,
):
    def __init__(
        self,
        host: str,
        exchange_name: str,
        routing_keys: list[str],
        create_queue: bool = True,
        queue_name: str | None = None,
    ) -> None:
        if create_queue and not queue_name:
            raise ValueError("A stable queue_name is required for an exchange consumer")

        self._exchange_name = exchange_name
        self._routing_keys = routing_keys

        super().__init__(host)

        with _pika_error_to_middleware_error(
            "exchange initialization",
            MessageMiddlewareMessageError,
        ):
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=False,
            )

            if create_queue:
                self._channel.queue_declare(
                    queue=queue_name,
                    durable=False,
                    exclusive=False,
                    auto_delete=False,
                )
                self._queue_name = queue_name

                for routing_key in routing_keys:
                    self._channel.queue_bind(
                        exchange=exchange_name,
                        queue=self._queue_name,
                        routing_key=routing_key,
                    )

    def send(self, message: bytes) -> None:
        self._check_connection()

        with _pika_error_to_middleware_error(
            "sending message",
            MessageMiddlewareMessageError,
        ):
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQBase,
    MessageMiddlewareQueue,
):
    _reports_liveness = True

    def __init__(
        self,
        host: str,
        queue_name: str,
        exclusive: bool = False,
        auto_delete: bool = False,
    ) -> None:
        self._queue_name = queue_name

        super().__init__(host)

        with _pika_error_to_middleware_error(
            "queue initialization",
            MessageMiddlewareMessageError,
        ):
            self._channel.queue_declare(
                queue=queue_name,
                durable=False,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )

    def send(self, message: bytes) -> None:
        self._check_connection()

        with _pika_error_to_middleware_error(
            "sending message",
            MessageMiddlewareMessageError,
        ):
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
            )
