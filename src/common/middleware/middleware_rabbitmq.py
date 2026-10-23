from contextlib import contextmanager
from functools import partial

import pika

from common.middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


@contextmanager
def _pika_error_to_middleware_error(
    operation,
    error_type,
    connection_error=MessageMiddlewareDisconnectedError,
):
    try:
        yield
    except pika.exceptions.AMQPConnectionError as error:
        raise connection_error(
            f"Error connecting to RabbitMQ during {operation}: {error}"
        ) from error
    except pika.exceptions.AMQPError as error:
        raise error_type(f"Error during {operation} in RabbitMQ: {error}") from error


class _MessageMiddlewareRabbitMQBase:
    def __init__(self, host: str) -> None:
        with _pika_error_to_middleware_error(
            "initialization",
            MessageMiddlewareMessageError,
        ):
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()

    def _check_connection(self) -> None:
        if self._connection.is_closed:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ is closed")

        if self._channel.is_closed:
            raise MessageMiddlewareMessageError("Channel to RabbitMQ is closed")

    def start_consuming(self, on_message_callback) -> None:
        self._check_connection()

        def callback(channel, method, _properties, body) -> None:
            ack = partial(
                channel.basic_ack,
                delivery_tag=method.delivery_tag,
            )
            nack = partial(
                channel.basic_nack,
                delivery_tag=method.delivery_tag,
            )
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
            self._channel.start_consuming()

    def stop_consuming(self) -> None:
        if self._connection.is_closed:
            return

        with _pika_error_to_middleware_error(
            "stopping consumption",
            MessageMiddlewareMessageError,
        ):
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)

    def close(self) -> None:
        if self._connection.is_closed:
            return

        with _pika_error_to_middleware_error(
            "closing connection",
            MessageMiddlewareCloseError,
            MessageMiddlewareCloseError,
        ):
            self._connection.close()


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQBase,
    MessageMiddlewareQueue,
):
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
