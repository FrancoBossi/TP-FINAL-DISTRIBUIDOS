import unittest
from unittest.mock import Mock, patch

from common.middleware import MessageMiddlewareQueueRabbitMQ


class TestMessageMiddlewareQueueRabbitMQ(unittest.TestCase):
    def test_declares_queue_and_sends_message(self):
        connection = Mock()
        connection.is_closed = False

        channel = connection.channel.return_value
        channel.is_closed = False

        with patch(
            "common.middleware.middleware_rabbitmq.pika.BlockingConnection",
            return_value=connection,
        ):
            queue = MessageMiddlewareQueueRabbitMQ(
                host="rabbitmq",
                queue_name="results.execution-123",
                exclusive=True,
                auto_delete=True,
            )

        channel.queue_declare.assert_called_once_with(
            queue="results.execution-123",
            durable=False,
            exclusive=True,
            auto_delete=True,
        )

        message = b"example message"
        queue.send(message)

        channel.basic_publish.assert_called_once_with(
            exchange="",
            routing_key="results.execution-123",
            body=message,
        )

    def test_consumes_message_with_manual_ack_and_nack(self):
        connection = Mock()
        connection.is_closed = False

        channel = connection.channel.return_value
        channel.is_closed = False

        with patch(
            "common.middleware.middleware_rabbitmq.pika.BlockingConnection",
            return_value=connection,
        ):
            queue = MessageMiddlewareQueueRabbitMQ(
                host="rabbitmq",
                queue_name="results.execution-123",
            )

        on_message = Mock()

        queue.start_consuming(on_message)

        channel.basic_qos.assert_called_once_with(prefetch_count=1)
        channel.basic_consume.assert_called_once_with(
            queue="results.execution-123",
            on_message_callback=unittest.mock.ANY,
            auto_ack=False,
        )
        channel.start_consuming.assert_called_once()

        rabbitmq_callback = channel.basic_consume.call_args.kwargs[
            "on_message_callback"
        ]

        method = Mock()
        method.delivery_tag = 42
        body = b"result message"

        rabbitmq_callback(channel, method, None, body)

        on_message.assert_called_once()
        received_body, ack, nack = on_message.call_args.args

        self.assertEqual(received_body, body)

        ack()
        nack()

        channel.basic_ack.assert_called_once_with(delivery_tag=42)
        channel.basic_nack.assert_called_once_with(delivery_tag=42)


if __name__ == "__main__":
    unittest.main()
