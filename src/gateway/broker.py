"""One RabbitMQ connection per run, owned by the gateway thread."""

from collections.abc import Iterator

from common.message_protocol.message import Message


class RabbitMQBroker:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        input_queue: str,
        run_id: str,
    ) -> None:
        import pika

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                credentials=pika.PlainCredentials(username, password),
            )
        )
        try:
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.input_queue = input_queue
            self.channel.queue_declare(queue=self.input_queue, durable=True)
            self.reply_queue = f"results.{run_id}"
            self.channel.queue_declare(
                queue=self.reply_queue, exclusive=True, auto_delete=True
            )
            self.channel.basic_qos(prefetch_count=1)
        except Exception:
            self.connection.close()
            raise

    def publish(self, message: Message) -> None:
        import pika

        body = message.serialize()
        self.channel.basic_publish(
            exchange="",
            routing_key=self.input_queue,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )

    def results(self) -> Iterator[tuple[int, Message] | None]:
        for method, _, body in self.channel.consume(
            self.reply_queue, inactivity_timeout=1
        ):
            if method is None:
                yield None
            else:
                yield method.delivery_tag, Message.deserialize(body)

    def acknowledge_result(self, delivery_tag: int) -> None:
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def close(self) -> None:
        if self.connection.is_open:
            self.connection.close()
