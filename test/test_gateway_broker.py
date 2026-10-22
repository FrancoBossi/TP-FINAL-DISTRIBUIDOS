import unittest
from unittest.mock import Mock, patch

from common.message_protocol.message import (
    Message,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from gateway.broker import RabbitMQBroker


class TestGatewayBroker(unittest.TestCase):
    def test_publishes_articles_and_end_once_to_input_batcher(self):
        connection = Mock()
        channel = connection.channel.return_value
        with patch("pika.BlockingConnection", return_value=connection):
            broker = RabbitMQBroker(
                "rabbitmq", "guest", "guest", "input_batcher.input", run_id="run"
            )
        channel.confirm_delivery.assert_called_once()
        declarations = [call.kwargs for call in channel.queue_declare.call_args_list]
        self.assertEqual(
            declarations,
            [
                {"queue": "input_batcher.input", "durable": True},
                {"queue": "results.run", "exclusive": True, "auto_delete": True},
            ],
        )
        for message in (
            RoutedArticleMessage({}, "run", "results.run"),
            RoutedInputEndMessage("run", "results.run"),
        ):
            channel.basic_publish.reset_mock()
            broker.publish(message)
            channel.basic_publish.assert_called_once()
            published = channel.basic_publish.call_args.kwargs
            self.assertEqual(published["routing_key"], "input_batcher.input")
            self.assertEqual(Message.deserialize(published["body"]), message)
            self.assertTrue(published["mandatory"])
        self.assertEqual(len(declarations), 2)

    def test_configurable_input_queue_preserves_publication_order(self):
        connection = Mock()
        channel = connection.channel.return_value
        with patch("pika.BlockingConnection", return_value=connection):
            broker = RabbitMQBroker(
                "rabbitmq", "guest", "guest", input_queue="batcher.custom", run_id="run"
            )
        channel.queue_declare.assert_any_call(queue="batcher.custom", durable=True)
        messages = [
            RoutedArticleMessage({"name": "first"}, "run", "results.run"),
            RoutedArticleMessage({"name": "second"}, "run", "results.run"),
            RoutedInputEndMessage("run", "results.run"),
        ]
        for message in messages:
            broker.publish(message)
        calls = channel.basic_publish.call_args_list
        self.assertEqual(
            [Message.deserialize(call.kwargs["body"]) for call in calls], messages
        )
        self.assertTrue(
            all(call.kwargs["routing_key"] == "batcher.custom" for call in calls)
        )

    def test_closes_connection_if_setup_fails(self):
        connection = Mock()
        connection.channel.return_value.queue_declare.side_effect = RuntimeError(
            "failed"
        )
        with patch("pika.BlockingConnection", return_value=connection):
            with self.assertRaises(RuntimeError):
                RabbitMQBroker(
                    "rabbitmq", "guest", "guest", "input_batcher.input", run_id="run"
                )
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
