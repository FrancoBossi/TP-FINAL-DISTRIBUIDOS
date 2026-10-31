import unittest
from unittest.mock import Mock, call, patch

import pika

from common.message_protocol.message import (
    AckMessage,
    QueryEndMessage,
    ResultMessage,
)
from common.middleware.middleware import MessageMiddlewareMessageError
from common.middleware.middleware_rabbitmq import MessagePublisherRabbitMQ
from result_sink.controller import ResultSinkController
from result_sink.messaging import ResultSinkMessaging


class TestResultSink(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.publisher = Mock()
        self.messaging = ResultSinkMessaging(self.input_queue, self.publisher)
        self.controller = ResultSinkController(self.messaging)
        self.ack = Mock()
        self.nack = Mock()
        self.controller.start()
        self.deliver = self.input_queue.start_consuming.call_args.args[0]

    def test_streams_interleaved_results_and_ends_without_waiting(self):
        messages = [
            ResultMessage(1, {"url": "first"}, "a"),
            QueryEndMessage(1, "a"),
            ResultMessage(2, {"image": "base64"}, "b"),
            ResultMessage(3, {"word": "shared"}, "a"),
            QueryEndMessage(2, "b"),
        ]
        events = Mock()
        events.attach_mock(self.publisher.send_to, "publish")
        events.attach_mock(self.ack, "ack")
        for message in messages:
            self.deliver(message.serialize(), self.ack, self.nack)
            self.assertEqual(
                self.publisher.send_to.call_args,
                call(f"results.{message.execution_id}", message.serialize()),
            )
        self.assertEqual(
            events.mock_calls,
            [
                event
                for message in messages
                for event in (
                    call.publish(
                        f"results.{message.execution_id}", message.serialize()
                    ),
                    call.ack(),
                )
            ],
        )
        self.nack.assert_not_called()

    def test_failed_publish_is_not_acknowledged_and_can_be_retried(self):
        message = QueryEndMessage(1, "a")
        self.publisher.send_to.side_effect = RuntimeError("unavailable")
        with self.assertRaises(RuntimeError):
            self.deliver(message.serialize(), self.ack, self.nack)
        self.ack.assert_not_called()
        self.nack.assert_called_once()
        self.publisher.send_to.side_effect = None
        self.deliver(message.serialize(), self.ack, self.nack)
        self.ack.assert_called_once()

    def test_rejects_invalid_input(self):
        for body in (
            b"invalid json",
            AckMessage().serialize(),
            ResultMessage(6, {}, "a").serialize(),
            ResultMessage(1, {}, "").serialize(),
            ResultMessage(1, [], "a").serialize(),
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.deliver(body, self.ack, self.nack)
        self.publisher.send_to.assert_not_called()
        self.ack.assert_not_called()
        self.assertEqual(self.nack.call_count, 5)

    def test_shutdown_closes_publisher_even_if_input_close_fails(self):
        self.controller.stop()
        self.input_queue.stop_consuming.assert_called_once()
        self.input_queue.close.side_effect = RuntimeError("close failed")
        with self.assertRaises(RuntimeError):
            self.messaging.close()
        self.publisher.close.assert_called_once()


class TestResultPublisher(unittest.TestCase):
    @patch("common.middleware.middleware_rabbitmq.pika.BlockingConnection")
    def test_confirms_publication_without_declaring_exclusive_queue(self, connect):
        connect.return_value.is_closed = False
        channel = connect.return_value.channel.return_value
        channel.is_closed = False
        publisher = MessagePublisherRabbitMQ("localhost")
        publisher.send_to("results.a", b"result")
        channel.confirm_delivery.assert_called_once()
        channel.queue_declare.assert_not_called()
        channel.basic_publish.assert_called_once_with(
            exchange="", routing_key="results.a", body=b"result", mandatory=True
        )
        channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        with self.assertRaises(MessageMiddlewareMessageError):
            publisher.send_to("results.missing", b"result")
