import unittest
from unittest.mock import Mock, call, patch

import pika

from common.message_protocol.message import (
    AckMessage,
    QueryEndMessage,
    QueryPartitionEndMessage,
    ResultMessage,
)
from common.middleware.middleware import MessageMiddlewareUnroutableError
from common.middleware.middleware_rabbitmq import MessagePublisherRabbitMQ
from result_sink.config import ResultSinkConfig
from result_sink.controller import ResultSinkController
from result_sink.messaging import ResultSinkMessaging


class TestResultSink(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.publisher = Mock()
        self.messaging = ResultSinkMessaging(self.input_queue, self.publisher)
        self.partition_counts = {1: 3, 2: 2, 3: 1, 4: 1, 5: 1}
        self.controller = ResultSinkController(
            self.messaging,
            self.partition_counts,
        )
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

    def test_query_end_is_published_after_all_query_partitions_finish(self):
        ends = [
            QueryPartitionEndMessage(1, partition_id, "execution-a")
            for partition_id in (0, 1, 1, 2)
        ]

        for end in ends[:-1]:
            self.deliver(end.serialize(), self.ack, self.nack)
            self.publisher.send_to.assert_not_called()

        self.deliver(ends[-1].serialize(), self.ack, self.nack)

        query_end = QueryEndMessage(1, "execution-a")
        self.publisher.send_to.assert_called_once_with(
            "results.execution-a",
            query_end.serialize(),
        )
        self.assertEqual(self.ack.call_count, 4)
        self.nack.assert_not_called()

    def test_query_barriers_are_isolated_by_execution_and_query(self):
        messages = [
            QueryPartitionEndMessage(1, 0, "execution-a"),
            QueryPartitionEndMessage(1, 0, "execution-b"),
            QueryPartitionEndMessage(2, 0, "execution-a"),
            QueryPartitionEndMessage(1, 1, "execution-a"),
            QueryPartitionEndMessage(2, 1, "execution-a"),
        ]

        for message in messages:
            self.deliver(message.serialize(), self.ack, self.nack)

        query_end = QueryEndMessage(2, "execution-a")
        self.publisher.send_to.assert_called_once_with(
            "results.execution-a",
            query_end.serialize(),
        )

    def test_completed_barrier_retries_query_end_after_publish_failure(self):
        message = QueryPartitionEndMessage(3, 0, "execution-a")
        self.publisher.send_to.side_effect = RuntimeError("unavailable")

        with self.assertRaises(RuntimeError):
            self.deliver(message.serialize(), self.ack, self.nack)

        self.ack.assert_not_called()
        self.nack.assert_called_once()

        self.publisher.send_to.side_effect = None
        self.deliver(message.serialize(), self.ack, self.nack)

        query_end = QueryEndMessage(3, "execution-a")
        self.publisher.send_to.assert_called_with(
            "results.execution-a",
            query_end.serialize(),
        )
        self.ack.assert_called_once()

    def test_rejects_query_partition_outside_configured_range(self):
        message = QueryPartitionEndMessage(2, 2, "execution-a")

        with self.assertRaisesRegex(ValueError, "outside configured range"):
            self.deliver(message.serialize(), self.ack, self.nack)

        self.publisher.send_to.assert_not_called()
        self.ack.assert_not_called()
        self.nack.assert_called_once()

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

    def test_missing_destination_is_discarded_and_next_execution_continues(self):
        self.publisher.send_to.side_effect = MessageMiddlewareUnroutableError("gone")

        for message in (
            ResultMessage(1, {}, "old"),
            QueryEndMessage(1, "old"),
        ):
            self.deliver(message.serialize(), self.ack, self.nack)

        self.assertEqual(self.ack.call_count, 2)
        self.nack.assert_not_called()

        self.publisher.send_to.side_effect = None
        message = ResultMessage(1, {}, "new")
        self.deliver(message.serialize(), self.ack, self.nack)

        self.publisher.send_to.assert_called_with(
            "results.new",
            message.serialize(),
        )
        self.assertEqual(self.ack.call_count, 3)

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
        with self.assertRaises(MessageMiddlewareUnroutableError):
            publisher.send_to("results.missing", b"result")


class TestResultSinkConfig(unittest.TestCase):
    def test_reads_partition_count_for_each_query(self):
        environment = {
            "Q1_PARTITION_COUNT": "3",
            "Q2_PARTITION_COUNT": "4",
            "Q3_PARTITION_COUNT": "5",
            "Q4_PARTITION_COUNT": "2",
            "Q5_PARTITION_COUNT": "1",
        }

        with patch.dict("os.environ", environment, clear=True):
            config = ResultSinkConfig()

        self.assertEqual(
            config.partition_counts,
            {1: 3, 2: 4, 3: 5, 4: 2, 5: 1},
        )
