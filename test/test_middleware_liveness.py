import unittest
from unittest.mock import MagicMock, Mock, patch

from common.health import TICK_INTERVAL_SECONDS
from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)

MODULE = "common.middleware.middleware_rabbitmq"


def open_connection():
    connection = Mock()
    connection.is_closed = False
    connection.channel.return_value.is_closed = False
    return connection


class TestQueueReportsLiveness(unittest.TestCase):
    def make_queue(self, connection):
        with patch(f"{MODULE}.pika.BlockingConnection", return_value=connection):
            return MessageMiddlewareQueueRabbitMQ("rabbitmq", "input")

    def test_health_check_tracks_the_rabbitmq_connection_and_channel(self):
        connection = open_connection()
        channel = connection.channel.return_value
        connection.is_open = True
        channel.is_open = True
        liveness = MagicMock()

        with patch(f"{MODULE}.LIVENESS", liveness):
            queue = self.make_queue(connection)
            health_check = liveness.add_check.call_args.args[1]

            self.assertTrue(health_check())
            connection.is_open = False
            self.assertFalse(health_check())
            connection.is_open = True
            channel.is_open = False
            self.assertFalse(health_check())

            queue.close()

        liveness.remove_check.assert_called_once_with(queue._health_check_name)

    def test_consumer_uses_the_bounded_poll_loop_and_ticks(self):
        connection = open_connection()
        queue = self.make_queue(connection)
        passes = []

        def process_data_events(**kwargs):
            passes.append(kwargs)
            queue.stop_consuming()

        connection.process_data_events.side_effect = process_data_events
        with patch(f"{MODULE}.LIVENESS") as liveness:
            queue.start_consuming(Mock())

        self.assertEqual(passes, [{"time_limit": TICK_INTERVAL_SECONDS}])
        liveness.tick.assert_called_once_with()
        connection.call_later.assert_not_called()

    def test_delivery_handler_runs_inside_the_busy_scope(self):
        connection = open_connection()
        queue = self.make_queue(connection)
        channel = connection.channel.return_value
        inside = []
        liveness = MagicMock()
        handler = Mock(
            side_effect=lambda *_: inside.append(
                liveness.busy.return_value.__enter__.called
            )
        )

        def process_data_events(**_kwargs):
            delivery = channel.basic_consume.call_args.kwargs["on_message_callback"]
            delivery(channel, Mock(delivery_tag=1), None, b"work")
            queue.stop_consuming()

        connection.process_data_events.side_effect = process_data_events
        with patch(f"{MODULE}.LIVENESS", liveness):
            queue.start_consuming(handler)

        self.assertEqual(inside, [True])
        liveness.busy.return_value.__exit__.assert_called_once()

    def test_idle_loop_ticks_each_pass_and_guards_the_idle_handler(self):
        connection = open_connection()
        queue = self.make_queue(connection)
        passes = []

        def process_data_events(**_kwargs):
            passes.append(1)
            if len(passes) == 3:
                queue.stop_consuming()

        connection.process_data_events.side_effect = process_data_events
        on_idle = Mock()
        liveness = MagicMock()

        with patch(f"{MODULE}.LIVENESS", liveness):
            queue.start_consuming(Mock(), on_idle=on_idle)

        self.assertEqual(liveness.tick.call_count, 3)
        self.assertEqual(on_idle.call_count, 2)
        self.assertEqual(liveness.busy.call_count, 2)
        self.assertEqual(
            connection.process_data_events.call_args_list,
            [unittest.mock.call(time_limit=TICK_INTERVAL_SECONDS)] * 3,
        )
        connection.call_later.assert_not_called()


class TestExchangeDoesNotReportLiveness(unittest.TestCase):
    def test_control_consumer_cannot_mask_a_blocked_data_loop(self):
        connection = open_connection()
        with patch(f"{MODULE}.pika.BlockingConnection", return_value=connection):
            exchange = MessageMiddlewareExchangeRabbitMQ("rabbitmq", "ctl", ["ctl"])

        channel = connection.channel.return_value
        liveness = MagicMock()
        handler = Mock()
        connection.process_data_events.side_effect = (
            lambda **_kwargs: exchange.stop_consuming()
        )

        with patch(f"{MODULE}.LIVENESS", liveness):
            exchange.start_consuming(handler)
            delivery = channel.basic_consume.call_args.kwargs["on_message_callback"]
            delivery(channel, Mock(delivery_tag=1), None, b"control")

        handler.assert_called_once()
        liveness.tick.assert_not_called()
        liveness.busy.assert_not_called()
        connection.call_later.assert_not_called()


if __name__ == "__main__":
    unittest.main()
