import unittest
from threading import Event
from unittest.mock import Mock

from common.coordinated_consumer import CoordinatedConsumer
from common.message_protocol.message import RoutedInputEndMessage


class TestCoordinatedConsumer(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.control_exchange_input = Mock()
        self.consumer = CoordinatedConsumer(
            input_queue=self.input_queue,
            control_exchange_input=self.control_exchange_input,
        )
        self.message = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

    def test_deserializes_data_message_and_preserves_acknowledgements(self):
        on_message = Mock()
        on_control_message = Mock()
        ack = Mock()
        nack = Mock()

        def consume_input(middleware_callback, on_idle=None):
            middleware_callback(self.message.serialize(), ack, nack)
            self.consumer.stop_consuming()

        self.input_queue.start_consuming.side_effect = consume_input

        self.consumer.start_consuming(on_message, on_control_message)

        on_message.assert_called_once_with(self.message, ack, nack)
        on_control_message.assert_not_called()

    def test_control_waits_for_prefetched_data_without_cancelling_consumer(self):
        control_received = Event()
        events = []
        ack = Mock()
        nack = Mock()

        def consume_control(callback):
            callback(self.message.serialize(), ack, nack)
            control_received.set()

        def consume_input(callback, on_idle):
            self.assertTrue(control_received.wait(timeout=1))
            self.input_queue.stop_consuming.assert_not_called()
            on_idle()

        self.control_exchange_input.start_consuming.side_effect = consume_control
        self.input_queue.start_consuming.side_effect = consume_input
        self.input_queue.drain_pending.side_effect = lambda: events.append("data")
        self.consumer.start_consuming(
            lambda *args: events.append("data"),
            lambda message: events.append("end"),
        )
        self.assertEqual(events, ["data", "end"])
        ack.assert_called_once()
        nack.assert_not_called()

    def test_propagates_control_consumer_error_to_data_thread(self):
        failed = Event()
        error = RuntimeError("control unavailable")

        def consume_control(callback):
            failed.set()
            raise error

        def consume_input(callback, on_idle):
            self.assertTrue(failed.wait(timeout=1))
            # Wait for the control thread to record its failure.
            self.consumer._control_error = error
            on_idle()

        self.control_exchange_input.start_consuming.side_effect = consume_control
        self.input_queue.start_consuming.side_effect = consume_input
        with self.assertRaises(RuntimeError) as raised:
            self.consumer.start_consuming(Mock(), Mock())
        self.assertIs(raised.exception, error)

    def test_stops_and_closes_both_input_connections(self):
        self.consumer.stop_consuming()
        self.consumer.close()

        self.input_queue.stop_consuming.assert_called_once()
        self.control_exchange_input.stop_consuming.assert_called_once()
        self.input_queue.close.assert_called_once()
        self.control_exchange_input.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
