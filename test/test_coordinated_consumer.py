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

        def consume_input(middleware_callback):
            middleware_callback(self.message.serialize(), ack, nack)
            self.consumer.stop_consuming()

        self.input_queue.start_consuming.side_effect = consume_input

        self.consumer.start_consuming(on_message, on_control_message)

        on_message.assert_called_once_with(self.message, ack, nack)
        on_control_message.assert_not_called()

    def test_processes_control_message_after_pausing_data(self):
        input_started = Event()
        input_stopped = Event()
        control_ack = Mock()
        control_nack = Mock()

        def consume_input(_middleware_callback):
            input_started.set()
            self.assertTrue(input_stopped.wait(timeout=1))

        def stop_input():
            input_stopped.set()

        def consume_control(middleware_callback):
            self.assertTrue(input_started.wait(timeout=1))
            middleware_callback(
                self.message.serialize(),
                control_ack,
                control_nack,
            )

        self.input_queue.start_consuming.side_effect = consume_input
        self.input_queue.stop_consuming.side_effect = stop_input
        self.control_exchange_input.start_consuming.side_effect = consume_control

        on_control_message = Mock(
            side_effect=lambda _message: self.consumer.stop_consuming()
        )

        self.consumer.start_consuming(Mock(), on_control_message)

        on_control_message.assert_called_once_with(self.message)
        control_ack.assert_called_once()
        control_nack.assert_not_called()

    def test_propagates_control_consumer_error_to_data_thread(self):
        input_stopped = Event()
        error = RuntimeError("control unavailable")

        self.control_exchange_input.start_consuming.side_effect = error
        self.input_queue.stop_consuming.side_effect = input_stopped.set
        self.input_queue.start_consuming.side_effect = lambda _callback: (
            self.assertTrue(input_stopped.wait(timeout=1))
        )

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
