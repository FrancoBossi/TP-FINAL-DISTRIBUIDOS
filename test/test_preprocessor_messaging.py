import unittest
from threading import Event
from unittest.mock import Mock

from common.message_protocol.message import (
    NormalizedArticleBatchMessage,
    RoutedInputEndMessage,
)
from preprocessor.messaging import PreprocessorMessaging


class TestPreprocessorMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.output_queue = Mock()
        self.control_exchange_input = Mock()
        self.control_exchange_output = Mock()

        self.messaging = PreprocessorMessaging(
            input_queue=self.input_queue,
            output_queue=self.output_queue,
            control_exchange_input=self.control_exchange_input,
            control_exchange_output=self.control_exchange_output,
        )

    def test_publishes_serialized_message_to_output_queue(self):
        message = NormalizedArticleBatchMessage(
            articles=[{"name": "Example article"}],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.publish(message)

        self.output_queue.send.assert_called_once_with(message.serialize())

    def test_broadcasts_serialized_message_through_control_exchange(self):
        message = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.messaging.broadcast_control(message)

        self.control_exchange_output.send.assert_called_once_with(message.serialize())

    def test_deserializes_input_and_preserves_acknowledgements(self):
        on_message = Mock()
        on_control_message = Mock()
        ack = Mock()
        nack = Mock()

        message = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        def consume_input(middleware_callback):
            middleware_callback(message.serialize(), ack, nack)
            self.messaging.stop_consuming()

        self.input_queue.start_consuming.side_effect = consume_input

        self.messaging.start_consuming(
            on_message,
            on_control_message,
        )

        on_message.assert_called_once_with(message, ack, nack)
        on_control_message.assert_not_called()

    def test_processes_control_message_from_main_thread(self):
        input_started = Event()
        input_stopped = Event()
        control_ack = Mock()
        control_nack = Mock()

        message = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        def consume_input(_middleware_callback):
            input_started.set()
            self.assertTrue(input_stopped.wait(timeout=1))

        def stop_input():
            input_stopped.set()

        def consume_control(middleware_callback):
            self.assertTrue(input_started.wait(timeout=1))
            middleware_callback(
                message.serialize(),
                control_ack,
                control_nack,
            )

        self.input_queue.start_consuming.side_effect = consume_input
        self.input_queue.stop_consuming.side_effect = stop_input
        self.control_exchange_input.start_consuming.side_effect = consume_control

        on_control_message = Mock(
            side_effect=lambda _message: self.messaging.stop_consuming()
        )

        self.messaging.start_consuming(
            Mock(),
            on_control_message,
        )

        on_control_message.assert_called_once_with(message)
        control_ack.assert_called_once()
        control_nack.assert_not_called()

    def test_stops_consumption_and_closes_all_connections(self):
        self.messaging.stop_consuming()
        self.messaging.close()

        self.input_queue.stop_consuming.assert_called_once()
        self.control_exchange_input.stop_consuming.assert_called_once()
        self.input_queue.close.assert_called_once()
        self.output_queue.close.assert_called_once()
        self.control_exchange_input.close.assert_called_once()
        self.control_exchange_output.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
