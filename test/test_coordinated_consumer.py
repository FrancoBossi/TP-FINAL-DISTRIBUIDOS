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
            message_id="m1:input-end",
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
        control_delivered = Event()
        events = []
        ack = Mock()
        nack = Mock()

        def consume_control(callback):
            control_delivered.set()
            callback(self.message.serialize(), ack, nack)

        def consume_input(callback, on_idle):
            self.assertTrue(control_delivered.wait(timeout=1))
            self.input_queue.stop_consuming.assert_not_called()
            on_idle()

        self.control_exchange_input.start_consuming.side_effect = consume_control
        self.input_queue.start_consuming.side_effect = consume_input
        self.input_queue.drain_pending.side_effect = lambda: events.append("data")
        self.consumer.start_consuming(
            lambda *args: events.append("data"),
            lambda message: (
                self.assertFalse(ack.called),
                events.append("end"),
            ),
        )
        self.assertEqual(events, ["data", "end"])
        ack.assert_called_once()
        nack.assert_not_called()

    def test_failed_control_is_requeued_without_ack(self):
        control_delivered = Event()
        ack = Mock()
        nack = Mock()
        error = RuntimeError("control application failed")

        def consume_control(callback):
            control_delivered.set()
            callback(self.message.serialize(), ack, nack)

        def consume_input(callback, on_idle):
            self.assertTrue(control_delivered.wait(timeout=1))
            on_idle()

        self.control_exchange_input.start_consuming.side_effect = consume_control
        self.input_queue.start_consuming.side_effect = consume_input

        with self.assertRaises(RuntimeError) as raised:
            self.consumer.start_consuming(Mock(), Mock(side_effect=error))

        self.assertIs(raised.exception, error)
        ack.assert_not_called()
        nack.assert_called_once_with(requeue=True)

    def test_shutdown_requeues_control_not_yet_applied(self):
        control_delivered = Event()
        ack = Mock()
        nack = Mock()

        def consume_control(callback):
            control_delivered.set()
            callback(self.message.serialize(), ack, nack)

        def consume_input(callback, on_idle):
            self.assertTrue(control_delivered.wait(timeout=1))
            self.consumer.stop_consuming()

        self.control_exchange_input.start_consuming.side_effect = consume_control
        self.input_queue.start_consuming.side_effect = consume_input

        on_control_message = Mock()
        self.consumer.start_consuming(Mock(), on_control_message)

        on_control_message.assert_not_called()
        ack.assert_not_called()
        nack.assert_called_once_with(requeue=True)

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
