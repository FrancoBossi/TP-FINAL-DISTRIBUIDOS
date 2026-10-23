import unittest
from unittest.mock import Mock

from common.message_protocol.message import QueryEndMessage, RoutedInputEndMessage
from gateway.messaging import GatewayMessaging


class TestGatewayMessaging(unittest.TestCase):
    def setUp(self):
        self.input_queue = Mock()
        self.result_queue = Mock()
        self.execution_id = "execution-123"
        self.result_queue_name = f"results.{self.execution_id}"

        self.messaging = GatewayMessaging(
            input_queue=self.input_queue,
            result_queue=self.result_queue,
            result_queue_name=self.result_queue_name,
        )

    def test_publishes_serialized_message_to_input_queue(self):
        message = RoutedInputEndMessage(
            execution_id=self.execution_id,
            reply_queue=self.result_queue_name,
        )

        self.messaging.publish(message)

        self.input_queue.send.assert_called_once_with(message.serialize())
        self.assertEqual(
            self.messaging.result_queue_name,
            self.result_queue_name,
        )

    def test_deserializes_result_and_preserves_acknowledgements(self):
        on_result = Mock()
        ack = Mock()
        nack = Mock()

        self.messaging.start_consuming_results(on_result)

        self.result_queue.start_consuming.assert_called_once()
        middleware_callback = self.result_queue.start_consuming.call_args.args[0]

        message = QueryEndMessage(
            query_id=1,
            execution_id=self.execution_id,
        )
        middleware_callback(message.serialize(), ack, nack)

        on_result.assert_called_once_with(message, ack, nack)

    def test_stops_consumption_and_closes_both_queues(self):
        self.messaging.stop_consuming_results()
        self.messaging.close()

        self.result_queue.stop_consuming.assert_called_once()
        self.input_queue.close.assert_called_once()
        self.result_queue.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
