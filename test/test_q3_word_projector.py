import unittest
from unittest.mock import Mock, call, patch

from common.message_protocol.message import (
    AckMessage,
    PartitionEndMessage,
    TokenizedArticleBatchMessage,
)
from common.partition_router import PartitionRouter
from q3.word_projector.controller import Q3WordProjectorController
from q3.word_projector.messaging import Q3WordProjectorMessaging
from q3.word_projector.projector import project_batch


class TestQ3WordProjector(unittest.TestCase):
    def setUp(self):
        self.router = PartitionRouter("q3_word_aggregator.input", 7)
        self.batch = TokenizedArticleBatchMessage(
            articles=[
                {"language": "en", "words": ["café", "and", "café"]},
                {"language": "fr", "words": ["café", "and"]},
                {"language": "en", "words": ["café"]},
                {"language": "es", "words": ["ignored"]},
                {"language": "fr", "words": []},
            ],
            execution_id="run",
            reply_queue="results.run",
        )
        self.partition_end_0 = PartitionEndMessage(
            "word_extractor", 0, "run", "results.run"
        )
        self.partition_end_1 = PartitionEndMessage(
            "word_extractor", 1, "run", "results.run"
        )

    def test_deduplicates_per_article_keeps_stopwords_and_routes_both_languages(self):
        batches = project_batch(self.batch, self.router)
        presences = [p for batch in batches.values() for p in batch.presences]
        self.assertCountEqual(
            presences,
            [
                {"word": "café", "language": "en"},
                {"word": "café", "language": "en"},
                {"word": "café", "language": "fr"},
                {"word": "and", "language": "en"},
                {"word": "and", "language": "fr"},
            ],
        )
        for word in ("café", "and"):
            destinations = [
                q
                for q, b in batches.items()
                if any(p["word"] == word for p in b.presences)
            ]
            self.assertEqual(1, len(destinations))
        self.assertEqual(batches, project_batch(self.batch, self.router))
        for batch in batches.values():
            self.assertEqual("run", batch.execution_id)
            self.assertEqual("results.run", batch.reply_queue)

    def test_no_candidates_produce_no_batches(self):
        for articles in (
            [],
            [{"language": "fr", "words": []}],
            [{"language": "es", "words": ["hola"]}],
        ):
            self.assertEqual(
                {},
                project_batch(
                    TokenizedArticleBatchMessage(articles, "run", "reply"), self.router
                ),
            )

    def test_controller_publishes_before_ack_and_broadcasts_input_end(self):
        messaging, ack, nack = Mock(), Mock(), Mock()
        events = Mock()
        events.attach_mock(messaging, "messaging")
        events.attach_mock(ack, "ack")
        controller = Q3WordProjectorController(messaging, self.router, 1, 2)
        controller.process_message(self.batch, ack, nack)
        expected = [
            call.messaging.publish(q, b)
            for q, b in project_batch(self.batch, self.router).items()
        ]
        self.assertEqual(expected + [call.ack()], events.mock_calls)
        messaging.reset_mock()
        controller.process_message(self.partition_end_0, ack, nack)
        messaging.broadcast_control.assert_called_once_with(self.partition_end_0)
        nack.assert_not_called()

    def test_control_barrier_forwards_own_end_to_every_aggregator(self):
        messaging = Mock()
        controller = Q3WordProjectorController(messaging, self.router, 1, 2)

        controller.process_control_message(self.partition_end_0)
        messaging.publish.assert_not_called()
        controller.process_control_message(self.partition_end_1)

        expected = PartitionEndMessage("q3_word_projector", 1, "run", "results.run")
        self.assertEqual(
            [call.publish(f"q3_word_aggregator.input.{i}", expected) for i in range(7)],
            messaging.mock_calls,
        )

    def test_failure_and_unexpected_message_are_not_acked(self):
        for message in (
            self.batch,
            AckMessage(),
        ):
            with self.subTest(message=message):
                messaging, ack, nack = Mock(), Mock(), Mock()
                messaging.publish.side_effect = RuntimeError("unavailable")
                with self.assertRaises((RuntimeError, ValueError)):
                    Q3WordProjectorController(
                        messaging, self.router, 1, 2
                    ).process_message(message, ack, nack)
                ack.assert_not_called()
                nack.assert_called_once_with()

    def test_messaging_routes_serialized_messages_and_deserializes_input(self):
        input_queue = Mock()
        outputs = {f"out.{i}": Mock() for i in range(2)}
        control_input = Mock()
        control_output = Mock()
        with patch("q3.word_projector.messaging.CoordinatedConsumer") as consumer_class:
            consumer = consumer_class.return_value
            messaging = Q3WordProjectorMessaging(
                input_queue,
                outputs,
                control_input,
                control_output,
            )
        end = PartitionEndMessage("q3_word_projector", 1, "run", "reply")
        messaging.publish("out.1", end)
        outputs["out.0"].send.assert_not_called()
        outputs["out.1"].send.assert_called_once_with(end.serialize())
        callback, control_callback = Mock(), Mock()
        messaging.start_consuming(callback, control_callback)
        consumer.start_consuming.assert_called_once_with(callback, control_callback)
        messaging.broadcast_control(end)
        control_output.send.assert_called_once_with(end.serialize())
        messaging.stop_consuming()
        messaging.close()
        consumer.stop_consuming.assert_called_once()
        consumer.close.assert_called_once()
        for queue in outputs.values():
            queue.close.assert_called_once()
        control_output.close.assert_called_once()
