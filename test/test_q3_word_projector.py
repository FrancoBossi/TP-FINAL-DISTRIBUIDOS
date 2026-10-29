import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    AckMessage,
    RoutedInputEndMessage,
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

    def test_controller_publishes_before_ack_and_forwards_end_to_empty_partitions(self):
        messaging, ack, nack = Mock(), Mock(), Mock()
        events = Mock()
        events.attach_mock(messaging, "messaging")
        events.attach_mock(ack, "ack")
        controller = Q3WordProjectorController(messaging, self.router)
        controller.process_message(self.batch, ack, nack)
        expected = [
            call.messaging.publish(q, b)
            for q, b in project_batch(self.batch, self.router).items()
        ]
        self.assertEqual(expected + [call.ack()], events.mock_calls)
        messaging.reset_mock()
        end = RoutedInputEndMessage("run", "results.run")
        controller.process_message(end, ack, nack)
        self.assertEqual(
            [call.publish(f"q3_word_aggregator.input.{i}", end) for i in range(7)],
            messaging.mock_calls,
        )
        nack.assert_not_called()

    def test_failure_and_unexpected_message_are_not_acked(self):
        for message in (
            self.batch,
            RoutedInputEndMessage("run", "reply"),
            AckMessage(),
        ):
            with self.subTest(message=message):
                messaging, ack, nack = Mock(), Mock(), Mock()
                messaging.publish.side_effect = RuntimeError("unavailable")
                with self.assertRaises((RuntimeError, ValueError)):
                    Q3WordProjectorController(messaging, self.router).process_message(
                        message, ack, nack
                    )
                ack.assert_not_called()
                nack.assert_called_once_with()

    def test_messaging_routes_serialized_messages_and_deserializes_input(self):
        input_queue = Mock()
        outputs = {f"out.{i}": Mock() for i in range(2)}
        messaging = Q3WordProjectorMessaging(input_queue, outputs)
        end = RoutedInputEndMessage("run", "reply")
        messaging.publish("out.1", end)
        outputs["out.0"].send.assert_not_called()
        outputs["out.1"].send.assert_called_once_with(end.serialize())
        callback, ack, nack = Mock(), Mock(), Mock()
        messaging.start_consuming(callback)
        input_queue.start_consuming.call_args.args[0](self.batch.serialize(), ack, nack)
        callback.assert_called_once_with(self.batch, ack, nack)
        messaging.stop_consuming()
        messaging.close()
        input_queue.stop_consuming.assert_called_once()
        input_queue.close.assert_called_once()
        for queue in outputs.values():
            queue.close.assert_called_once()
