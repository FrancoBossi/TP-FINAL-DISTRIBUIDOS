import unittest

from common.message_identity import build_message_id
from common.message_protocol.message import (
    ArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from input_batcher.batcher import BatchAccumulator


class TestBatchAccumulator(unittest.TestCase):
    def _batch_id(
        self,
        execution_id: str,
        *messages: RoutedArticleMessage,
        partition_id: int = 0,
    ) -> str:
        return build_message_id(
            execution_id=execution_id,
            producer_id=f"input_batcher:{partition_id}",
            operation="article_batch",
            identity={
                "source_message_ids": [message.message_id for message in messages]
            },
        )

    def _article(
        self,
        name: str,
        execution_id: str = "execution-123",
    ) -> RoutedArticleMessage:
        return RoutedArticleMessage(
            article={"name": name},
            execution_id=execution_id,
            reply_queue=f"results.{execution_id}",
            message_id=f"m1:{execution_id}:{name}",
        )

    def _end(self, execution_id: str) -> RoutedInputEndMessage:
        return RoutedInputEndMessage(
            execution_id=execution_id,
            reply_queue=f"results.{execution_id}",
            message_id=f"m1:{execution_id}:end",
        )

    def test_accumulates_and_splits_at_serialized_size_limit(self):
        first = self._article("A")
        second = self._article("B")
        third = self._article("C")

        two_article_batch = ArticleBatchMessage(
            articles=[first.article, second.article],
            execution_id=first.execution_id,
            reply_queue=first.reply_queue,
            message_id=self._batch_id(first.execution_id, first, second),
        )
        max_batch_bytes = len(two_article_batch.serialize())

        accumulator = BatchAccumulator(max_batch_bytes)

        self.assertIsNone(accumulator.add_article(first))
        self.assertIsNone(accumulator.add_article(second))

        published = accumulator.add_article(third)

        self.assertEqual(
            published,
            two_article_batch,
        )
        self.assertLessEqual(
            len(published.serialize()),
            max_batch_bytes,
        )

        remaining = accumulator.flush(self._end(first.execution_id))

        self.assertEqual(
            remaining.articles,
            [third.article],
        )
        self.assertLessEqual(
            len(remaining.serialize()),
            max_batch_bytes,
        )

    def test_keeps_interleaved_executions_separate(self):
        first_execution = self._article("A", "execution-1")
        second_execution = self._article("B", "execution-2")

        accumulator = BatchAccumulator(1024)

        accumulator.add_article(first_execution)
        accumulator.add_article(second_execution)

        first_batch = accumulator.flush(self._end("execution-1"))
        second_batch = accumulator.flush(self._end("execution-2"))

        self.assertEqual(first_batch.articles, [first_execution.article])
        self.assertEqual(second_batch.articles, [second_execution.article])
        self.assertIsNone(accumulator.flush(self._end("execution-1")))

    def test_rejects_invalid_limit_and_preserves_oversized_article(self):
        with self.assertRaises(ValueError):
            BatchAccumulator(0)

        article = self._article("oversized")
        single_article_batch = ArticleBatchMessage(
            articles=[article.article],
            execution_id=article.execution_id,
            reply_queue=article.reply_queue,
            message_id=self._batch_id(article.execution_id, article),
        )
        accumulator = BatchAccumulator(len(single_article_batch.serialize()) - 1)

        self.assertIsNone(accumulator.add_article(article))
        self.assertEqual(
            accumulator.flush(self._end(article.execution_id)), single_article_batch
        )

    def test_nested_unicode_articles_match_exact_wire_size_at_boundary(self):
        first = RoutedArticleMessage(
            article={"name": 'Café "東京"', "sections": [{"text": "a\\b\n😀"}]},
            execution_id='exécution"1',
            reply_queue='results.é"1',
            message_id="m1:first",
        )
        second = RoutedArticleMessage(
            article={"name": "Deux", "image": None, "values": [True, 3.25]},
            execution_id=first.execution_id,
            reply_queue=first.reply_queue,
            message_id="m1:second",
        )
        expected = ArticleBatchMessage(
            [first.article, second.article],
            first.execution_id,
            first.reply_queue,
            self._batch_id(first.execution_id, first, second),
        )
        for delta in (0, -1):
            with self.subTest(delta=delta):
                limit = len(expected.serialize()) + delta
                accumulator = BatchAccumulator(limit)
                self.assertIsNone(accumulator.add_article(first))
                published = accumulator.add_article(second)
                end = RoutedInputEndMessage(
                    first.execution_id,
                    first.reply_queue,
                    "m1:input-end",
                )
                remaining = accumulator.flush(end)
                if delta == 0:
                    self.assertIsNone(published)
                    self.assertEqual(remaining, expected)
                else:
                    self.assertEqual(published.articles, [first.article])
                    self.assertEqual(remaining.articles, [second.article])
                    self.assertLessEqual(len(published.serialize()), limit)
                self.assertLessEqual(len(remaining.serialize()), limit)

    def test_batch_identity_depends_on_order_and_partition(self):
        first = self._article("A")
        second = self._article("B")

        first_order = BatchAccumulator(4096, partition_id=0)
        first_order.add_article(first)
        first_order.add_article(second)
        first_batch = first_order.flush(self._end(first.execution_id))

        second_order = BatchAccumulator(4096, partition_id=0)
        second_order.add_article(second)
        second_order.add_article(first)
        second_batch = second_order.flush(self._end(first.execution_id))

        another_partition = BatchAccumulator(4096, partition_id=1)
        another_partition.add_article(first)
        another_partition.add_article(second)
        partitioned_batch = another_partition.flush(self._end(first.execution_id))

        self.assertNotEqual(first_batch.message_id, second_batch.message_id)
        self.assertNotEqual(first_batch.message_id, partitioned_batch.message_id)

    def test_oversized_articles_are_isolated_without_losing_pending_articles(self):
        first = self._article("A")
        oversized = self._article("X" * 1024)
        another_oversized = self._article("Y" * 1024)
        last = self._article("B")
        accumulator = BatchAccumulator(256)
        self.assertIsNone(accumulator.add_article(first))
        batches = [
            accumulator.add_article(oversized),
            accumulator.add_article(another_oversized),
            accumulator.add_article(last),
            accumulator.flush(self._end(first.execution_id)),
        ]
        self.assertEqual(
            [batch.articles for batch in batches],
            [
                [first.article],
                [oversized.article],
                [another_oversized.article],
                [last.article],
            ],
        )
        self.assertIsNone(accumulator.flush(self._end(first.execution_id)))


if __name__ == "__main__":
    unittest.main()
