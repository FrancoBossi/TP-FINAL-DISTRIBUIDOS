import unittest

from common.message_protocol.message import (
    ArticleBatchMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from input_batcher.batcher import BatchAccumulator


class TestBatchAccumulator(unittest.TestCase):
    def _article(
        self,
        name: str,
        execution_id: str = "execution-123",
    ) -> RoutedArticleMessage:
        return RoutedArticleMessage(
            article={"name": name},
            execution_id=execution_id,
            reply_queue=f"results.{execution_id}",
        )

    def _end(self, execution_id: str) -> RoutedInputEndMessage:
        return RoutedInputEndMessage(
            execution_id=execution_id,
            reply_queue=f"results.{execution_id}",
        )

    def test_accumulates_and_splits_at_serialized_size_limit(self):
        first = self._article("A")
        second = self._article("B")
        third = self._article("C")

        two_article_batch = ArticleBatchMessage(
            articles=[first.article, second.article],
            execution_id=first.execution_id,
            reply_queue=first.reply_queue,
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

    def test_rejects_invalid_limit_and_oversized_article(self):
        with self.assertRaises(ValueError):
            BatchAccumulator(0)

        article = self._article("oversized")
        single_article_batch = ArticleBatchMessage(
            articles=[article.article],
            execution_id=article.execution_id,
            reply_queue=article.reply_queue,
        )
        accumulator = BatchAccumulator(len(single_article_batch.serialize()) - 1)

        with self.assertRaisesRegex(ValueError, "exceeds batch size"):
            accumulator.add_article(article)

    def test_nested_unicode_articles_match_exact_wire_size_at_boundary(self):
        first = RoutedArticleMessage(
            article={"name": 'Café "東京"', "sections": [{"text": "a\\b\n😀"}]},
            execution_id='exécution"1',
            reply_queue='results.é"1',
        )
        second = RoutedArticleMessage(
            article={"name": "Deux", "image": None, "values": [True, 3.25]},
            execution_id=first.execution_id,
            reply_queue=first.reply_queue,
        )
        expected = ArticleBatchMessage(
            [first.article, second.article], first.execution_id, first.reply_queue
        )
        for delta in (0, -1):
            with self.subTest(delta=delta):
                limit = len(expected.serialize()) + delta
                accumulator = BatchAccumulator(limit)
                self.assertIsNone(accumulator.add_article(first))
                published = accumulator.add_article(second)
                end = RoutedInputEndMessage(first.execution_id, first.reply_queue)
                remaining = accumulator.flush(end)
                if delta == 0:
                    self.assertIsNone(published)
                    self.assertEqual(remaining, expected)
                else:
                    self.assertEqual(published.articles, [first.article])
                    self.assertEqual(remaining.articles, [second.article])
                    self.assertLessEqual(len(published.serialize()), limit)
                self.assertLessEqual(len(remaining.serialize()), limit)

    def test_oversized_article_does_not_change_pending_batch(self):
        first = self._article("A")
        accumulator = BatchAccumulator(256)
        accumulator.add_article(first)
        with self.assertRaisesRegex(ValueError, "exceeds batch size"):
            accumulator.add_article(self._article("X" * 1024))
        accumulator.add_article(self._article("B"))
        batch = accumulator.flush(self._end(first.execution_id))
        self.assertEqual(batch.articles, [{"name": "A"}, {"name": "B"}])


if __name__ == "__main__":
    unittest.main()
