import copy
import unittest

from common.message_protocol.message import (
    TextArticleBatchMessage,
    TokenizedArticleBatchMessage,
)
from word_extractor.extractor import tokenize_batch


class TestWordExtractor(unittest.TestCase):
    def test_normalizes_tokens_and_preserves_query_specific_words(self):
        message = TextArticleBatchMessage(
            articles=[
                {
                    "article_id": "article-1",
                    "url": "https://example.com/article-1",
                    "language": "fr",
                    "modification_date": "2023-01-01T00:00:00",
                    "abstract": "CAFÉ, café! And le réseau 123 ٤٥ robot2.",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        original = copy.deepcopy(message)

        result = tokenize_batch(message)

        self.assertEqual(
            TokenizedArticleBatchMessage(
                articles=[
                    {
                        "article_id": "article-1",
                        "url": "https://example.com/article-1",
                        "language": "fr",
                        "modification_date": "2023-01-01T00:00:00",
                        "words": ["café", "café", "and", "le", "réseau", "robot2"],
                    }
                ],
                execution_id="execution-123",
                reply_queue="results.execution-123",
            ),
            result,
        )
        self.assertEqual(original, message)

    def test_keeps_all_languages_dates_and_empty_abstracts(self):
        cases = (
            ("en", "2019-01-01T00:00:00", None, []),
            ("fr", "2020-12-31T23:59:59", "", []),
            ("es", "2024-01-01T00:00:00", "Hola hola", ["hola", "hola"]),
            ("de", "2021-01-01T00:00:00", "  !!! 123 ", []),
        )
        articles = [
            {
                "article_id": f"article-{index}",
                "url": f"https://example.com/{index}",
                "language": language,
                "modification_date": date,
                "abstract": abstract,
            }
            for index, (language, date, abstract, _) in enumerate(cases)
        ]
        result = tokenize_batch(
            TextArticleBatchMessage(articles, "execution-123", "results.execution-123")
        )

        self.assertEqual(len(articles), len(result.articles))
        for source, output, (_, _, _, expected_words) in zip(
            articles, result.articles, cases, strict=True
        ):
            with self.subTest(article_id=source["article_id"]):
                self.assertEqual(
                    {k: v for k, v in source.items() if k != "abstract"},
                    {k: v for k, v in output.items() if k != "words"},
                )
                self.assertEqual(expected_words, output["words"])

    def test_empty_batch_preserves_execution_metadata(self):
        self.assertEqual(
            TokenizedArticleBatchMessage([], "execution-123", "results.execution-123"),
            tokenize_batch(
                TextArticleBatchMessage([], "execution-123", "results.execution-123")
            ),
        )


if __name__ == "__main__":
    unittest.main()
