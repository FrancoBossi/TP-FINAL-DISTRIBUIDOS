import unittest

from common.message_protocol.message import (
    ArticleBatchMessage,
    NormalizedArticleBatchMessage,
)
from preprocessor.normalizer import normalize_batch


class TestPreprocessor(unittest.TestCase):
    def test_normalizes_article_batch(self):
        message = ArticleBatchMessage(
            articles=[
                {
                    "name": "École",
                    "url": "https://fr.wikipedia.org/wiki/École",
                    "in_language": {"identifier": "FR"},
                    "date_modified": "2020-01-02T03:04:05",
                    "abstract": "Une école est un établissement.",
                    "sections": ('[{"name":"Introduction"},{"name":"Biographie"}]'),
                    "image": {
                        "content_url": "https://example.com/image.jpg",
                        "height": 400,
                        "width": 600,
                    },
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        result = normalize_batch(message)

        expected = NormalizedArticleBatchMessage(
            articles=[
                {
                    "name": "École",
                    "url": "https://fr.wikipedia.org/wiki/École",
                    "language": "fr",
                    "modification_date": "2020-01-02T03:04:05",
                    "abstract": "Une école est un établissement.",
                    "sections": ["Introduction", "Biographie"],
                    "image_url": "https://example.com/image.jpg",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
