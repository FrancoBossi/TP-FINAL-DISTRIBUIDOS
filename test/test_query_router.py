import unittest

from common.message_protocol.message import (
    NormalizedArticleBatchMessage,
    Q1Q2ArticleBatchMessage,
    TextArticleBatchMessage,
)
from query_router.router import route_batch


class TestQueryRouter(unittest.TestCase):
    def test_projects_every_article_to_both_branches(self):
        message = NormalizedArticleBatchMessage(
            articles=[
                {
                    "name": "English article",
                    "url": "https://en.wikipedia.org/wiki/English_article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "abstract": "An English abstract.",
                    "sections": ["Biography"],
                    "image_url": "https://example.com/english.jpg",
                },
                {
                    "name": "Article français",
                    "url": "https://fr.wikipedia.org/wiki/Article_français",
                    "language": "fr",
                    "modification_date": "2024-01-01T00:00:00",
                    "abstract": None,
                    "sections": [],
                    "image_url": None,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        q1_q2_batch, text_batch = route_batch(message)

        expected_q1_q2 = Q1Q2ArticleBatchMessage(
            articles=[
                {
                    "name": "English article",
                    "url": "https://en.wikipedia.org/wiki/English_article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "sections": ["Biography"],
                    "image_url": "https://example.com/english.jpg",
                },
                {
                    "name": "Article français",
                    "url": "https://fr.wikipedia.org/wiki/Article_français",
                    "language": "fr",
                    "modification_date": "2024-01-01T00:00:00",
                    "sections": [],
                    "image_url": None,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        expected_text = TextArticleBatchMessage(
            articles=[
                {
                    "article_id": ("https://en.wikipedia.org/wiki/English_article"),
                    "url": "https://en.wikipedia.org/wiki/English_article",
                    "language": "en",
                    "modification_date": "2020-01-01T00:00:00",
                    "abstract": "An English abstract.",
                },
                {
                    "article_id": ("https://fr.wikipedia.org/wiki/Article_français"),
                    "url": "https://fr.wikipedia.org/wiki/Article_français",
                    "language": "fr",
                    "modification_date": "2024-01-01T00:00:00",
                    "abstract": None,
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.assertEqual(q1_q2_batch, expected_q1_q2)
        self.assertEqual(text_batch, expected_text)


if __name__ == "__main__":
    unittest.main()
