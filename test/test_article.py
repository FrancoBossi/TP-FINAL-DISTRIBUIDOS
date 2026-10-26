import unittest
from datetime import datetime

from common.article import (
    Article,
    article_from_raw_record,
    article_to_normalized_record,
)


class TestArticleNormalization(unittest.TestCase):
    def test_normalizes_complete_raw_record(self):
        raw_record = {
            "name": "École",
            "url": "https://fr.wikipedia.org/wiki/École",
            "in_language": {"identifier": "FR"},
            "date_modified": "2020-01-02T03:04:05",
            "abstract": "Une école est un établissement.",
            "sections": (
                '[{"name":"Introduction"},{"name":"Biographie"},{"type":"paragraph"}]'
            ),
            "image": {
                "content_url": "https://example.com/image.jpg",
                "height": 400,
                "width": 600,
            },
        }

        article = article_from_raw_record(raw_record)

        self.assertEqual(
            article,
            Article(
                name="École",
                url="https://fr.wikipedia.org/wiki/École",
                language="fr",
                modification_date=datetime(2020, 1, 2, 3, 4, 5),
                abstract="Une école est un établissement.",
                sections=("Introduction", "Biographie"),
                image_url="https://example.com/image.jpg",
            ),
        )
        self.assertEqual(
            article_to_normalized_record(article),
            {
                "name": "École",
                "url": "https://fr.wikipedia.org/wiki/École",
                "language": "fr",
                "modification_date": "2020-01-02T03:04:05",
                "abstract": "Une école est un établissement.",
                "sections": ["Introduction", "Biographie"],
                "image_url": "https://example.com/image.jpg",
            },
        )

    def test_normalizes_missing_optional_values(self):
        raw_record = {
            "name": "Minimal article",
            "url": "https://example.com/minimal",
            "in_language": {"identifier": "en"},
            "date_modified": "2024-01-01T00:00:00",
            "abstract": None,
            "sections": None,
            "image": None,
        }

        article = article_from_raw_record(raw_record)

        self.assertIsNone(article.abstract)
        self.assertEqual(article.sections, ())
        self.assertIsNone(article.image_url)

    def test_rejects_invalid_raw_record(self):
        raw_record = {
            "name": "Invalid article",
            "url": "https://example.com/invalid",
            "in_language": None,
            "date_modified": "not-a-date",
            "abstract": None,
            "sections": "not-json",
            "image": None,
        }

        with self.assertRaises(ValueError):
            article_from_raw_record(raw_record)


if __name__ == "__main__":
    unittest.main()
