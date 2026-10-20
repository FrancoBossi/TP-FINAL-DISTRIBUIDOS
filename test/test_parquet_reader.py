import json
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import pyarrow as pa
import pyarrow.parquet as parquet
from common.article import Article
from dataset.parquet_reader import iter_articles

class TestParquetReader(unittest.TestCase):

    def test_iter_articles(self):

        rows = [
            {
                "name": "Article 1",
                "url": "https://example.com/article1",
                "in_language": {"identifier": "en"},
                "date_modified": datetime(2023, 1, 1, 12, 0, 0),
                "abstract": "This is the abstract of Article 1.",
                "sections": json.dumps([{"name": "Section 1"}, {"name": "Section 2"}]),
                "image": {"content_url": "https://example.com/image1.jpg"},
            },
            {
                "name": "Article 2",
                "url": "https://example.com/article2",
                "in_language": {"identifier": "es"},
                "date_modified": datetime(2023, 2, 1, 12, 0, 0),
                "abstract": None,
                "sections": None,
                "image": None,
            },
        ]

        expected_articles = [
            Article(
                name="Article 1",
                url="https://example.com/article1",
                language="en",
                modification_date=datetime(2023, 1, 1, 12, 0, 0),
                abstract="This is the abstract of Article 1.",
                sections=("Section 1", "Section 2"),
                image_url="https://example.com/image1.jpg",
            ),
            Article(
                name="Article 2",
                url="https://example.com/article2",
                language="es",
                modification_date=datetime(2023, 2, 1, 12, 0, 0),
                abstract=None,
                sections=(),
                image_url=None,
            ),
        ]

        with TemporaryDirectory() as temp_dir:
            parquet_file_path = Path(temp_dir) / "test.parquet"

            table = pa.Table.from_pylist(rows)
            parquet.write_table(table, parquet_file_path)

            articles = list(iter_articles(parquet_file_path, batch_size=1))

            self.assertEqual(expected_articles, articles)

if __name__ == "__main__":
    unittest.main()
    