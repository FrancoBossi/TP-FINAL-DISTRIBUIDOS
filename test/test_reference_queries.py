import unittest
from datetime import datetime
from common.article import Article
from common.query_results import (Q1Result,Q2Result,Q4Result,Q5Result)
from reference.query1 import process_query1
from reference.query2 import process_query2
from reference.query3 import process_query3
from reference.query4 import process_query4
from reference.query5 import (calculate_query5_average,process_query5)


class TestReferenceQueries(unittest.TestCase):

    def setUp(self):

        self.articles = (
            Article(
                name="Old English",
                url="https://example.com/old-en",
                language="en",
                modification_date=datetime(2020, 1, 1),
                abstract="Network system system and",
                sections=("Biography",),
                image_url=(
                    "data:application/octet-stream;base64,"
                    "AQID"
                ),
            ),
            Article(
                name="Old French",
                url="https://example.com/old-fr",
                language="fr",
                modification_date=datetime(2019, 1, 1),
                abstract="Network système de",
                sections=("BIOGRAPHIE",),
                image_url=None,
            ),
            Article(
                name="New English",
                url="https://example.com/new-en",
                language="en",
                modification_date=datetime(2021, 1, 1),
                abstract="Network computer database",
                sections=(),
                image_url=None,
            ),
            Article(
                name="New French",
                url="https://example.com/new-fr",
                language="fr",
                modification_date=datetime(2022, 1, 1),
                abstract="Network ordinateur",
                sections=(),
                image_url=None,
            ),
        )

    def test_reference_queries(self):

        query1_results = []

        for article in self.articles:
            result = process_query1(article)

            if result is not None:
                query1_results.append(result)

        self.assertEqual([Q1Result(url="https://example.com/old-en",modification_date=datetime(2020, 1, 1),)], query1_results)

        query2_results = []

        for article in self.articles:
            result = process_query2(article)

            if result is not None:
                query2_results.append(result)

        self.assertEqual([Q2Result(name="Old English",url="https://example.com/old-en",image=b"\x01\x02\x03",)], query2_results)

        query3_result = process_query3(self.articles)

        self.assertEqual({"network"},query3_result)

        query4_result = process_query4(self.articles)

        self.assertEqual(
            (
                Q4Result(word="network", count=4),
                Q4Result(word="computer", count=1),
                Q4Result(word="database", count=1),
                Q4Result(word="ordinateur", count=1),
                Q4Result(word="system", count=1),
                Q4Result(word="système", count=1),
            ),
            query4_result,
        )

        query5_average = calculate_query5_average(self.articles)

        self.assertEqual(2.0,query5_average)

        query5_result = process_query5(self.articles,query5_average)

        self.assertEqual((Q5Result(url="https://example.com/new-en", word_count=3),), query5_result)


if __name__ == "__main__":
    unittest.main()