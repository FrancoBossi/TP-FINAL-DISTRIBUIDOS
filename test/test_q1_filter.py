import unittest

from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    ResultMessage,
)
from q1.filter import build_query1_results


class TestQ1EnglishFilter(unittest.TestCase):
    def test_builds_results_only_for_english_articles(self):
        message = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/english",
                    "language": "en",
                    "modification_date": "2020-12-31T23:59:59",
                },
                {
                    "url": "https://example.com/french",
                    "language": "fr",
                    "modification_date": "2019-01-01T00:00:00",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        results = build_query1_results(message)

        self.assertEqual(
            results,
            [
                ResultMessage(
                    query_id=1,
                    record={
                        "url": "https://example.com/english",
                        "modification_date": "2020-12-31T23:59:59",
                    },
                    execution_id="execution-123",
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
