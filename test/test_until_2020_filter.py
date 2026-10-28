import unittest

from common.message_protocol.message import (
    Q1CandidateBatchMessage,
    Q1Q2ArticleBatchMessage,
    Q2CandidateBatchMessage,
)
from until_2020_filter.filter import filter_batch


class TestUntil2020Filter(unittest.TestCase):
    def test_filters_by_year_and_projects_both_branches(self):
        message = Q1Q2ArticleBatchMessage(
            articles=[
                {
                    "name": "Article from 2019",
                    "url": "https://example.com/2019",
                    "language": "en",
                    "modification_date": "2019-06-15T10:30:00",
                    "sections": ["History"],
                    "image_url": None,
                },
                {
                    "name": "Article from 2020",
                    "url": "https://example.com/2020",
                    "language": "fr",
                    "modification_date": "2020-12-31T23:59:59",
                    "sections": ["Biographie"],
                    "image_url": "https://example.com/image.jpg",
                },
                {
                    "name": "Article from 2021",
                    "url": "https://example.com/2021",
                    "language": "en",
                    "modification_date": "2021-01-01T00:00:00",
                    "sections": ["Biography"],
                    "image_url": "https://example.com/other-image.jpg",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        q1_batch, q2_batch = filter_batch(message)

        expected_q1_batch = Q1CandidateBatchMessage(
            articles=[
                {
                    "url": "https://example.com/2019",
                    "language": "en",
                    "modification_date": "2019-06-15T10:30:00",
                },
                {
                    "url": "https://example.com/2020",
                    "language": "fr",
                    "modification_date": "2020-12-31T23:59:59",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        expected_q2_batch = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "Article from 2019",
                    "url": "https://example.com/2019",
                    "sections": ["History"],
                    "image_url": None,
                },
                {
                    "name": "Article from 2020",
                    "url": "https://example.com/2020",
                    "sections": ["Biographie"],
                    "image_url": "https://example.com/image.jpg",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.assertEqual(q1_batch, expected_q1_batch)
        self.assertEqual(q2_batch, expected_q2_batch)


if __name__ == "__main__":
    unittest.main()
