import unittest

from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    Q2CandidateBatchMessage,
)
from q2.biography_filter.filter import filter_biography_batch


class TestQ2BiographyFilter(unittest.TestCase):
    def test_filters_biography_sections_ignoring_case(self):
        message = Q2CandidateBatchMessage(
            articles=[
                {
                    "name": "English biography",
                    "url": "https://example.com/english",
                    "sections": ["History", "BIOGRAPHY"],
                    "image_url": "https://example.com/english.jpg",
                },
                {
                    "name": "French biography",
                    "url": "https://example.com/french",
                    "sections": ["BIOGRAPHIE"],
                    "image_url": None,
                },
                {
                    "name": "Article without biography",
                    "url": "https://example.com/history",
                    "sections": ["History"],
                    "image_url": "https://example.com/history.jpg",
                },
                {
                    "name": "Article with a similar section",
                    "url": "https://example.com/similar",
                    "sections": ["Early biography"],
                    "image_url": "https://example.com/similar.jpg",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        result = filter_biography_batch(message)

        self.assertEqual(
            result,
            ImageCandidateBatchMessage(
                articles=[
                    {
                        "name": "English biography",
                        "url": "https://example.com/english",
                        "image_url": "https://example.com/english.jpg",
                    },
                    {
                        "name": "French biography",
                        "url": "https://example.com/french",
                        "image_url": None,
                    },
                ],
                execution_id="execution-123",
                reply_queue="results.execution-123",
            ),
        )


if __name__ == "__main__":
    unittest.main()
