import unittest
from unittest.mock import Mock, call

from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    ResultMessage,
)
from q2.image_fetcher.processor import build_query2_results


class TestQ2ImageFetcher(unittest.TestCase):
    def test_builds_results_and_skips_missing_or_failed_images(self):
        message = ImageCandidateBatchMessage(
            articles=[
                {
                    "name": "Downloaded image",
                    "url": "https://example.com/downloaded",
                    "image_url": "https://example.com/downloaded.jpg",
                },
                {
                    "name": "Missing URL",
                    "url": "https://example.com/missing-url",
                    "image_url": None,
                },
                {
                    "name": "Failed download",
                    "url": "https://example.com/failed",
                    "image_url": "https://example.com/failed.jpg",
                },
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )
        download_image = Mock(
            side_effect=[
                b"\x01\x02\x03",
                None,
            ]
        )

        with self.assertLogs(
            "q2.image_fetcher.processor",
            level="WARNING",
        ) as logs:
            results = build_query2_results(
                message,
                download_image=download_image,
            )

        self.assertEqual(
            results,
            [
                ResultMessage(
                    query_id=2,
                    record={
                        "name": "Downloaded image",
                        "url": "https://example.com/downloaded",
                        "image": "AQID",
                    },
                    execution_id="execution-123",
                )
            ],
        )
        self.assertEqual(
            download_image.call_args_list,
            [
                call("https://example.com/downloaded.jpg"),
                call("https://example.com/failed.jpg"),
            ],
        )
        self.assertEqual(len(logs.output), 2)


if __name__ == "__main__":
    unittest.main()
