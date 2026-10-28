import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

from infra.wikimedia import download_image_bytes


class TestWikimedia(unittest.TestCase):
    def test_downloads_image_bytes(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"image bytes"

        with patch(
            "infra.wikimedia.urlopen",
            return_value=response,
        ) as urlopen:
            result = download_image_bytes(
                "https://example.com/image.jpg",
            )

        self.assertEqual(result, b"image bytes")
        urlopen.assert_called_once()

    def test_does_not_retry_http_404(self):
        error = HTTPError(
            url="https://example.com/missing.jpg",
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=None,
        )

        with (
            patch(
                "infra.wikimedia.urlopen",
                side_effect=error,
            ) as urlopen,
            patch("infra.wikimedia.sleep") as sleep,
            self.assertLogs("infra.wikimedia", level="WARNING") as logs,
        ):
            result = download_image_bytes(
                "https://example.com/missing.jpg",
            )

        self.assertIsNone(result)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()
        self.assertIn("HTTP 404", logs.output[0])

    def test_retries_transient_network_error(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"image bytes"

        with (
            patch(
                "infra.wikimedia.urlopen",
                side_effect=[
                    URLError("temporary failure"),
                    response,
                ],
            ) as urlopen,
            patch("infra.wikimedia.sleep") as sleep,
        ):
            result = download_image_bytes(
                "https://example.com/image.jpg",
                retry_delay=0,
            )

        self.assertEqual(result, b"image bytes")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)

    def test_retries_transient_http_error(self):
        error = HTTPError(
            url="https://example.com/image.jpg",
            code=503,
            msg="Service Unavailable",
            hdrs=None,
            fp=None,
        )
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"image bytes"

        with (
            patch(
                "infra.wikimedia.urlopen",
                side_effect=[error, response],
            ) as urlopen,
            patch("infra.wikimedia.sleep") as sleep,
        ):
            result = download_image_bytes(
                "https://example.com/image.jpg",
                retry_delay=0,
            )

        self.assertEqual(result, b"image bytes")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)

    def test_returns_none_after_exhausting_retries(self):
        with (
            patch(
                "infra.wikimedia.urlopen",
                side_effect=URLError("temporary failure"),
            ) as urlopen,
            patch("infra.wikimedia.sleep") as sleep,
            self.assertLogs("infra.wikimedia", level="WARNING") as logs,
        ):
            result = download_image_bytes(
                "https://example.com/image.jpg",
                max_attempts=2,
                retry_delay=0,
            )

        self.assertIsNone(result)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)
        self.assertIn("failed after 2 attempts", logs.output[-1])


if __name__ == "__main__":
    unittest.main()
