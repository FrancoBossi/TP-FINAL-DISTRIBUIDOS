import unittest
from unittest.mock import MagicMock, call, patch
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
                request_delay=0,
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
                request_delay=0,
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
                request_delay=0,
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
                request_delay=0,
                retry_delay=0,
            )

        self.assertEqual(result, b"image bytes")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)

    def test_default_attempts_and_delays_pace_all_requests(self):
        events = []

        def fail_request(*args, **kwargs):
            events.append("request")
            raise URLError("temporary failure")

        with (
            patch("infra.wikimedia.urlopen", side_effect=fail_request) as urlopen,
            patch(
                "infra.wikimedia.sleep", side_effect=lambda delay: events.append(delay)
            ),
        ):
            self.assertIsNone(download_image_bytes("https://example.com/image.jpg"))

        self.assertEqual(urlopen.call_count, 5)
        self.assertEqual(events, [0.5, "request", 1.0] * 4 + [0.5, "request"])

    def test_paces_requests_across_separate_images(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"image bytes"
        with (
            patch("infra.wikimedia.urlopen", return_value=response),
            patch("infra.wikimedia.sleep") as sleep,
        ):
            download_image_bytes("https://example.com/first.jpg")
            download_image_bytes("https://example.com/second.jpg")

        self.assertEqual(sleep.call_args_list, [call(0.5), call(0.5)])

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
                request_delay=0,
                max_attempts=2,
                retry_delay=0,
            )

        self.assertIsNone(result)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)
        self.assertIn("failed after 2 attempts", logs.output[-1])


if __name__ == "__main__":
    unittest.main()
