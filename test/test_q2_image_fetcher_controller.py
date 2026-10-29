import unittest
from unittest.mock import Mock, call, patch

from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    Q1CandidateBatchMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedInputEndMessage,
)
from q2.image_fetcher.controller import ImageFetcherController


class TestImageFetcherController(unittest.TestCase):
    def setUp(self):
        self.messaging = Mock()
        self.ack = Mock()
        self.nack = Mock()

        self.controller = ImageFetcherController(
            messaging=self.messaging,
        )

        self.batch = ImageCandidateBatchMessage(
            articles=[
                {
                    "name": "Example article",
                    "url": "https://example.com/article",
                    "image_url": "https://example.com/image.jpg",
                }
            ],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.input_end = RoutedInputEndMessage(
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        self.result = ResultMessage(
            query_id=2,
            record={
                "name": "Example article",
                "url": "https://example.com/article",
                "image": "AQID",
            },
            execution_id="execution-123",
        )

    def test_downloads_and_publishes_query2_results(self):
        with patch(
            "q2.image_fetcher.controller.build_query2_results",
            return_value=[self.result],
        ) as build_results:
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        build_results.assert_called_once_with(self.batch)
        self.assertEqual(
            self.messaging.method_calls,
            [call.publish(self.result)],
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_nothing_when_no_image_was_downloaded(self):
        with patch(
            "q2.image_fetcher.controller.build_query2_results",
            return_value=[],
        ):
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.messaging.publish.assert_not_called()
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_publishes_query_end_when_input_ends(self):
        self.controller.process_message(
            self.input_end,
            self.ack,
            self.nack,
        )

        self.messaging.publish.assert_called_once_with(
            QueryEndMessage(
                query_id=2,
                execution_id="execution-123",
            )
        )
        self.ack.assert_called_once()
        self.nack.assert_not_called()

    def test_nacks_when_publication_fails(self):
        error = RuntimeError("ResultSink unavailable")
        self.messaging.publish.side_effect = error

        with (
            patch(
                "q2.image_fetcher.controller.build_query2_results",
                return_value=[self.result],
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
            self.controller.process_message(
                self.batch,
                self.ack,
                self.nack,
            )

        self.assertIs(raised.exception, error)
        self.messaging.publish.assert_called_once()
        self.nack.assert_called_once()
        self.ack.assert_not_called()

    def test_rejects_unexpected_message(self):
        message = Q1CandidateBatchMessage(
            articles=[],
            execution_id="execution-123",
            reply_queue="results.execution-123",
        )

        with self.assertRaisesRegex(
            ValueError,
            "IMAGE_CANDIDATE_BATCH or ROUTED_INPUT_END",
        ):
            self.controller.process_message(
                message,
                self.ack,
                self.nack,
            )

        self.messaging.publish.assert_not_called()
        self.nack.assert_called_once()
        self.ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
