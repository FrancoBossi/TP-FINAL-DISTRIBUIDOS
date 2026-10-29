import json
import unittest

from common.message_protocol.message import (
    AckMessage,
    ArticleBatchMessage,
    ArticleMessage,
    ErrorMessage,
    ImageCandidateBatchMessage,
    InputEndMessage,
    Message,
    NormalizedArticleBatchMessage,
    Q1CandidateBatchMessage,
    Q1Q2ArticleBatchMessage,
    Q2CandidateBatchMessage,
    Q3WordPresenceBatchMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
    TextArticleBatchMessage,
    TokenizedArticleBatchMessage,
)
from common.message_protocol.message_type import MessageType


class TestMessage(unittest.TestCase):
    def test_roundtrip_text_pipeline_contracts(self):
        articles = [
            {
                "article_id": "https://example.com/école",
                "url": "https://example.com/école",
                "language": "fr",
                "modification_date": "2023-01-01T00:00:00",
                "words": ["café", "café", "and"],
            },
            {
                "article_id": "https://example.com/empty",
                "url": "https://example.com/empty",
                "language": "en",
                "modification_date": "2019-01-01T00:00:00",
                "words": [],
            },
        ]
        presences = [
            {"word": "café", "language": "en"},
            {"word": "café", "language": "fr"},
            {"word": "and", "language": "fr"},
        ]
        for message_class, field, payload in (
            (TokenizedArticleBatchMessage, "articles", articles),
            (Q3WordPresenceBatchMessage, "presences", presences),
        ):
            for values in (payload, []):
                with self.subTest(message_class=message_class, payload=values):
                    message = message_class(
                        **{field: values},
                        execution_id="execution-123",
                        reply_queue="results.execution-123",
                    )
                    encoded = message.serialize()
                    decoded = Message.deserialize(encoded)
                    self.assertIs(type(decoded), message_class)
                    self.assertEqual(message, decoded)
                    self.assertEqual(json.loads(encoded)["type"], message.type.value)
                    if values:
                        self.assertIn("café".encode(), encoded)

    def test_text_pipeline_requires_payload_and_execution_metadata(self):
        for kind, payload_field in (
            (MessageType.TOKENIZED_ARTICLE_BATCH, "articles"),
            (MessageType.Q3_WORD_PRESENCE_BATCH, "presences"),
        ):
            fields = {
                "type": kind.value,
                payload_field: [],
                "execution_id": "execution-123",
                "reply_queue": "results.execution-123",
            }
            for missing in (payload_field, "execution_id", "reply_queue"):
                with self.subTest(kind=kind, missing=missing):
                    incomplete = {k: v for k, v in fields.items() if k != missing}
                    with self.assertRaises(ValueError):
                        Message.deserialize(json.dumps(incomplete).encode())

    def test_roundtrip_all_message_classes(self):
        article = {
            "name": "École",
            "abstract": None,
            "sections": ["Biographie"],
        }
        normalized_article = {
            "name": "École",
            "url": "https://fr.wikipedia.org/wiki/École",
            "language": "fr",
            "modification_date": "2020-01-02T03:04:05",
            "abstract": None,
            "sections": ["Biographie"],
            "image_url": None,
        }

        execution_id = "execution-123"

        messages = [
            ArticleMessage(article=article),
            RoutedArticleMessage(
                article=article,
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            ArticleBatchMessage(
                articles=[
                    article,
                    {"name": "Network", "abstract": "Computer network"},
                ],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            NormalizedArticleBatchMessage(
                articles=[normalized_article],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            InputEndMessage(),
            RoutedInputEndMessage(
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            AckMessage(),
            ErrorMessage(message="failed"),
            ResultMessage(
                query_id=3,
                record={"word": "network"},
                execution_id=execution_id,
            ),
            QueryEndMessage(
                query_id=3,
                execution_id=execution_id,
            ),
            Q1Q2ArticleBatchMessage(
                articles=[
                    {
                        "name": normalized_article["name"],
                        "url": normalized_article["url"],
                        "language": normalized_article["language"],
                        "modification_date": normalized_article["modification_date"],
                        "sections": normalized_article["sections"],
                        "image_url": normalized_article["image_url"],
                    }
                ],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            Q1CandidateBatchMessage(
                articles=[
                    {
                        "url": normalized_article["url"],
                        "language": normalized_article["language"],
                        "modification_date": normalized_article["modification_date"],
                    }
                ],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            Q2CandidateBatchMessage(
                articles=[
                    {
                        "name": normalized_article["name"],
                        "url": normalized_article["url"],
                        "sections": normalized_article["sections"],
                        "image_url": normalized_article["image_url"],
                    }
                ],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            ImageCandidateBatchMessage(
                articles=[
                    {
                        "name": normalized_article["name"],
                        "url": normalized_article["url"],
                        "image_url": normalized_article["image_url"],
                    }
                ],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
            TextArticleBatchMessage(
                articles=[
                    {
                        "article_id": normalized_article["url"],
                        "url": normalized_article["url"],
                        "language": normalized_article["language"],
                        "modification_date": normalized_article["modification_date"],
                        "abstract": normalized_article["abstract"],
                    }
                ],
                execution_id=execution_id,
                reply_queue=f"results.{execution_id}",
            ),
        ]

        for message in messages:
            with self.subTest(message=message):
                decoded = Message.deserialize(message.serialize())

                self.assertIs(type(decoded), type(message))
                self.assertEqual(decoded, message)
                self.assertIsInstance(decoded.type, MessageType)
                self.assertEqual(
                    json.loads(message.serialize())["type"],
                    message.type.value,
                )

        self.assertEqual(messages[0].article, article)
        self.assertIn("École".encode(), messages[0].serialize())

        routed_fields = json.loads(messages[1].serialize())
        self.assertEqual(
            routed_fields["execution_id"],
            execution_id,
        )

    def test_rejects_invalid_envelopes_and_missing_or_extra_fields(self):
        for fields in (
            [],
            {},
            {"type": 1},
            {"type": "UNKNOWN"},
            {
                "type": "RESULT",
                "query_id": 1,
                "execution_id": "execution-123",
            },
            {"type": "ARTICLE"},
            {"type": "ACK", "record": {}},
            {
                "type": "INPUT_END",
                "execution_id": "execution-123",
            },
        ):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                Message.deserialize(json.dumps(fields).encode())

    def test_required_fields_are_required_at_construction(self):
        with self.assertRaises(TypeError):
            ArticleMessage()

    def test_rejects_invalid_json(self):
        with self.assertRaises(ValueError):
            Message.deserialize(b"not JSON")

    def test_rejects_non_json_payloads(self):
        for value in (b"raw image", float("nan")):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                ArticleMessage({"value": value}).serialize()


if __name__ == "__main__":
    unittest.main()
