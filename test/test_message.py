import json
import unittest

from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
    InputEndMessage,
    Message,
    QueryEndMessage,
    ResultMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from common.message_protocol.message_type import MessageType


class TestMessage(unittest.TestCase):
    def test_roundtrip_all_message_classes(self):
        article = {"name": "École", "abstract": None, "sections": ["Biographie"]}
        messages = [
            ArticleMessage(article),
            RoutedArticleMessage(article, "run", "results.run"),
            InputEndMessage(),
            RoutedInputEndMessage("run", "results.run"),
            AckMessage(),
            ErrorMessage("failed"),
            ResultMessage(3, {"word": "network"}, "run"),
            QueryEndMessage(3, "run"),
        ]
        for message in messages:
            with self.subTest(message=message):
                decoded = Message.deserialize(message.serialize())
                self.assertIs(type(decoded), type(message))
                self.assertEqual(decoded, message)
                self.assertIsInstance(decoded.type, MessageType)
                self.assertEqual(
                    json.loads(message.serialize())["type"], message.type.value
                )
        self.assertEqual(messages[0].article, article)
        self.assertIn("École".encode(), messages[0].serialize())

    def test_rejects_invalid_envelopes_and_missing_or_extra_fields(self):
        for fields in (
            [],
            {},
            {"type": 1},
            {"type": "UNKNOWN"},
            {"type": "RESULT", "query_id": 1, "run_id": "run"},
            {"type": "ARTICLE"},
            {"type": "ACK", "record": {}},
            {"type": "INPUT_END", "run_id": "run"},
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
