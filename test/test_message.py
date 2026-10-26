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
        article = {
            "name": "École",
            "abstract": None,
            "sections": ["Biographie"],
        }
        execution_id = "execution-123"

        messages = [
            ArticleMessage(article=article),
            RoutedArticleMessage(
                article=article,
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
        ]

        for message in messages:
            with self.subTest(message=message):
                decoded = Message.deserialize(message.serialize())

                self.assertIs(type(decoded), type(message))
                self.assertEqual(decoded, message)
                self.assertIsInstance(decoded.type, MessageType)

        self.assertEqual(messages[0].article, article)
        self.assertIn("École".encode(), messages[0].serialize())

    def test_binary_values_are_preserved_without_base64(self):
        message = ResultMessage(
            query_id=2,
            record={"name": "article", "image": b"\x00\x01\xff"},
            execution_id="execution-123",
        )
        self.assertEqual(Message.deserialize(message.serialize()), message)

    def test_required_fields_are_required_at_construction(self):
        with self.assertRaises(TypeError):
            ArticleMessage()

    def test_rejects_invalid_binary_messages(self):
        with self.assertRaises(ValueError):
            Message.deserialize(b"not binary")
        for body in (
            b"",
            b"\x02\x03",
            b"\x01\xff",
            b"\x03\x00",
        ):
            with self.subTest(body=body), self.assertRaises(ValueError):
                Message.deserialize(body)

    def test_rejects_unsupported_values(self):
        with self.assertRaises(TypeError):
            ArticleMessage({"value": object()}).serialize()
        with self.assertRaises(ValueError):
            ArticleMessage({"value": float("nan")}).serialize()


if __name__ == "__main__":
    unittest.main()
