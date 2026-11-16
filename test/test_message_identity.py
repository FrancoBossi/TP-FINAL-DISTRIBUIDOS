import re
import unittest

from common.message_identity import build_message_id, build_result_id


class MessageIdentityTest(unittest.TestCase):
    def test_message_id_is_stable_for_equivalent_mappings(self):
        first = build_message_id(
            execution_id="execution-1",
            producer_id="preprocessor:0",
            operation="normalized_batch",
            identity={
                "input_message_id": "m1:input",
                "route": {"partition": 2, "queue": "query-router"},
            },
        )
        second = build_message_id(
            execution_id="execution-1",
            producer_id="preprocessor:0",
            operation="normalized_batch",
            identity={
                "route": {"queue": "query-router", "partition": 2},
                "input_message_id": "m1:input",
            },
        )

        self.assertEqual(first, second)
        self.assertRegex(first, re.compile(r"^m1:[0-9a-f]{64}$"))

    def test_message_id_changes_when_logical_occurrence_changes(self):
        first = build_message_id(
            execution_id="execution-1",
            producer_id="gateway",
            operation="article",
            identity={"sequence": 10},
        )
        second = build_message_id(
            execution_id="execution-1",
            producer_id="gateway",
            operation="article",
            identity={"sequence": 11},
        )

        self.assertNotEqual(first, second)

    def test_unicode_identity_is_supported(self):
        first = build_message_id(
            execution_id="ejecución",
            producer_id="etapa:0",
            operation="artículo",
            identity={"palabra": "información"},
        )
        second = build_message_id(
            execution_id="ejecución",
            producer_id="etapa:0",
            operation="artículo",
            identity={"palabra": "información"},
        )

        self.assertEqual(first, second)

    def test_result_kind_separates_record_from_query_end(self):
        record = build_result_id(
            execution_id="execution-1",
            query_id=3,
            result_kind="record",
            identity={"word": "end"},
        )
        query_end = build_result_id(
            execution_id="execution-1",
            query_id=3,
            result_kind="end",
            identity={},
        )

        self.assertNotEqual(record, query_end)
        self.assertRegex(record, re.compile(r"^r1:[0-9a-f]{64}$"))

    def test_result_id_changes_between_queries(self):
        query_one = build_result_id(
            execution_id="execution-1",
            query_id=1,
            result_kind="record",
            identity={"url": "https://example.test/article"},
        )
        query_two = build_result_id(
            execution_id="execution-1",
            query_id=2,
            result_kind="record",
            identity={"url": "https://example.test/article"},
        )

        self.assertNotEqual(query_one, query_two)

    def test_rejects_invalid_required_fields(self):
        invalid_message_arguments = (
            {"execution_id": "", "producer_id": "gateway", "operation": "article"},
            {
                "execution_id": "execution-1",
                "producer_id": "",
                "operation": "article",
            },
            {
                "execution_id": "execution-1",
                "producer_id": "gateway",
                "operation": "",
            },
        )
        for arguments in invalid_message_arguments:
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                build_message_id(**arguments, identity={})

    def test_rejects_invalid_query_id(self):
        for query_id in (True, 0, 6, "1"):
            with self.subTest(query_id=query_id), self.assertRaises(ValueError):
                build_result_id(
                    execution_id="execution-1",
                    query_id=query_id,
                    result_kind="record",
                    identity={"url": "https://example.test/article"},
                )

    def test_rejects_non_json_identity(self):
        with self.assertRaises(ValueError):
            build_message_id(
                execution_id="execution-1",
                producer_id="gateway",
                operation="article",
                identity={"sequence": object()},
            )


if __name__ == "__main__":
    unittest.main()
