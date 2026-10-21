import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from reference.compare import compare_results


class TestResultComparator(unittest.TestCase):
    def _write_lines(self, path: Path, lines: tuple[str, ...]) -> None:

        content = "\n".join(lines) + "\n"
        path.write_text(content, encoding="utf-8")

    def test_ignores_record_and_key_order(self):

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            expected_file = temp_path / "expected.jsonl"
            actual_file = temp_path / "actual.jsonl"

            expected_lines = (
                '{"url": "https://example.com/old-en", "modification_date": "2020-01-01T00:00:00"}',
                '{"url": "https://example.com/old-fr", "modification_date": "2019-01-01T00:00:00"}',
            )

            actual_lines = (
                '{"modification_date": "2019-01-01T00:00:00", "url": "https://example.com/old-fr"}',
                '{"modification_date": "2020-01-01T00:00:00", "url": "https://example.com/old-en"}',
            )

            self._write_lines(expected_file, expected_lines)
            self._write_lines(actual_file, actual_lines)

            missing_count, extra_count = compare_results(
                expected_file,
                actual_file,
            )

            self.assertEqual(missing_count, 0)
            self.assertEqual(extra_count, 0)

    def test_detects_missing_extra_and_duplicate_records(self):

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            expected_file = temp_path / "expected.jsonl"
            actual_file = temp_path / "actual.jsonl"

            expected_lines = (
                '{"word": "hola"}',
                '{"word": "hola"}',
                '{"word": "chau"}',
            )

            actual_lines = (
                '{"word": "hola"}',
                '{"word": "bien"}',
            )

            self._write_lines(expected_file, expected_lines)
            self._write_lines(actual_file, actual_lines)

            missing_count, extra_count = compare_results(
                expected_file,
                actual_file,
            )

            self.assertEqual(missing_count, 2)
            self.assertEqual(extra_count, 1)


if __name__ == "__main__":
    unittest.main()
