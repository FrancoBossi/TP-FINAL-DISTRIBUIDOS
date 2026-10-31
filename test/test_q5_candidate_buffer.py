import tempfile
import unittest
from pathlib import Path

from q5.comparator.buffer import CandidateBuffer


class TestCandidateBuffer(unittest.TestCase):
    def test_appends_and_reads_candidates_across_buffer_instances(self):
        with tempfile.TemporaryDirectory() as directory:
            buffer = CandidateBuffer(directory)
            buffer.append(
                "execution-123",
                [
                    {
                        "article_id": "article-1",
                        "url": "https://example.com/école",
                        "word_count": 3,
                    }
                ],
            )
            buffer.append(
                "execution-123",
                [
                    {
                        "article_id": "article-2",
                        "url": "https://example.com/network",
                        "word_count": 5,
                    }
                ],
            )

            reopened_buffer = CandidateBuffer(directory)

            self.assertEqual(
                list(reopened_buffer.iterate("execution-123")),
                [
                    {
                        "article_id": "article-1",
                        "url": "https://example.com/école",
                        "word_count": 3,
                    },
                    {
                        "article_id": "article-2",
                        "url": "https://example.com/network",
                        "word_count": 5,
                    },
                ],
            )

    def test_isolates_executions_and_clears_only_the_selected_one(self):
        with tempfile.TemporaryDirectory() as directory:
            buffer = CandidateBuffer(directory)
            first_candidate = {
                "article_id": "article-1",
                "url": "https://example.com/1",
                "word_count": 3,
            }
            second_candidate = {
                "article_id": "article-2",
                "url": "https://example.com/2",
                "word_count": 5,
            }

            buffer.append("execution-1", [first_candidate])
            buffer.append("execution-2", [second_candidate])
            buffer.clear("execution-1")

            self.assertEqual(
                list(buffer.iterate("execution-1")),
                [],
            )
            self.assertEqual(
                list(buffer.iterate("execution-2")),
                [second_candidate],
            )
            self.assertEqual(
                len(list(Path(directory).iterdir())),
                1,
            )


if __name__ == "__main__":
    unittest.main()
