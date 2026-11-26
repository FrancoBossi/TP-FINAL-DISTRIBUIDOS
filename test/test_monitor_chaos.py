import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.monitor_chaos import follow_log


class ChaosMonitorTest(unittest.TestCase):
    def test_follow_log_reads_existing_and_appended_lines_portably(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "chaos.jsonl"
            path.write_text("existing\n", encoding="utf-8")
            lines = follow_log(path)

            self.assertEqual(next(lines), "existing\n")

            def append(_seconds):
                with path.open("a", encoding="utf-8") as log:
                    log.write("appended\n")
                    log.flush()

            with patch("scripts.monitor_chaos.time.sleep", side_effect=append):
                self.assertEqual(next(lines), "appended\n")
            lines.close()


if __name__ == "__main__":
    unittest.main()
