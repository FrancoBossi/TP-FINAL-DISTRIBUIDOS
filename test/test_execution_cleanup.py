import tempfile
import unittest
from unittest.mock import patch

from common.execution_cleanup import ExecutionCleanup
from common.persistence import AtomicJsonStore, PersistenceLayout


class ExecutionCleanupTest(unittest.TestCase):
    def test_restart_finishes_cleanup_and_keeps_other_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            layout = PersistenceLayout(directory, "worker", 0)
            abandoned = layout.state_path("abandoned")
            active = layout.state_path("active")
            AtomicJsonStore(abandoned).save({})
            AtomicJsonStore(active).save({})
            cleanup = ExecutionCleanup(layout, retention_seconds=10)

            def interrupted(execution_id):
                self.assertTrue(cleanup.is_closed(execution_id))
                raise RuntimeError("interrupted")

            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                cleanup.cleanup("abandoned", interrupted)
            self.assertTrue(abandoned.exists())

            cleanup = ExecutionCleanup(layout, retention_seconds=10)
            cleared = []
            with patch("common.execution_cleanup.time.time", return_value=100):
                cleanup.recover(cleared.append)
            cleanup.cleanup("abandoned", cleared.append)

            self.assertEqual(cleared, ["abandoned"])
            self.assertFalse(abandoned.parent.exists())
            self.assertTrue(active.exists())
            with patch("common.execution_cleanup.time.time", return_value=111):
                cleanup.expire(lambda _: False)
                self.assertTrue(cleanup.is_closed("abandoned"))
                cleanup.expire(lambda _: True)
            self.assertFalse(cleanup.is_closed("abandoned"))
