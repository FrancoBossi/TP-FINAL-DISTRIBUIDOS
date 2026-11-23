import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from common.message_protocol.message import PartitionEndMessage
from common.persistence import (
    AtomicJsonStore,
    DurableExecutionStore,
    PendingOutput,
    PersistenceCorruptionError,
    PersistenceLayout,
)


class PersistenceLayoutTest(unittest.TestCase):
    def test_isolates_component_partition_and_execution(self):
        layout = PersistenceLayout("/state", "input_batcher", 2)

        self.assertEqual(
            Path(
                "/state/input_batcher/partition-2/executions/execution-123/state.json"
            ),
            layout.state_path("execution-123"),
        )

    def test_rejects_path_traversal_and_invalid_partitions(self):
        for component in ("", "../worker", "worker/name"):
            with self.subTest(component=component), self.assertRaises(ValueError):
                PersistenceLayout("/state", component, 0)
        for partition_id in (-1, True):
            with self.subTest(partition_id=partition_id), self.assertRaises(ValueError):
                PersistenceLayout("/state", "worker", partition_id)

        layout = PersistenceLayout("/state", "worker", 0)
        for execution_id in ("", "../execution", "execution/name"):
            with self.subTest(execution_id=execution_id), self.assertRaises(ValueError):
                layout.state_path(execution_id)

    def test_discovers_only_committed_execution_states_in_stable_order(self):
        layout = PersistenceLayout(self._temp_root(), "worker", 0)
        for execution_id in ("execution-b", "execution-a"):
            AtomicJsonStore(layout.state_path(execution_id)).save({"committed": True})
        incomplete = layout.executions_path / "execution-incomplete"
        incomplete.mkdir(parents=True)
        (incomplete / ".state.json.tmp").write_text("{}", encoding="utf-8")

        self.assertEqual(("execution-a", "execution-b"), layout.execution_ids())

    def _temp_root(self) -> Path:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        return Path(temporary_directory.name)


class AtomicJsonStoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "nested" / "state.json"
        self.store = AtomicJsonStore(self.path)

    def test_missing_state_loads_as_none(self):
        self.assertIsNone(self.store.load())

    def test_round_trips_canonical_unicode_state(self):
        state = {
            "inbox": ["m1:école"],
            "outbox": [{"message_id": "m1:salida", "attempts": 0}],
            "state": {"count": 2, "complete": False},
        }

        self.store.save(state)

        self.assertEqual(state, self.store.load())
        encoded = self.path.read_text(encoding="utf-8")
        self.assertTrue(encoded.endswith("\n"))
        self.assertEqual(state, json.loads(encoded))

    def test_ignores_orphan_temporary_write_and_keeps_last_commit(self):
        committed = {"generation": 1}
        self.store.save(committed)
        temporary = self.path.with_name(".state.json.tmp")
        temporary.write_text('{"generation":2}\n', encoding="utf-8")

        self.assertEqual(committed, self.store.load())
        self.assertFalse(temporary.exists())

    def test_failed_replace_keeps_previous_state(self):
        committed = {"generation": 1}
        self.store.save(committed)

        with patch("common.persistence.os.replace", side_effect=OSError("crash")):
            with self.assertRaisesRegex(OSError, "crash"):
                self.store.save({"generation": 2})

        self.assertEqual(committed, self.store.load())
        self.assertFalse(self.path.with_name(".state.json.tmp").exists())

    def test_fsyncs_file_before_replace_and_directory_afterwards(self):
        events = []
        real_fsync = os.fsync
        real_replace = os.replace

        def record_fsync(descriptor):
            events.append("fsync")
            return real_fsync(descriptor)

        def record_replace(source, destination):
            events.append("replace")
            return real_replace(source, destination)

        with (
            patch("common.persistence.os.fsync", side_effect=record_fsync),
            patch("common.persistence.os.replace", side_effect=record_replace),
        ):
            self.store.save({"generation": 1})

        self.assertEqual(["fsync", "replace", "fsync"], events)

    def test_rejects_invalid_json_values_before_touching_disk(self):
        for state in (["not", "an", "object"], {"value": float("nan")}, {1: 2}):
            with self.subTest(state=state), self.assertRaises(ValueError):
                self.store.save(state)
        self.assertFalse(self.path.exists())

    def test_reports_corrupt_committed_state(self):
        self.path.parent.mkdir(parents=True)
        self.path.write_text("not json", encoding="utf-8")

        with self.assertRaises(PersistenceCorruptionError):
            self.store.load()


class DurableExecutionStoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "state.json"
        self.atomic_store = AtomicJsonStore(self.path)
        self.store = DurableExecutionStore(self.atomic_store)

    def _output(
        self,
        destination: str = "next.input",
        message_id: str = "m1:output",
        stage: str = "worker",
    ) -> PendingOutput:
        return PendingOutput.from_message(
            destination,
            PartitionEndMessage(
                stage=stage,
                partition_id=0,
                execution_id="execution-123",
                reply_queue="results.execution-123",
                message_id=message_id,
            ),
        )

    def test_commits_inbox_state_and_outbox_as_one_recoverable_document(self):
        output = self._output()

        applied = self.store.apply(
            "m1:input",
            {"count": 1},
            [output],
        )
        recovered = DurableExecutionStore(AtomicJsonStore(self.path))

        self.assertTrue(applied)
        self.assertTrue(recovered.has_applied("m1:input"))
        self.assertEqual({"count": 1}, recovered.state)
        self.assertEqual((output,), recovered.pending_outputs)
        self.assertEqual(
            output.deserialize_message(),
            recovered.pending_outputs[0].deserialize_message(),
        )

    def test_duplicate_input_does_not_mutate_state_or_append_outputs(self):
        original = self._output()
        self.store.apply("m1:input", {"count": 1}, [original])

        applied = self.store.apply(
            "m1:input",
            {"count": 999},
            [self._output("other.input", "m1:other")],
        )

        self.assertFalse(applied)
        self.assertEqual({"count": 1}, self.store.state)
        self.assertEqual((original,), self.store.pending_outputs)

    def test_confirm_removes_only_the_published_destination_and_survives_restart(self):
        first = self._output("branch-a.input")
        second = self._output("branch-b.input")
        self.store.apply("m1:input", {"count": 1}, [first, second])

        self.assertTrue(self.store.confirm(first))
        self.assertFalse(self.store.confirm(first))
        recovered = DurableExecutionStore(AtomicJsonStore(self.path))

        self.assertEqual((second,), recovered.pending_outputs)

    def test_unconfirmed_output_remains_pending_after_restart(self):
        output = self._output()
        self.store.apply("m1:input", {"count": 1}, [output])

        recovered = DurableExecutionStore(AtomicJsonStore(self.path))

        self.assertEqual((output,), recovered.pending_outputs)

    def test_same_message_can_target_multiple_destinations(self):
        first = self._output("partition.0")
        second = self._output("partition.1")

        self.store.apply("m1:input", {}, [first, second])

        self.assertEqual((first, second), self.store.pending_outputs)

    def test_compacts_inbox_to_only_the_ids_still_needed(self):
        self.store.apply("m1:first", {"batch": [1]}, [])
        self.store.apply("m1:second", {"batch": [1, 2]}, [])

        self.assertTrue(self.store.compact_inbox(["m1:second"]))
        self.assertFalse(self.store.has_applied("m1:first"))
        self.assertTrue(self.store.has_applied("m1:second"))
        recovered = DurableExecutionStore(AtomicJsonStore(self.path))
        self.assertFalse(recovered.has_applied("m1:first"))
        self.assertTrue(recovered.has_applied("m1:second"))

    def test_rejects_conflicting_outbox_identity(self):
        first = self._output(stage="first")
        conflicting = self._output(stage="second")

        with self.assertRaisesRegex(ValueError, "conflicts"):
            self.store.apply("m1:input", {}, [first, conflicting])
        self.assertFalse(self.path.exists())

    def test_failed_atomic_commit_does_not_change_memory(self):
        output = self._output()

        with patch.object(self.atomic_store, "save", side_effect=OSError("disk")):
            with self.assertRaisesRegex(OSError, "disk"):
                self.store.apply("m1:input", {"count": 1}, [output])

        self.assertFalse(self.store.has_applied("m1:input"))
        self.assertEqual({}, self.store.state)
        self.assertEqual((), self.store.pending_outputs)

    def test_rejects_corrupt_execution_schema(self):
        AtomicJsonStore(self.path).save(
            {"version": 1, "inbox": ["m1:input", "m1:input"], "state": {}, "outbox": []}
        )

        with self.assertRaises(PersistenceCorruptionError):
            DurableExecutionStore(AtomicJsonStore(self.path))


if __name__ == "__main__":
    unittest.main()
