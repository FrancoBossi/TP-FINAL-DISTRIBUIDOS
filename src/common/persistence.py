import json
import os
import re
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.message_protocol.message import Message

_SAFE_PATH_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class PersistenceCorruptionError(RuntimeError):
    """The durable state exists but is not a valid state object."""


@dataclass(frozen=True)
class PendingOutput:
    destination: str
    message_id: str
    body: str

    @classmethod
    def from_message(cls, destination: str, message: Message) -> "PendingOutput":
        message_id = getattr(message, "message_id", None)
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("outbox message requires a nonempty message_id")
        if not isinstance(destination, str) or not destination:
            raise ValueError("outbox destination must be a nonempty string")
        return cls(
            destination=destination,
            message_id=message_id,
            body=message.serialize().decode("utf-8"),
        )

    def deserialize_message(self) -> Message:
        message = Message.deserialize(self.body.encode("utf-8"))
        if getattr(message, "message_id", None) != self.message_id:
            raise PersistenceCorruptionError(
                "outbox message_id does not match its serialized body"
            )
        return message

    @property
    def key(self) -> tuple[str, str]:
        return self.destination, self.message_id

    def to_record(self) -> dict[str, str]:
        return {
            "destination": self.destination,
            "message_id": self.message_id,
            "body": self.body,
        }

    @classmethod
    def from_record(cls, record: Any) -> "PendingOutput":
        if not isinstance(record, dict) or set(record) != {
            "destination",
            "message_id",
            "body",
        }:
            raise PersistenceCorruptionError("invalid durable outbox entry")
        if any(
            not isinstance(record[field], str) or not record[field] for field in record
        ):
            raise PersistenceCorruptionError("invalid durable outbox entry")
        output = cls(**record)
        output.deserialize_message()
        return output


def _validate_object_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("state object keys must be strings")
        for item in value.values():
            _validate_object_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _validate_object_keys(item)


def _validate_path_part(value: str, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_PATH_PART.fullmatch(value):
        raise ValueError(f"{field} contains unsupported path characters")
    return value


class PersistenceLayout:
    """Build isolated state paths for one component partition."""

    def __init__(self, root: str | Path, component: str, partition_id: int) -> None:
        if type(partition_id) is not int or partition_id < 0:
            raise ValueError("partition_id must be a nonnegative integer")
        self.root = Path(root)
        self.component = _validate_path_part(component, "component")
        self.partition_id = partition_id

    def state_path(self, execution_id: str) -> Path:
        execution_id = _validate_path_part(execution_id, "execution_id")
        return (
            self.root
            / self.component
            / f"partition-{self.partition_id}"
            / "executions"
            / execution_id
            / "state.json"
        )

    @property
    def executions_path(self) -> Path:
        return (
            self.root / self.component / f"partition-{self.partition_id}" / "executions"
        )

    def execution_ids(self) -> tuple[str, ...]:
        if not self.executions_path.exists():
            return ()
        execution_ids = []
        for entry in self.executions_path.iterdir():
            if not entry.is_dir() or not (entry / "state.json").is_file():
                continue
            try:
                execution_ids.append(_validate_path_part(entry.name, "execution_id"))
            except ValueError as error:
                raise PersistenceCorruptionError(
                    f"Invalid execution state directory {entry}"
                ) from error
        return tuple(sorted(execution_ids))


class AtomicJsonStore:
    """Persist one JSON object through fsync and atomic replacement."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._temporary_path = self.path.with_name(f".{self.path.name}.tmp")

    def load(self) -> dict[str, Any] | None:
        self._discard_incomplete_write()
        if not self.path.exists():
            return None

        try:
            with self.path.open("r", encoding="utf-8") as state_file:
                state = json.load(state_file)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise PersistenceCorruptionError(
                f"Cannot read durable state from {self.path}"
            ) from error

        if not isinstance(state, dict):
            raise PersistenceCorruptionError(
                f"Durable state in {self.path} must be a JSON object"
            )
        return state

    def save(self, state: Mapping[str, Any]) -> None:
        if not isinstance(state, Mapping):
            raise ValueError("state must be a mapping")
        _validate_object_keys(state)
        try:
            encoded = json.dumps(
                dict(state),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as error:
            raise ValueError("state must contain valid JSON values") from error

        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._temporary_path.open("w", encoding="utf-8") as temporary:
                temporary.write(encoded)
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())

            os.replace(self._temporary_path, self.path)
            self._fsync_directory()
        except BaseException:
            self._temporary_path.unlink(missing_ok=True)
            raise

    def _discard_incomplete_write(self) -> None:
        if not self._temporary_path.exists():
            return
        try:
            self._temporary_path.unlink()
        except OSError as error:
            raise PersistenceCorruptionError(
                f"Cannot discard incomplete state write {self._temporary_path}"
            ) from error

    def _fsync_directory(self) -> None:
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class DurableExecutionStore:
    """Atomically persist inbox, component state and pending outputs."""

    VERSION = 1

    def __init__(self, store: AtomicJsonStore) -> None:
        self._store = store
        loaded = store.load()
        self._document = self._empty_document() if loaded is None else loaded
        self._validate_document(self._document)

    @classmethod
    def _empty_document(cls) -> dict[str, Any]:
        return {
            "version": cls.VERSION,
            "inbox": [],
            "state": {},
            "outbox": [],
        }

    def has_applied(self, message_id: str) -> bool:
        self._validate_message_id(message_id)
        return message_id in self._document["inbox"]

    @property
    def state(self) -> dict[str, Any]:
        return deepcopy(self._document["state"])

    @property
    def pending_outputs(self) -> tuple[PendingOutput, ...]:
        return tuple(
            PendingOutput.from_record(record) for record in self._document["outbox"]
        )

    def apply(
        self,
        message_id: str,
        state: Mapping[str, Any],
        outputs: Iterable[PendingOutput],
    ) -> bool:
        """Commit one input once; return False when it is already in the inbox."""
        self._validate_message_id(message_id)
        if self.has_applied(message_id):
            return False
        if not isinstance(state, Mapping):
            raise ValueError("component state must be a mapping")

        new_outputs = tuple(outputs)
        self._validate_new_outputs(new_outputs)
        next_document = deepcopy(self._document)
        next_document["inbox"].append(message_id)
        next_document["state"] = deepcopy(dict(state))
        existing_outputs = {
            PendingOutput.from_record(record).key: record
            for record in next_document["outbox"]
        }
        for output in new_outputs:
            existing = existing_outputs.get(output.key)
            if existing is not None:
                if existing != output.to_record():
                    raise ValueError("outbox identity conflicts with another body")
                continue
            record = output.to_record()
            next_document["outbox"].append(record)
            existing_outputs[output.key] = record

        self._store.save(next_document)
        self._document = next_document
        return True

    def confirm(self, output: PendingOutput) -> bool:
        """Durably remove one broker-confirmed output from the outbox."""
        records = self._document["outbox"]
        remaining = [
            record
            for record in records
            if PendingOutput.from_record(record).key != output.key
        ]
        if len(remaining) == len(records):
            return False

        next_document = deepcopy(self._document)
        next_document["outbox"] = remaining
        self._store.save(next_document)
        self._document = next_document
        return True

    def compact_inbox(self, retained_message_ids: Iterable[str]) -> bool:
        """Keep only IDs still needed after the broker accepted their ACKs."""
        retained = tuple(dict.fromkeys(retained_message_ids))
        for message_id in retained:
            self._validate_message_id(message_id)
        current = self._document["inbox"]
        if any(message_id not in current for message_id in retained):
            raise ValueError("cannot retain a message absent from the durable inbox")
        if current == list(retained):
            return False

        next_document = deepcopy(self._document)
        next_document["inbox"] = list(retained)
        self._store.save(next_document)
        self._document = next_document
        return True

    @staticmethod
    def _validate_message_id(message_id: str) -> None:
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("message_id must be a nonempty string")

    @staticmethod
    def _validate_new_outputs(outputs: tuple[PendingOutput, ...]) -> None:
        seen: dict[tuple[str, str], PendingOutput] = {}
        for output in outputs:
            if not isinstance(output, PendingOutput):
                raise ValueError("outputs must contain PendingOutput values")
            output.deserialize_message()
            previous = seen.get(output.key)
            if previous is not None and previous != output:
                raise ValueError("outbox identity conflicts with another body")
            seen[output.key] = output

    @classmethod
    def _validate_document(cls, document: Any) -> None:
        if not isinstance(document, dict) or set(document) != {
            "version",
            "inbox",
            "state",
            "outbox",
        }:
            raise PersistenceCorruptionError("invalid durable execution state")
        if document["version"] != cls.VERSION:
            raise PersistenceCorruptionError("unsupported durable state version")
        if not isinstance(document["inbox"], list) or any(
            not isinstance(message_id, str) or not message_id
            for message_id in document["inbox"]
        ):
            raise PersistenceCorruptionError("invalid durable inbox")
        if len(set(document["inbox"])) != len(document["inbox"]):
            raise PersistenceCorruptionError("durable inbox contains duplicates")
        if not isinstance(document["state"], dict):
            raise PersistenceCorruptionError("invalid durable component state")
        if not isinstance(document["outbox"], list):
            raise PersistenceCorruptionError("invalid durable outbox")

        outputs = [PendingOutput.from_record(record) for record in document["outbox"]]
        keys = [output.key for output in outputs]
        if len(set(keys)) != len(keys):
            raise PersistenceCorruptionError("durable outbox contains duplicates")
