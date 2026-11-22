import hashlib
import json
from collections.abc import Mapping
from typing import Any

MESSAGE_ID_VERSION = "m1"
RESULT_ID_VERSION = "r1"


def _require_nonempty_string(value: str, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a nonempty string")


def _canonical_digest(domain: str, fields: Mapping[str, Any]) -> str:
    if not isinstance(fields, Mapping):
        raise ValueError("identity fields must be a mapping")
    if any(not isinstance(key, str) for key in fields):
        raise ValueError("identity field names must be strings")

    try:
        canonical = json.dumps(
            fields,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValueError("identity fields must contain valid JSON values") from error

    digest = hashlib.sha256(canonical).hexdigest()
    return f"{domain}:{digest}"


def build_message_id(
    *,
    execution_id: str,
    producer_id: str,
    operation: str,
    identity: Mapping[str, Any],
) -> str:
    """Build the stable ID of one logical internal message."""
    _require_nonempty_string(execution_id, "execution_id")
    _require_nonempty_string(producer_id, "producer_id")
    _require_nonempty_string(operation, "operation")

    return _canonical_digest(
        MESSAGE_ID_VERSION,
        {
            "execution_id": execution_id,
            "identity": dict(identity),
            "operation": operation,
            "producer_id": producer_id,
        },
    )


def build_result_id(
    *,
    execution_id: str,
    query_id: int,
    result_kind: str,
    identity: Mapping[str, Any],
) -> str:
    """Build the stable ID of one client-observable result or query end."""
    _require_nonempty_string(execution_id, "execution_id")
    if type(query_id) is not int or not 1 <= query_id <= 5:
        raise ValueError("query_id must be an integer from 1 through 5")
    _require_nonempty_string(result_kind, "result_kind")

    return _canonical_digest(
        RESULT_ID_VERSION,
        {
            "execution_id": execution_id,
            "identity": dict(identity),
            "query_id": query_id,
            "result_kind": result_kind,
        },
    )


def derive_message_id(
    *,
    execution_id: str,
    input_message_id: str,
    producer_id: str,
    operation: str,
    identity: Mapping[str, Any] | None = None,
) -> str:
    """Derive one deterministic output ID from an input message."""
    _require_nonempty_string(input_message_id, "input_message_id")
    output_identity = {"input_message_id": input_message_id}
    if identity is not None:
        output_identity.update(identity)
    return build_message_id(
        execution_id=execution_id,
        producer_id=producer_id,
        operation=operation,
        identity=output_identity,
    )


def build_control_message_id(
    *,
    execution_id: str,
    stage: str,
    partition_id: int,
    operation: str = "partition_end",
) -> str:
    """Build a control ID from its meaning, independent of arrival order."""
    if type(partition_id) is not int or partition_id < 0:
        raise ValueError("partition_id must be a nonnegative integer")
    return build_message_id(
        execution_id=execution_id,
        producer_id=stage,
        operation=operation,
        identity={"partition_id": partition_id},
    )


def build_result_message_ids(
    *,
    execution_id: str,
    query_id: int,
    result_kind: str,
    identity: Mapping[str, Any],
    producer_id: str,
) -> tuple[str, str]:
    """Return the client-visible result ID and its internal message ID."""
    result_id = build_result_id(
        execution_id=execution_id,
        query_id=query_id,
        result_kind=result_kind,
        identity=identity,
    )
    message_id = build_message_id(
        execution_id=execution_id,
        producer_id=producer_id,
        operation=result_kind,
        identity={"result_id": result_id},
    )
    return result_id, message_id
