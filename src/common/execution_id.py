import math
import re
from time import time
from uuid import uuid4

_LATE_WORK_GRACE_SECONDS = 3600
_TIMED_ID = re.compile(r"e1-([0-9]{1,16})-[0-9a-f]{32}")


def new_execution_id(timeout_seconds: float | None) -> str:
    token = uuid4().hex
    if timeout_seconds is None:
        return token
    cutoff = math.ceil(time() + timeout_seconds + _LATE_WORK_GRACE_SECONDS)
    return f"e1-{cutoff}-{token}"


def execution_cutoff(execution_id: str) -> int | None:
    match = _TIMED_ID.fullmatch(execution_id)
    return int(match[1]) if match is not None else None
