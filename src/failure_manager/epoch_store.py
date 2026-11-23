"""Durable record of the highest coordinator epoch a FailureManager has seen.

The file is replaced atomically, so a crash leaves either the previous or the new
value, never a partial one. A restarted node resumes from it and therefore never
announces an epoch that an earlier coordinator already used.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class EpochStore:
    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    def load(self) -> int:
        try:
            value = int(self._path.read_text(encoding="utf-8").strip())
        except FileNotFoundError:
            return 0
        except ValueError:
            logger.warning("Ignoring unreadable epoch file %s", self._path)
            return 0

        return max(value, 0)

    def save(self, epoch: int) -> None:
        if epoch < 0:
            raise ValueError("epoch cannot be negative")

        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_name(self._path.name + ".tmp")

        with open(temporary, "w", encoding="utf-8") as file:
            file.write(f"{epoch}\n")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary, self._path)

        directory = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
