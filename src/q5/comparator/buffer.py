import hashlib
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


class CandidateBuffer:
    def __init__(self, directory: str | Path) -> None:
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        execution_id: str,
        articles: Iterable[dict[str, Any]],
    ) -> None:
        path = self._path_for(execution_id)

        with path.open("a", encoding="utf-8") as file:
            for article in articles:
                file.write(
                    json.dumps(
                        article,
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                )
                file.write("\n")

    def iterate(self, execution_id: str) -> Iterator[dict[str, Any]]:
        path = self._path_for(execution_id)

        if not path.exists():
            return

        with path.open("r", encoding="utf-8") as file:
            for line in file:
                article = json.loads(line)

                if not isinstance(article, dict):
                    raise ValueError("Expected a Q5 candidate object")

                yield article

    def clear(self, execution_id: str) -> None:
        self._path_for(execution_id).unlink(missing_ok=True)

    def _path_for(self, execution_id: str) -> Path:
        digest = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()
        return self._directory / f"{digest}.jsonl"
