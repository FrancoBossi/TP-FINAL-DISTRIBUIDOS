import base64
import json
from pathlib import Path
from typing import Dict, Iterable, Union

from common.query_results import (
    Q1Result,
    Q2Result,
    Q4Result,
    Q5Result,
)


def _write_json_file(
    path: Union[str, Path], records: Iterable[Dict[str, object]]
) -> None:

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        for record in records:
            line = json.dumps(record, ensure_ascii=False)

            encoded_line = (line + "\n").encode("utf-8")
            written_bytes = f.write(encoded_line)

            if written_bytes != len(encoded_line):
                raise IOError(f"Failed to write the entire line to {path}")


def write_query1_results(path: Union[str, Path], results: Iterable[Q1Result]) -> None:

    records = (
        {
            "url": result.url,
            "modification_date": result.modification_date.isoformat(),
        }
        for result in results
    )

    _write_json_file(path, records)


def write_query2_results(path: Union[str, Path], results: Iterable[Q2Result]) -> None:

    records = (
        {
            "name": result.name,
            "url": result.url,
            "image": base64.b64encode(result.image).decode("ascii"),
        }
        for result in results
    )

    _write_json_file(path, records)


def write_query3_results(path: Union[str, Path], words: Iterable[str]) -> None:

    records = ({"word": word} for word in sorted(words))

    _write_json_file(path, records)


def write_query4_results(path: Union[str, Path], results: Iterable[Q4Result]) -> None:

    records = (
        {
            "word": result.word,
            "count": result.count,
        }
        for result in results
    )

    _write_json_file(path, records)


def write_query5_results(path: Union[str, Path], results: Iterable[Q5Result]) -> None:

    records = (
        {
            "url": result.url,
            "word_count": result.word_count,
        }
        for result in results
    )

    _write_json_file(path, records)
