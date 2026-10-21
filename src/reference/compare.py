import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Counter as CounterType
from typing import Tuple

QUERY_FILES = (
    "query1.jsonl",
    "query2.jsonl",
    "query3.jsonl",
    "query4.jsonl",
    "query5.jsonl",
)


def _calculate_record_hash(record: dict) -> str:

    normalized_record = json.dumps(
        record, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )

    record_hash = hashlib.sha256(normalized_record.encode("utf-8")).hexdigest()

    return record_hash


def _read_records_hash(path: Path) -> CounterType[str]:

    hashes = Counter()

    with path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            if not line.strip():
                raise ValueError(f"Empty line found in {path} at line {line_number}")

            try:
                record = json.loads(line)

            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in {path} at line {line_number}: {e}")

            if not isinstance(record, dict):
                raise ValueError(
                    f"Expected a JSON object in {path} at line {line_number}, got {type(record).__name__}"
                )

            record_hash = _calculate_record_hash(record)

            hashes[record_hash] += 1

    return hashes


def compare_results(expected_dir: Path, actual_dir: Path) -> Tuple[int, int]:

    expected_hashes = _read_records_hash(expected_dir)
    actual_hashes = _read_records_hash(actual_dir)

    missing_records = expected_hashes - actual_hashes
    extra_records = actual_hashes - expected_hashes

    missing_count = sum(missing_records.values())
    extra_count = sum(extra_records.values())

    return missing_count, extra_count


def compare_result_directories(expected_dir: Path, actual_dir: Path) -> bool:

    all_files_match = True

    for query_file in QUERY_FILES:
        expected_file = expected_dir / query_file
        actual_file = actual_dir / query_file

        try:
            missing_count, extra_count = compare_results(expected_file, actual_file)

        except (OSError, ValueError) as e:
            print(f"Error comparing {query_file}: {e}")
            all_files_match = False
            continue

        if missing_count == 0 and extra_count == 0:
            print(f"{query_file}: Match")
        else:
            print(
                f"{query_file}: Mismatch - Missing: {missing_count}, Extra: {extra_count}"
            )
            all_files_match = False

    return all_files_match


def parser() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Compare query results between two directories."
    )
    parser.add_argument(
        "expected_dir",
        type=Path,
        help="Path to the directory containing the expected query results.",
    )
    parser.add_argument(
        "actual_dir",
        type=Path,
        help="Path to the directory containing the actual query results.",
    )
    return parser.parse_args()


def main() -> None:

    args = parser()
    expected_dir = args.expected_dir
    actual_dir = args.actual_dir

    if not expected_dir.is_dir():
        raise ValueError(f"Expected directory {expected_dir} is not a directory.")

    if not actual_dir.is_dir():
        raise ValueError(f"Actual directory {actual_dir} is not a directory.")

    all_files_match = compare_result_directories(expected_dir, actual_dir)

    if all_files_match:
        print("All query result files match.")
    else:
        print("Some query result files do not match.")

    raise SystemExit(0 if all_files_match else 1)


if __name__ == "__main__":
    main()
