import argparse
from pathlib import Path
from typing import Iterator

from common.query_results import Q1Result, Q2Result
from common.writer import (
    write_query1_results,
    write_query2_results,
    write_query3_results,
    write_query4_results,
    write_query5_results,
)
from dataset.parquet_reader import iter_articles
from reference.query1 import process_query1
from reference.query2 import process_query2
from reference.query3 import process_query3
from reference.query4 import process_query4
from reference.query5 import (
    calculate_query5_average,
    process_query5,
)


def iter_query1_results(dataset_path: Path) -> Iterator[Q1Result]:

    for article in iter_articles(dataset_path):
        result = process_query1(article)

        if result is not None:
            yield result


def iter_query2_results(dataset_path: Path) -> Iterator[Q2Result]:

    for article in iter_articles(dataset_path):
        result = process_query2(article)

        if result is not None:
            yield result


def serial_run(dataset_path: Path, output_dir: Path) -> None:

    print("Running Query 1...")

    query1_results = iter_query1_results(dataset_path)
    write_query1_results(output_dir / "query1.jsonl", query1_results)

    print("Running Query 2...")

    query2_results = iter_query2_results(dataset_path)
    write_query2_results(output_dir / "query2.jsonl", query2_results)

    print("Running Query 3...")

    query3_result = process_query3(iter_articles(dataset_path))
    write_query3_results(output_dir / "query3.jsonl", query3_result)

    print("Running Query 4...")

    query4_result = process_query4(iter_articles(dataset_path))
    write_query4_results(output_dir / "query4.jsonl", query4_result)

    print("Running Query 5...")

    query5_average = calculate_query5_average(iter_articles(dataset_path))
    query5_result = process_query5(iter_articles(dataset_path), query5_average)
    write_query5_results(output_dir / "query5.jsonl", query5_result)

    print("Serial run completed. Results written to:", output_dir)


def parser() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Run reference queries on the dataset."
    )
    parser.add_argument(
        "dataset_path",
        type=Path,
        help="Path to the input Parquet file.",
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Directory where the query results will be written.",
    )
    return parser.parse_args()


def main() -> None:

    args = parser()
    dataset_path = args.dataset_path
    output_dir = args.output_dir

    if not dataset_path.is_file():
        raise ValueError(f"Dataset path {dataset_path} is not a file.")

    output_dir.mkdir(parents=True, exist_ok=True)

    serial_run(dataset_path, output_dir)


if __name__ == "__main__":
    main()
