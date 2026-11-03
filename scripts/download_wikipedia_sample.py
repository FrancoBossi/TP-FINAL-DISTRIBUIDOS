# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb==2.0.0", "pyarrow", "kagglehub>=1.0.3,<2"]
# ///
"""Download wikipedia dataset sample.

$ uv run scripts/download_wikipedia_sample.py --files-per-language 30
"""

import argparse
import json
from pathlib import Path

import duckdb
import kagglehub
import pyarrow.parquet as pq

DATASET = "wikimedia-foundation/wikipedia-structured-contents"
COLUMNS = [
    "abstract",
    "date_modified",
    "image",
    "in_language",
    "name",
    "sections",
    "url",
]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def download_shards(language, directory, file_count):
    selected = []
    for index in range(file_count):
        remote = f"{language}wiki/data/{language}wiki_namespace_0_{index:05d}.parquet"
        local = directory / remote
        if not local.exists():
            print(f"Downloading {remote}", flush=True)
            local = Path(
                kagglehub.dataset_download(
                    DATASET, path=remote, output_dir=str(directory)
                )
            )
        pq.ParquetFile(local)
        selected.append(local)
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files-per-language", type=int, default=10)
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("datasets/kaggle_shards")
    )
    parser.add_argument(
        "--output", type=Path, default=Path("datasets/wikipedia_kaggle.parquet")
    )
    args = parser.parse_args()
    if args.files_per_language <= 0:
        parser.error("--files-per-language must be positive")
    if args.output.exists():
        parser.error(f"Output already exists: {args.output}")
    sources = {
        language: download_shards(language, args.cache_dir, args.files_per_language)
        for language in ("en", "fr")
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    partial = args.output.with_suffix(".partial.parquet")
    paths = [path for files in sources.values() for path in files]
    filenames = "[" + ",".join(quote(path) for path in paths) + "]"
    query = (
        f"COPY (SELECT {','.join(COLUMNS)} FROM read_parquet({filenames})) "
        f"TO {quote(partial)} (FORMAT 'PARQUET', ROW_GROUP_SIZE 2048)"
    )
    print(query, flush=True)
    try:
        with duckdb.connect() as connection:
            connection.execute("SET threads=1")
            connection.execute(query)
            counts = dict(
                connection.execute(
                    "SELECT in_language.identifier, count(*) FROM read_parquet(?) GROUP BY 1",
                    [str(partial)],
                ).fetchall()
            )
            expected_rows = sum(
                pq.ParquetFile(path).metadata.num_rows for path in paths
            )
            if sum(counts.values()) != expected_rows:
                raise RuntimeError("Merged row count differs from the selected files")
        partial.replace(args.output)
    finally:
        partial.unlink(missing_ok=True)
    manifest = {
        "dataset": DATASET,
        "files_per_language": args.files_per_language,
        "counts": counts,
        "columns": COLUMNS,
        "sources": {
            language: [str(path) for path in paths]
            for language, paths in sources.items()
        },
        "sampling": "all rows from the first selected shards of each language",
    }
    args.output.with_suffix(".manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(f"Saved {args.output}: {counts}")


if __name__ == "__main__":
    main()
