from pathlib import Path
from typing import Any, Iterator, Union

import pyarrow.parquet as parquet

from common.article import Article, article_from_raw_record

PARQUET_COLUMNS = (
    "abstract",
    "date_modified",
    "image",
    "in_language",
    "name",
    "sections",
    "url",
)

DEFAULT_READ_BATCH_SIZE = 1024


def iter_article_records(
    path: Union[str, Path],
    batch_size: int = DEFAULT_READ_BATCH_SIZE,
) -> Iterator[dict[str, Any]]:
    parquet_file = parquet.ParquetFile(path)

    for batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=PARQUET_COLUMNS,
    ):
        for row in batch.to_pylist():
            row["date_modified"] = row["date_modified"].isoformat()
            yield row


def iter_articles(  # Uso exclusivo de ejecucion serial, no se usa en el resto del proyecto
    path: Union[str, Path],
    batch_size: int = DEFAULT_READ_BATCH_SIZE,
) -> Iterator[Article]:
    for record in iter_article_records(path, batch_size):
        yield article_from_raw_record(record)
