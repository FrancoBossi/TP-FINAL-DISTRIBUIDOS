import json
from pathlib import Path
from typing import Iterator, Optional, Tuple, Union
import pyarrow.parquet as parquet
from common.article import Article

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

def _parse_sections(sections: Optional[str]) -> Tuple[str, ...]:

    if sections is None:
        return tuple()

    sections = json.loads(sections)

    names = []

    for section in sections:
        if "name" in section:
            names.append(section["name"])

    return tuple(names)

def _parse_image_url(image: Optional[dict]) -> Optional[str]:

    if image is None:
        return None

    return image.get("content_url")

def _parse_article(row: dict) -> Article:
    return Article(
        name=row["name"],
        url=row["url"],
        language=row["in_language"]["identifier"],
        modification_date=row["date_modified"],
        abstract=row.get("abstract"),
        sections=_parse_sections(row.get("sections")),
        image_url=_parse_image_url(row.get("image")),
    )

def iter_articles(path: Union[str, Path], batch_size: int = DEFAULT_READ_BATCH_SIZE) -> Iterator[Article]:

    parquet_file = parquet.ParquetFile(path)

    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=PARQUET_COLUMNS):
        for row in batch.to_pylist():
            yield _parse_article(row)
