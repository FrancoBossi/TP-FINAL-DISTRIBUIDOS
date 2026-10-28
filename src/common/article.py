import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple


@dataclass(frozen=True)
class Article:
    name: str
    url: str
    language: str
    modification_date: datetime
    abstract: Optional[str]
    sections: Tuple[str, ...]
    image_url: Optional[str]


def _required_string(record: dict[str, Any], field: str) -> str:

    value = record.get(field)

    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")

    return value


def _optional_string(record: dict[str, Any], field: str) -> Optional[str]:

    value = record.get(field)

    if value is not None and not isinstance(value, str):
        raise ValueError(f"{field} must be a string or null")

    return value


def _parse_sections(value: Any) -> Tuple[str, ...]:

    if value is None:
        return tuple()

    if not isinstance(value, str):
        raise ValueError("sections must be a JSON string or null")

    try:
        sections = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError("sections must contain valid JSON") from error

    if not isinstance(sections, list):
        raise ValueError("sections JSON must contain a list")

    names = []

    for section in sections:
        if not isinstance(section, dict):
            raise ValueError("each section must be an object")

        name = section.get("name")

        if name is None:
            continue

        if not isinstance(name, str):
            raise ValueError("section name must be a string")

        names.append(name)

    return tuple(names)


def _parse_image_url(value: Any) -> Optional[str]:

    if value is None:
        return None

    if not isinstance(value, dict):
        raise ValueError("image must be an object or null")

    return _optional_string(value, "content_url")


def article_from_raw_record(record: dict[str, Any]) -> Article:

    if not isinstance(record, dict):
        raise ValueError("article must be an object")

    in_language = record.get("in_language")

    if not isinstance(in_language, dict):
        raise ValueError("in_language must be an object")

    modification_date_value = _required_string(
        record,
        "date_modified",
    )

    try:
        modification_date = datetime.fromisoformat(
            modification_date_value,
        )
    except ValueError as error:
        raise ValueError("date_modified must be an ISO date") from error

    return Article(
        name=_required_string(record, "name"),
        url=_required_string(record, "url"),
        language=_required_string(
            in_language,
            "identifier",
        ).lower(),
        modification_date=modification_date,
        abstract=_optional_string(record, "abstract"),
        sections=_parse_sections(record.get("sections")),
        image_url=_parse_image_url(record.get("image")),
    )


def article_to_normalized_record(article: Article) -> dict[str, Any]:
    return {
        "name": article.name,
        "url": article.url,
        "language": article.language,
        "modification_date": article.modification_date.isoformat(),
        "abstract": article.abstract,
        "sections": list(article.sections),
        "image_url": article.image_url,
    }
