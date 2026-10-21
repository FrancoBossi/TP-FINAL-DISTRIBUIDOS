from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple


@dataclass(frozen=True)
class Article:
    name: str
    url: str
    language: str
    modification_date: datetime
    abstract: Optional[str]
    sections: Tuple[str, ...]
    image_url: Optional[str]
