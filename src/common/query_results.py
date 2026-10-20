from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Q1Result:
    url: str
    modification_date: datetime

@dataclass(frozen=True)
class Q2Result:
    name: str
    url: str
    image: bytes

@dataclass(frozen=True)
class Q4Result:
    word: str
    count: int

@dataclass(frozen=True)
class Q5Result:
    url: str
    word_count: int