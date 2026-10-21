import re
from typing import Optional, Tuple

WORD_PATTERN = re.compile(r"\b\w+\b")


def extract_words(abstract: Optional[str]) -> Tuple[str, ...]:

    if abstract is None:
        return tuple()

    words = WORD_PATTERN.findall(abstract.lower())

    return tuple(word for word in words if not word.isdigit())
