from typing import FrozenSet, Optional
from nltk.corpus import stopwords
from common.text import extract_words

STOP_WORDS: FrozenSet[str] = frozenset(stopwords.words("english") + stopwords.words("french"))

def extract_unique_non_stopwords(text: Optional[str]) -> FrozenSet[str]:

    words = extract_words(text)
    return frozenset(word for word in words if word not in STOP_WORDS)