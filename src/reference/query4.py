from collections import Counter
from typing import Iterable, Tuple

from common.article import Article
from common.query_results import Q4Result
from common.stop_words import extract_unique_non_stopwords

TOP = 20


def process_query4(articles: Iterable[Article]) -> Tuple[Q4Result, ...]:

    word_counter = Counter()

    for article in articles:
        words = extract_unique_non_stopwords(article.abstract)
        word_counter.update(words)

    ordered_words = sorted(
        word_counter.items(), key=lambda x: (-x[1], x[0])
    )  # El desempate se resuelve por orden alfabetico

    top_words = ordered_words[:TOP]
    results = []

    for word, count in top_words:
        results.append(Q4Result(word=word, count=count))

    return tuple(results)
