from typing import Iterable, Tuple
from common.article import Article
from common.query_results import Q5Result
from common.stop_words import extract_unique_non_stopwords

def calculate_query5_average(articles: Iterable[Article]) -> float:

    total_word_count = 0
    abstract_count = 0

    for article in articles:

        if article.modification_date.year > 2020:
            continue

        if article.abstract is None:
            continue

        words = extract_unique_non_stopwords(article.abstract)

        total_word_count += len(words)
        abstract_count += 1

    return total_word_count / abstract_count

def process_query5(articles: Iterable[Article], average: float) -> Tuple[Q5Result, ...]:

    results = []

    for article in articles:

        if article.modification_date.year <= 2020:
            continue

        words = extract_unique_non_stopwords(article.abstract)

        word_count = len(words)

        if word_count <= average:
            continue

        result = Q5Result(url=article.url, word_count=word_count)
        results.append(result)

    return tuple(results)