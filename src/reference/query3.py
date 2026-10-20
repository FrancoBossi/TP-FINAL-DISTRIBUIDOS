from typing import Iterable, Set
from common.article import Article
from common.text import extract_words

def process_query3(articles: Iterable[Article]) -> Set[str]:

    english_words = set()
    french_words = set()

    for article in articles:

        words = extract_words(article.abstract)

        if article.language == "en":
            english_words.update(words)
        elif article.language == "fr":
            french_words.update(words)

    return english_words.intersection(french_words)