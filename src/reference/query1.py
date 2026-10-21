from typing import Optional

from common.article import Article
from common.query_results import Q1Result


def process_query1(article: Article) -> Optional[Q1Result]:

    if article.modification_date.year > 2020:
        return None

    if article.language != "en":
        return None

    return Q1Result(url=article.url, modification_date=article.modification_date)
