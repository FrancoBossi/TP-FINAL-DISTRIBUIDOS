from typing import Optional
from common.article import Article
from common.query_results import Q2Result
from infra.wikimedia import download_image_bytes

SECTION_NAMES = ("biography", "biographie")

def is_query2_candidate(article: Article) -> bool:

    if article.modification_date.year > 2020:
        return False

    for section in article.sections:
        if section.lower() in SECTION_NAMES:
            return True

    return False

def process_query2(article: Article) -> Optional[Q2Result]:

    if not is_query2_candidate(article):
        return None

    if article.image_url is None:
        return None

    image_bytes = download_image_bytes(article.image_url)

    if image_bytes is None:
        return None

    return Q2Result(name=article.name, url=article.url, image=image_bytes)