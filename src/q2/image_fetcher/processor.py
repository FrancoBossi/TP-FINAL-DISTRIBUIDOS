import base64
import logging
from collections.abc import Callable
from typing import Optional

from common.message_identity import build_result_message_ids
from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    ResultMessage,
)
from infra.wikimedia import download_image_bytes

ImageDownloader = Callable[[str], Optional[bytes]]

logger = logging.getLogger(__name__)


def build_query2_results(
    message: ImageCandidateBatchMessage,
    download_image: ImageDownloader = download_image_bytes,
) -> list[ResultMessage]:
    results = []

    for article in message.articles:
        image_url = article["image_url"]

        if image_url is None:
            logger.warning(
                "Skipping Q2 candidate without image URL: %s (%s)",
                article["name"],
                article["url"],
            )
            continue

        image_bytes = download_image(image_url)

        if image_bytes is None:
            logger.warning(
                "Skipping Q2 candidate after image download failure: %s (%s)",
                article["name"],
                article["url"],
            )
            continue

        encoded_image = base64.b64encode(image_bytes).decode("ascii")
        result_id, message_id = build_result_message_ids(
            execution_id=message.execution_id,
            query_id=2,
            result_kind="result",
            identity={"url": article["url"]},
            producer_id="q2_image_fetcher",
        )

        results.append(
            ResultMessage(
                query_id=2,
                record={
                    "name": article["name"],
                    "url": article["url"],
                    "image": encoded_image,
                },
                execution_id=message.execution_id,
                message_id=message_id,
                result_id=result_id,
            )
        )

    return results
