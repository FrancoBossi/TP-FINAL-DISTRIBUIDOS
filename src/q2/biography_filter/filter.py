from common.message_protocol.message import (
    ImageCandidateBatchMessage,
    Q2CandidateBatchMessage,
)

SECTION_NAMES = ("biography", "biographie")


def filter_biography_batch(
    message: Q2CandidateBatchMessage,
) -> ImageCandidateBatchMessage:
    candidates = [
        article
        for article in message.articles
        if any(section.lower() in SECTION_NAMES for section in article["sections"])
    ]

    image_candidates = [
        {
            "name": article["name"],
            "url": article["url"],
            "image_url": article["image_url"],
        }
        for article in candidates
    ]

    return ImageCandidateBatchMessage(
        articles=image_candidates,
        execution_id=message.execution_id,
        reply_queue=message.reply_queue,
    )
