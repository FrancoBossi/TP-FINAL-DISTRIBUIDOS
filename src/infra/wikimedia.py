import logging
from time import sleep
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TIMEOUT = 10.0
MAX_ATTEMPTS = 2
RETRY_DELAY = 1.0

USER_AGENT = (
    "TP-FINAL-DISTRIBUIDOS/0.1 (https://github.com/FrancoBossi/TP-FINAL-DISTRIBUIDOS)"
)

logger = logging.getLogger(__name__)


def download_image_bytes(
    image_url: str,
    timeout: float = TIMEOUT,
    max_attempts: int = MAX_ATTEMPTS,
    retry_delay: float = RETRY_DELAY,
) -> Optional[bytes]:
    request = Request(
        image_url,
        headers={"User-Agent": USER_AGENT},
    )

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read()

        except HTTPError as error:
            error.close()

            if error.code == 404:
                logger.warning(
                    "Image not found with HTTP 404: %s",
                    image_url,
                )
                return None

            retryable = error.code == 429 or 500 <= error.code < 600

            if not retryable:
                logger.warning(
                    "Image download failed permanently with HTTP %d: %s",
                    error.code,
                    image_url,
                )
                return None

            logger.warning(
                "Transient HTTP %d downloading %s, attempt %d/%d",
                error.code,
                image_url,
                attempt,
                max_attempts,
            )

        except (URLError, TimeoutError) as error:
            logger.warning(
                "Transient error downloading %s, attempt %d/%d: %s",
                image_url,
                attempt,
                max_attempts,
                error,
            )

        if attempt < max_attempts:
            sleep(retry_delay)

    logger.warning(
        "Image download failed after %d attempts: %s",
        max_attempts,
        image_url,
    )
    return None
