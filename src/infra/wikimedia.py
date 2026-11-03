import logging
import os
from time import sleep
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TIMEOUT = float(os.environ.get("IMAGE_FETCH_TIMEOUT", "10"))
MAX_ATTEMPTS = int(os.environ.get("IMAGE_FETCH_MAX_ATTEMPTS", "5"))
RETRY_DELAY = float(os.environ.get("IMAGE_FETCH_RETRY_DELAY", "1"))
REQUEST_DELAY = float(os.environ.get("IMAGE_FETCH_REQUEST_DELAY", "0.5"))

USER_AGENT = (
    "TP-FINAL-DISTRIBUIDOS/0.1 (https://github.com/FrancoBossi/TP-FINAL-DISTRIBUIDOS)"
)

logger = logging.getLogger(__name__)


def download_image_bytes(
    image_url: str,
    timeout: float = TIMEOUT,
    max_attempts: int = MAX_ATTEMPTS,
    retry_delay: float = RETRY_DELAY,
    request_delay: float = REQUEST_DELAY,
) -> Optional[bytes]:
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    if timeout <= 0 or retry_delay < 0 or request_delay < 0:
        raise ValueError("timeout must be positive and delays must be nonnegative")

    request = Request(
        image_url,
        headers={"User-Agent": USER_AGENT},
    )

    for attempt in range(1, max_attempts + 1):
        if request_delay:
            sleep(request_delay)

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
