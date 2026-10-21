from time import sleep
from typing import Optional
from urllib.error import URLError
from urllib.request import (  # Request es la solicitud HTTP que se envía al servidor para obtener la imagen, y urlopen es la función que abre la URL y devuelve un objeto de respuesta.
    Request,
    urlopen,
)

TIMEOUT = 10.0
MAX_ATTEMPTS = 2
RETRY_DELAY = 1.0

USER_AGENT = (
    "TP-FINAL-DISTRIBUIDOS/0.1 (https://github.com/FrancoBossi/TP-FINAL-DISTRIBUIDOS)"
)


def download_image_bytes(
    image_url: str,
    timeout: float = TIMEOUT,
    max_attempts: int = MAX_ATTEMPTS,
    retry_delay: float = RETRY_DELAY,
) -> Optional[bytes]:

    request = Request(image_url, headers={"User-Agent": USER_AGENT})

    for attempt in range(max_attempts):
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read()
        except (URLError, TimeoutError):
            if attempt < max_attempts - 1:
                sleep(retry_delay)

    return None
