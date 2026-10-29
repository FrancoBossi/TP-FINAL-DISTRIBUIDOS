import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from word_extractor.config import (
    WordExtractorConfig,
    configure_logging,
)
from word_extractor.controller import WordExtractorController
from word_extractor.messaging import WordExtractorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    q3_output_queue_name: str,
    q4_q5_output_queue_name: str,
) -> WordExtractorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    q3_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q3_output_queue_name,
    )
    q4_q5_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q4_q5_output_queue_name,
    )

    return WordExtractorMessaging(
        input_queue=input_queue,
        q3_output_queue=q3_output_queue,
        q4_q5_output_queue=q4_q5_output_queue,
    )


def handle_sigterm(
    controller: WordExtractorController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = WordExtractorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        q3_output_queue_name=config.q3_output_queue,
        q4_q5_output_queue_name=config.q4_q5_output_queue,
    )
    controller = WordExtractorController(
        messaging=messaging,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "WordExtractor consuming from %s and publishing to %s and %s",
        config.input_queue,
        config.q3_output_queue,
        config.q4_q5_output_queue,
    )

    try:
        controller.start()
    except KeyboardInterrupt:
        return 0
    finally:
        messaging.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
