import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q2.biography_filter.config import (
    Q2BiographyFilterConfig,
    configure_logging,
)
from q2.biography_filter.controller import Q2BiographyFilterController
from q2.biography_filter.messaging import Q2BiographyFilterMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_name: str,
) -> Q2BiographyFilterMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=output_queue_name,
    )

    return Q2BiographyFilterMessaging(
        input_queue=input_queue,
        output_queue=output_queue,
    )


def handle_sigterm(
    controller: Q2BiographyFilterController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = Q2BiographyFilterConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        output_queue_name=config.output_queue,
    )
    controller = Q2BiographyFilterController(
        messaging=messaging,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q2BiographyFilter consuming from %s and publishing to %s",
        config.input_queue,
        config.output_queue,
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
