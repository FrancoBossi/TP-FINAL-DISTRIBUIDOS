import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from until_2020.config import Until2020Config, configure_logging
from until_2020.controller import Until2020Controller
from until_2020.messaging import Until2020Messaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    config = Until2020Config()

    messaging = Until2020Messaging(
        input_queue=MessageMiddlewareQueueRabbitMQ(config.mom_host, config.input_queue),
        q1_output_queue=MessageMiddlewareQueueRabbitMQ(
            config.mom_host, config.q1_output_queue
        ),
        q2_output_queue=MessageMiddlewareQueueRabbitMQ(
            config.mom_host, config.q2_output_queue
        ),
    )
    controller = Until2020Controller(messaging)
    signal.signal(signal.SIGTERM, partial(_handle_sigterm, controller))

    logger.info(
        "Until2020 consuming from %s and publishing to %s and %s",
        config.input_queue,
        config.q1_output_queue,
        config.q2_output_queue,
    )

    try:
        controller.start()
    except KeyboardInterrupt:
        return 0
    finally:
        messaging.close()

    return 0


def _handle_sigterm(
    controller: Until2020Controller,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


if __name__ == "__main__":
    raise SystemExit(main())
