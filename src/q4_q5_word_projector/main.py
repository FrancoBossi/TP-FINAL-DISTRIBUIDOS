import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q4_q5_word_projector.config import (
    Q4Q5WordProjectorConfig,
    configure_logging,
)
from q4_q5_word_projector.controller import Q4Q5WordProjectorController
from q4_q5_word_projector.messaging import Q4Q5WordProjectorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    q4_output_queue_name: str,
    q5_output_queue_name: str,
) -> Q4Q5WordProjectorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    q4_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q4_output_queue_name,
    )
    q5_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q5_output_queue_name,
    )

    return Q4Q5WordProjectorMessaging(
        input_queue=input_queue,
        q4_output_queue=q4_output_queue,
        q5_output_queue=q5_output_queue,
    )


def handle_sigterm(
    controller: Q4Q5WordProjectorController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = Q4Q5WordProjectorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        q4_output_queue_name=config.q4_output_queue,
        q5_output_queue_name=config.q5_output_queue,
    )
    controller = Q4Q5WordProjectorController(
        messaging=messaging,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q4Q5WordProjector consuming from %s and publishing to %s and %s",
        config.input_queue,
        config.q4_output_queue,
        config.q5_output_queue,
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
