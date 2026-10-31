import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q5.date_router.config import (
    Q5DateRouterConfig,
    configure_logging,
)
from q5.date_router.controller import Q5DateRouterController
from q5.date_router.messaging import Q5DateRouterMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    historical_output_queue_name: str,
    candidate_output_queue_name: str,
) -> Q5DateRouterMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    historical_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=historical_output_queue_name,
    )
    candidate_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=candidate_output_queue_name,
    )

    return Q5DateRouterMessaging(
        input_queue=input_queue,
        historical_output_queue=historical_output_queue,
        candidate_output_queue=candidate_output_queue,
    )


def handle_sigterm(
    controller: Q5DateRouterController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = Q5DateRouterConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        historical_output_queue_name=config.historical_output_queue,
        candidate_output_queue_name=config.candidate_output_queue,
    )
    controller = Q5DateRouterController(messaging)

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q5DateRouter consuming from %s and publishing to %s and %s",
        config.input_queue,
        config.historical_output_queue,
        config.candidate_output_queue,
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
