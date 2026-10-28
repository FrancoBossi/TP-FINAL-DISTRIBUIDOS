import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from query_router.config import QueryRouterConfig, configure_logging
from query_router.controller import QueryRouterController
from query_router.messaging import QueryRouterMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    q1_q2_output_queue_name: str,
    text_output_queue_name: str,
) -> QueryRouterMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    q1_q2_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q1_q2_output_queue_name,
    )
    text_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=text_output_queue_name,
    )

    return QueryRouterMessaging(
        input_queue=input_queue,
        q1_q2_output_queue=q1_q2_output_queue,
        text_output_queue=text_output_queue,
    )


def handle_sigterm(
    controller: QueryRouterController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = QueryRouterConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        q1_q2_output_queue_name=config.q1_q2_output_queue,
        text_output_queue_name=config.text_output_queue,
    )
    controller = QueryRouterController(
        messaging=messaging,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "QueryRouter consuming from %s and publishing to %s and %s",
        config.input_queue,
        config.q1_q2_output_queue,
        config.text_output_queue,
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
