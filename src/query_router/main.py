import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from query_router.config import QueryRouterConfig, configure_logging
from query_router.controller import QueryRouterController
from query_router.messaging import QueryRouterMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    q1_q2_output_queue_name: str,
    text_output_queue_name: str,
    control_exchange_name: str,
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
    control_exchange_input = MessageMiddlewareExchangeRabbitMQ(
        host=mom_host,
        exchange_name=control_exchange_name,
        routing_keys=[control_exchange_name],
    )
    control_exchange_output = MessageMiddlewareExchangeRabbitMQ(
        host=mom_host,
        exchange_name=control_exchange_name,
        routing_keys=[control_exchange_name],
        create_queue=False,
    )

    return QueryRouterMessaging(
        input_queue=input_queue,
        q1_q2_output_queue=q1_q2_output_queue,
        text_output_queue=text_output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
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
        control_exchange_name=config.control_exchange,
    )
    controller = QueryRouterController(
        messaging=messaging,
        partition_id=config.partition_id,
        preprocessor_partition_count=config.preprocessor_partition_count,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "QueryRouter partition %s consuming from %s, awaiting %s "
        "preprocessor partitions and publishing to %s and %s",
        config.partition_id,
        config.input_queue,
        config.preprocessor_partition_count,
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
