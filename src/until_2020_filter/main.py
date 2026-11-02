import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from until_2020_filter.config import (
    Until2020FilterConfig,
    configure_logging,
)
from until_2020_filter.controller import Until2020FilterController
from until_2020_filter.messaging import Until2020FilterMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    q1_output_queue_name: str,
    q2_output_queue_name: str,
    control_exchange_name: str,
) -> Until2020FilterMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    q1_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q1_output_queue_name,
    )
    q2_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q2_output_queue_name,
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

    return Until2020FilterMessaging(
        input_queue=input_queue,
        q1_output_queue=q1_output_queue,
        q2_output_queue=q2_output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
    )


def handle_sigterm(
    controller: Until2020FilterController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = Until2020FilterConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        q1_output_queue_name=config.q1_output_queue,
        q2_output_queue_name=config.q2_output_queue,
        control_exchange_name=config.control_exchange,
    )
    controller = Until2020FilterController(
        messaging=messaging,
        partition_id=config.partition_id,
        query_router_partition_count=config.query_router_partition_count,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Until2020Filter partition %s consuming from %s, awaiting %s "
        "QueryRouter partitions and publishing to %s and %s",
        config.partition_id,
        config.input_queue,
        config.query_router_partition_count,
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


if __name__ == "__main__":
    raise SystemExit(main())
