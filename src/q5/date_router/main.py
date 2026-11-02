import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.partition_router import PartitionRouter
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
    historical_partition_count: int = 1,
    control_exchange_name: str = "q5_date_router.control",
    candidate_output_queue_prefix: str = "q5_comparator.input",
    candidate_partition_count: int = 1,
) -> Q5DateRouterMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    historical_output_queues = {
        f"{historical_output_queue_name}.{partition}": MessageMiddlewareQueueRabbitMQ(
            host=mom_host, queue_name=f"{historical_output_queue_name}.{partition}"
        )
        for partition in range(historical_partition_count)
    }
    candidate_output_queues = {
        f"{candidate_output_queue_prefix}.{partition}": MessageMiddlewareQueueRabbitMQ(
            host=mom_host,
            queue_name=f"{candidate_output_queue_prefix}.{partition}",
        )
        for partition in range(candidate_partition_count)
    }
    candidate_output_queue = next(iter(candidate_output_queues.values()))
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

    return Q5DateRouterMessaging(
        input_queue=input_queue,
        historical_output_queue=next(iter(historical_output_queues.values())),
        candidate_output_queue=candidate_output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
        historical_output_queues=historical_output_queues,
        candidate_output_queues=candidate_output_queues,
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
        historical_partition_count=config.historical_partition_count,
        control_exchange_name=config.control_exchange,
        candidate_output_queue_prefix=config.candidate_output_queue_prefix,
        candidate_partition_count=config.candidate_partition_count,
    )
    controller = Q5DateRouterController(
        messaging,
        PartitionRouter(
            config.historical_output_queue_prefix,
            config.historical_partition_count,
        ),
        partition_id=config.partition_id,
        projector_partition_count=config.projector_partition_count,
        candidate_router=PartitionRouter(
            config.candidate_output_queue_prefix,
            config.candidate_partition_count,
        ),
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q5DateRouter partition %s consuming from %s and publishing to %s and %s",
        config.partition_id,
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
