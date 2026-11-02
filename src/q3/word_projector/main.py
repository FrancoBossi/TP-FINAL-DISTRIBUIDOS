import logging
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.partition_router import PartitionRouter
from common.shutdown import shutdown_signals
from q3.word_projector.config import Q3WordProjectorConfig, configure_logging
from q3.word_projector.controller import Q3WordProjectorController
from q3.word_projector.messaging import Q3WordProjectorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_prefix: str,
    partition_count: int = 1,
    control_exchange_name: str = "q3_word_projector.control",
) -> Q3WordProjectorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    output_queues = {
        f"{output_queue_prefix}.{partition}": MessageMiddlewareQueueRabbitMQ(
            host=mom_host, queue_name=f"{output_queue_prefix}.{partition}"
        )
        for partition in range(partition_count)
    }
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

    return Q3WordProjectorMessaging(
        input_queue=input_queue,
        output_queues=output_queues,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
    )


def handle_sigterm(
    controller: Q3WordProjectorController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = Q3WordProjectorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        output_queue_prefix=config.output_queue_prefix,
        partition_count=config.partition_count,
        control_exchange_name=config.control_exchange,
    )
    controller = Q3WordProjectorController(
        messaging=messaging,
        router=PartitionRouter(config.output_queue_prefix, config.partition_count),
        partition_id=config.partition_id,
        word_extractor_partition_count=config.word_extractor_partition_count,
    )

    logger.info(
        "Q3WordProjector partition %s consuming from %s and publishing to %s",
        config.partition_id,
        config.input_queue,
        f"{config.output_queue_prefix}.0",
    )

    try:
        with shutdown_signals(controller.stop):
            controller.start()
    except KeyboardInterrupt:
        return 0
    finally:
        messaging.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
