import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.partition_router import PartitionRouter
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
    q4_partition_count: int = 1,
    control_exchange_name: str = "q4_q5_word_projector.control",
) -> Q4Q5WordProjectorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    q4_output_queues = {
        f"{q4_output_queue_name}.{partition}": MessageMiddlewareQueueRabbitMQ(
            host=mom_host, queue_name=f"{q4_output_queue_name}.{partition}"
        )
        for partition in range(q4_partition_count)
    }
    q5_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q5_output_queue_name,
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

    return Q4Q5WordProjectorMessaging(
        input_queue=input_queue,
        q4_output_queues=q4_output_queues,
        q5_output_queue=q5_output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
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
        q4_output_queue_name=config.q4_output_queue_prefix,
        q5_output_queue_name=config.q5_output_queue,
        q4_partition_count=config.q4_partition_count,
        control_exchange_name=config.control_exchange,
    )
    controller = Q4Q5WordProjectorController(
        messaging=messaging,
        router=PartitionRouter(
            config.q4_output_queue_prefix, config.q4_partition_count
        ),
        partition_id=config.partition_id,
        word_extractor_partition_count=config.word_extractor_partition_count,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q4Q5WordProjector partition %s consuming from %s and publishing to %s and %s",
        config.partition_id,
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
