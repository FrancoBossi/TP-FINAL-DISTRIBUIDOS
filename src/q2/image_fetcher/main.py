import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from q2.image_fetcher.config import (
    ImageFetcherConfig,
    configure_logging,
)
from q2.image_fetcher.controller import ImageFetcherController
from q2.image_fetcher.messaging import ImageFetcherMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_name: str,
    control_exchange_name: str,
) -> ImageFetcherMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=output_queue_name,
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

    return ImageFetcherMessaging(
        input_queue=input_queue,
        output_queue=output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
    )


def handle_sigterm(
    controller: ImageFetcherController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = ImageFetcherConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        output_queue_name=config.output_queue,
        control_exchange_name=config.control_exchange,
    )
    controller = ImageFetcherController(
        messaging=messaging,
        partition_id=config.partition_id,
        biography_filter_partition_count=(config.biography_filter_partition_count),
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "ImageFetcher partition %s consuming from %s, awaiting %s "
        "Q2BiographyFilter partitions and publishing to %s",
        config.partition_id,
        config.input_queue,
        config.biography_filter_partition_count,
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
