import logging
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.shutdown import shutdown_signals
from word_extractor.config import (
    WordExtractorConfig,
    configure_logging,
)
from word_extractor.controller import WordExtractorController
from word_extractor.messaging import WordExtractorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    q3_output_queue_name: str,
    q4_q5_output_queue_name: str,
    control_exchange_name: str,
) -> WordExtractorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    q3_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q3_output_queue_name,
    )
    q4_q5_output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=q4_q5_output_queue_name,
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

    return WordExtractorMessaging(
        input_queue=input_queue,
        q3_output_queue=q3_output_queue,
        q4_q5_output_queue=q4_q5_output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
    )


def handle_sigterm(
    controller: WordExtractorController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = WordExtractorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        q3_output_queue_name=config.q3_output_queue,
        q4_q5_output_queue_name=config.q4_q5_output_queue,
        control_exchange_name=config.control_exchange,
    )
    controller = WordExtractorController(
        messaging=messaging,
        partition_id=config.partition_id,
        query_router_partition_count=config.query_router_partition_count,
    )

    logger.info(
        "WordExtractor partition %s consuming from %s, awaiting %s "
        "QueryRouter partitions and publishing to %s and %s",
        config.partition_id,
        config.input_queue,
        config.query_router_partition_count,
        config.q3_output_queue,
        config.q4_q5_output_queue,
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
