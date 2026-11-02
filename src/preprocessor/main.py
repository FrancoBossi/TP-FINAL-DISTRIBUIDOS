import logging
from types import FrameType

from common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.shutdown import shutdown_signals
from preprocessor.config import PreprocessorConfig, configure_logging
from preprocessor.controller import PreprocessorController
from preprocessor.messaging import PreprocessorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_name: str,
    control_exchange_name: str,
) -> PreprocessorMessaging:
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

    return PreprocessorMessaging(
        input_queue=input_queue,
        output_queue=output_queue,
        control_exchange_input=control_exchange_input,
        control_exchange_output=control_exchange_output,
    )


def handle_sigterm(
    controller: PreprocessorController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = PreprocessorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        output_queue_name=config.output_queue,
        control_exchange_name=config.control_exchange,
    )
    controller = PreprocessorController(
        messaging=messaging,
        partition_id=config.partition_id,
    )

    logger.info(
        "Preprocessor partition %s consuming from %s and publishing to %s",
        config.partition_id,
        config.input_queue,
        config.output_queue,
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
