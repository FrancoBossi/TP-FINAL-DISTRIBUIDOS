import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q3.word_aggregator.config import Q3WordAggregatorConfig, configure_logging
from q3.word_aggregator.controller import Q3WordAggregatorController
from q3.word_aggregator.messaging import Q3WordAggregatorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_name: str,
) -> Q3WordAggregatorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=output_queue_name,
    )

    return Q3WordAggregatorMessaging(
        input_queue=input_queue,
        output_queue=output_queue,
    )


def handle_sigterm(
    controller: Q3WordAggregatorController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = Q3WordAggregatorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        output_queue_name=config.output_queue,
    )
    controller = Q3WordAggregatorController(
        messaging=messaging,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q3WordAggregator consuming from %s and publishing to %s",
        config.input_queue,
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
