import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.partition_router import PartitionRouter
from q3.word_projector.config import Q3WordProjectorConfig, configure_logging
from q3.word_projector.controller import Q3WordProjectorController
from q3.word_projector.messaging import Q3WordProjectorMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_prefix: str,
) -> Q3WordProjectorMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=f"{output_queue_prefix}.0",
    )

    return Q3WordProjectorMessaging(
        input_queue=input_queue,
        output_queues={f"{output_queue_prefix}.0": output_queue},
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
    )
    controller = Q3WordProjectorController(
        messaging=messaging,
        router=PartitionRouter(config.output_queue_prefix, 1),
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "Q3WordProjector consuming from %s and publishing to %s",
        config.input_queue,
        f"{config.output_queue_prefix}.0",
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
