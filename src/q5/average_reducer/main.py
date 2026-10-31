import logging
import signal
from functools import partial

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q5.average_reducer.config import (
    Q5AverageReducerConfig,
    configure_logging,
)
from q5.average_reducer.controller import Q5AverageReducerController
from q5.average_reducer.messaging import Q5AverageReducerMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    config = Q5AverageReducerConfig()

    messaging = Q5AverageReducerMessaging(
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.input_queue,
        ),
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.output_queue,
        ),
    )
    controller = Q5AverageReducerController(
        messaging,
        config.partition_count,
    )

    signal.signal(
        signal.SIGTERM,
        partial(lambda c, _s, _f: c.stop(), controller),
    )
    logger.info(
        "Q5AverageReducer consuming from %s and waiting for %s partitions",
        config.input_queue,
        config.partition_count,
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
