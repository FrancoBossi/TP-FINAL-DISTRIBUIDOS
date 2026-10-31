import logging
import signal
from functools import partial

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q5.stats_worker.config import Q5StatsWorkerConfig, configure_logging
from q5.stats_worker.controller import Q5StatsWorkerController
from q5.stats_worker.messaging import Q5StatsWorkerMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    config = Q5StatsWorkerConfig()

    messaging = Q5StatsWorkerMessaging(
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.input_queue,
        ),
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.output_queue,
        ),
    )
    controller = Q5StatsWorkerController(
        messaging,
        config.partition_id,
    )

    signal.signal(
        signal.SIGTERM,
        partial(lambda c, _s, _f: c.stop(), controller),
    )
    logger.info(
        "Q5StatsWorker partition %s consuming from %s",
        config.partition_id,
        config.input_queue,
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
