import logging

from common.health import HealthServer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.persistence import PersistenceLayout
from common.shutdown import shutdown_signals
from q5.average_reducer.config import (
    Q5AverageReducerConfig,
    configure_logging,
)
from q5.average_reducer.controller import Q5AverageReducerController
from q5.average_reducer.messaging import Q5AverageReducerMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    health = HealthServer.from_env()
    health.start()
    config = Q5AverageReducerConfig()

    output_queues = {
        f"{config.output_queue_prefix}.{partition}": MessageMiddlewareQueueRabbitMQ(
            config.mom_host, f"{config.output_queue_prefix}.{partition}"
        )
        for partition in range(config.comparator_partition_count)
    }
    messaging = Q5AverageReducerMessaging(
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.input_queue,
        ),
        next(iter(output_queues.values())),
        output_queues,
    )
    controller = Q5AverageReducerController(
        messaging,
        config.partition_count,
        persistence_layout=PersistenceLayout(
            config.state_root, "q5_average_reducer", config.partition_id
        ),
        output_destinations=list(output_queues),
    )

    logger.info(
        "Q5AverageReducer consuming from %s and waiting for %s partitions",
        config.input_queue,
        config.partition_count,
    )

    try:
        with shutdown_signals(controller.stop):
            health.set_ready()
            controller.start()
    except KeyboardInterrupt:
        return 0
    finally:
        health.close()
        messaging.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
