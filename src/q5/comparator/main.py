import logging

from common.health import HealthServer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.persistence import PersistenceLayout
from common.shutdown import shutdown_signals
from q5.comparator.buffer import CandidateBuffer
from q5.comparator.config import Q5ComparatorConfig, configure_logging
from q5.comparator.controller import Q5ComparatorController
from q5.comparator.messaging import Q5ComparatorMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    health = HealthServer.from_env()
    health.start()
    config = Q5ComparatorConfig()

    messaging = Q5ComparatorMessaging(
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.input_queue,
        ),
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.output_queue,
        ),
    )
    candidate_buffer = CandidateBuffer(config.buffer_directory)
    controller = Q5ComparatorController(
        messaging,
        candidate_buffer,
        config.partition_id,
        config.date_router_partition_count,
        persistence_layout=PersistenceLayout(
            config.state_root, "q5_comparator", config.partition_id
        ),
        output_destination=config.output_queue,
    )

    logger.info(
        "Q5Comparator consuming from %s with buffer in %s",
        config.input_queue,
        config.buffer_directory,
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
