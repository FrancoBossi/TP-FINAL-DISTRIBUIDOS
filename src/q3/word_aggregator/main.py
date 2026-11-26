import logging
from types import FrameType

from common.health import HealthServer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.persistence import PersistenceLayout
from common.shutdown import shutdown_signals
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
    health = HealthServer.from_env()
    health.start()
    config = Q3WordAggregatorConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
        output_queue_name=config.output_queue,
    )
    controller = Q3WordAggregatorController(
        messaging=messaging,
        partition_id=config.partition_id,
        partition_count=config.partition_count,
        projector_partition_count=config.projector_partition_count,
        persistence_layout=PersistenceLayout(
            config.state_root, "q3_word_aggregator", config.partition_id
        ),
        output_destination=config.output_queue,
    )

    logger.info(
        "Q3WordAggregator consuming from %s and publishing to %s",
        config.input_queue,
        config.output_queue,
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
