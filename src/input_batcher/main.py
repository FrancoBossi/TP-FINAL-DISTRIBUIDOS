import logging
from types import FrameType

from common.health import HealthServer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.persistence import PersistenceLayout
from common.shutdown import shutdown_signals
from input_batcher.batcher import BatchAccumulator
from input_batcher.config import InputBatcherConfig, configure_logging
from input_batcher.controller import InputBatcherController
from input_batcher.messaging import InputBatcherMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    output_queue_name: str,
) -> InputBatcherMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    output_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=output_queue_name,
    )

    return InputBatcherMessaging(
        input_queue=input_queue,
        output_queue=output_queue,
    )


def handle_sigterm(
    controller: InputBatcherController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    health = HealthServer.from_env()
    health.start()
    config = InputBatcherConfig()

    input_queue_name = f"{config.input_queue_prefix}.{config.partition_id}"

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=input_queue_name,
        output_queue_name=config.output_queue,
    )
    batcher = BatchAccumulator(config.max_batch_bytes, config.partition_id)
    controller = InputBatcherController(
        batcher=batcher,
        messaging=messaging,
        persistence_layout=PersistenceLayout(
            config.state_root,
            "input_batcher",
            config.partition_id,
        ),
        output_destination=config.output_queue,
    )

    logger.info(
        "InputBatcher partition %d consuming from %s",
        config.partition_id,
        input_queue_name,
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
