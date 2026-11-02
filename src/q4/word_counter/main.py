import logging

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.partition_router import PartitionRouter
from common.shutdown import shutdown_signals
from q4.word_counter.config import Q4WordCounterConfig, configure_logging
from q4.word_counter.controller import Q4WordCounterController
from q4.word_counter.messaging import Q4WordCounterMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    config = Q4WordCounterConfig()
    output_queues = {
        f"{config.output_queue_prefix}.{partition}": MessageMiddlewareQueueRabbitMQ(
            config.mom_host, f"{config.output_queue_prefix}.{partition}"
        )
        for partition in range(config.reducer_partition_count)
    }
    messaging = Q4WordCounterMessaging(
        MessageMiddlewareQueueRabbitMQ(config.mom_host, config.input_queue),
        next(iter(output_queues.values())),
        output_queues,
        PartitionRouter(config.output_queue_prefix, config.reducer_partition_count),
    )
    controller = Q4WordCounterController(
        messaging,
        config.partition_id,
        config.projector_partition_count,
    )
    logger.info("Q4WordCounter consuming from %s", config.input_queue)
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
