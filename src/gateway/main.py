import signal
from types import FrameType
from typing import NoReturn

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.partition_router import PartitionRouter
from gateway.config import GatewayConfig, configure_logging
from gateway.messaging import GatewayMessaging
from gateway.server import serve


def handle_sigterm(signum: int, frame: FrameType | None) -> NoReturn:
    raise SystemExit(0)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
    execution_id: str,
) -> GatewayMessaging:
    result_queue_name = f"results.{execution_id}"

    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    try:
        result_queue = MessageMiddlewareQueueRabbitMQ(
            host=mom_host,
            queue_name=result_queue_name,
            exclusive=True,
            auto_delete=True,
        )
    except Exception:
        input_queue.close()
        raise

    return GatewayMessaging(
        input_queue=input_queue,
        result_queue=result_queue,
        result_queue_name=result_queue_name,
    )


def main() -> int:
    configure_logging()
    config = GatewayConfig()

    input_router = PartitionRouter(
        queue_prefix=config.input_batcher_queue_prefix,
        partition_count=config.input_batcher_partitions,
    )

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        serve(
            config.host,
            config.port,
            lambda execution_id: create_messaging(
                mom_host=config.mom_host,
                input_queue_name=input_router.queue_for(execution_id),
                execution_id=execution_id,
            ),
        )
    except KeyboardInterrupt:
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
