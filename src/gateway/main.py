import signal
from types import FrameType
from typing import NoReturn

from common.partition_router import PartitionRouter
from gateway.broker import RabbitMQBroker
from gateway.config import GatewayConfig, configure_logging
from gateway.server import serve


def handle_sigterm(signum: int, frame: FrameType | None) -> NoReturn:
    raise SystemExit(0)


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
            lambda run_id: RabbitMQBroker(
                config.mom_host,
                config.mom_user,
                config.mom_password,
                input_queue=input_router.queue_for(run_id),
                run_id=run_id,
            ),
        )
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
