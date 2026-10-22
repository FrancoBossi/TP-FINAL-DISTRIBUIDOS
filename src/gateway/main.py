import signal
from types import FrameType
from typing import NoReturn

from gateway.broker import RabbitMQBroker
from gateway.config import GatewayConfig, configure_logging
from gateway.server import serve


def handle_sigterm(signum: int, frame: FrameType | None) -> NoReturn:
    raise SystemExit(0)


def main() -> int:
    configure_logging()
    config = GatewayConfig()
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        serve(
            config.host,
            config.port,
            lambda run_id: RabbitMQBroker(
                config.mom_host,
                config.mom_user,
                config.mom_password,
                input_queue=config.input_queue,
                run_id=run_id,
            ),
        )
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
