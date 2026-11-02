import logging

from client.client import Client
from client.config import ClientConfig, configure_logging
from common.shutdown import shutdown_signals


def main() -> int:
    configure_logging()
    config = ClientConfig()
    try:

        def stop():
            raise SystemExit(0)

        with shutdown_signals(stop):
            Client(config.host, config.port).run(config.input_file, config.output_dir)
    except (OSError, ValueError, EOFError, RuntimeError):
        logging.exception("Client failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
