import logging

from client.client import Client
from client.config import ClientConfig, configure_logging


def main() -> int:
    configure_logging()
    config = ClientConfig()
    try:
        Client(config.host, config.port).run(config.input_file, config.output_dir)
    except (OSError, ValueError, EOFError, RuntimeError):
        logging.exception("Client failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
