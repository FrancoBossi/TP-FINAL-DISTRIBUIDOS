import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class GatewayConfig:
    host: str = field(default_factory=lambda: os.environ.get("SERVER_HOST", "0.0.0.0"))
    port: int = field(
        default_factory=lambda: int(os.environ.get("SERVER_PORT", "5000"))
    )
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    mom_user: str = field(default_factory=lambda: os.environ.get("MOM_USER", "guest"))
    mom_password: str = field(
        default_factory=lambda: os.environ.get("MOM_PASSWORD", "guest")
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get("INPUT_QUEUE", "input_batcher.input")
    )
