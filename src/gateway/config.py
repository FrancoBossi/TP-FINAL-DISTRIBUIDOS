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
    input_batcher_queue_prefix: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_BATCHER_QUEUE_PREFIX",
            "input_batcher.input",
        )
    )
    input_batcher_partitions: int = field(
        default_factory=lambda: int(os.environ.get("INPUT_BATCHER_PARTITIONS", "1"))
    )
