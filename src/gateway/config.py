import logging
import math
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
    lifecycle_queue: str = field(
        default_factory=lambda: os.environ.get("LIFECYCLE_QUEUE", "result_sink.input")
    )
    execution_timeout_seconds: float = field(
        default_factory=lambda: float(
            os.environ.get("EXECUTION_TIMEOUT_SECONDS", "3600")
        )
    )

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.execution_timeout_seconds)
            or self.execution_timeout_seconds <= 0
        ):
            raise ValueError("EXECUTION_TIMEOUT_SECONDS must be positive and finite")
