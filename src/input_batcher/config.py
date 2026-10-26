import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class InputBatcherConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    input_queue_prefix: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_BATCHER_QUEUE_PREFIX",
            "input_batcher.input",
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("INPUT_BATCHER_PARTITION_ID", "0"))
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE",
            "preprocessor.input",
        )
    )
    max_batch_bytes: int = field(
        default_factory=lambda: int(os.environ.get("MAX_BATCH_BYTES", "1048576"))
    )
