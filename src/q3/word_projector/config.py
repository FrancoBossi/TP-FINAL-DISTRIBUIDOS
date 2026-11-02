import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q3WordProjectorConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get("INPUT_QUEUE", "q3_word_projector.input")
    )
    output_queue_prefix: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE_PREFIX", "q3_word_aggregator.input"
        )
    )
    partition_count: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_COUNT", "1"))
    )
    control_exchange: str = field(
        default_factory=lambda: os.environ.get(
            "CONTROL_EXCHANGE", "q3_word_projector.control"
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
    word_extractor_partition_count: int = field(
        default_factory=lambda: int(
            os.environ.get("WORD_EXTRACTOR_PARTITION_COUNT", "1")
        )
    )
