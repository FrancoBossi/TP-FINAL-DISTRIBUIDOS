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
