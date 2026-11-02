import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q4WordCounterConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get("INPUT_QUEUE", "q4_word_counter.input.0")
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get("OUTPUT_QUEUE", "q4_top20_reducer.input")
    )
    output_queue_prefix: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE_PREFIX", "q4_top20_reducer.input"
        )
    )
    reducer_partition_count: int = field(
        default_factory=lambda: int(os.environ.get("REDUCER_PARTITION_COUNT", "1"))
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
    projector_partition_count: int = field(
        default_factory=lambda: int(os.environ.get("PROJECTOR_PARTITION_COUNT", "1"))
    )
