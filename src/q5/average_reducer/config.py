import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q5AverageReducerConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "q5_average_reducer.input",
        )
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get("OUTPUT_QUEUE", "q5_comparator.input")
    )
    partition_count: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_COUNT", "1"))
    )
