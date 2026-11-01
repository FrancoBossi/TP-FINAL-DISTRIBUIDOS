import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class ResultSinkConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "result_sink.input",
        )
    )
    partition_counts: dict[int, int] = field(
        default_factory=lambda: {
            1: int(os.environ.get("Q1_PARTITION_COUNT", "1")),
            2: int(os.environ.get("Q2_PARTITION_COUNT", "1")),
            3: int(os.environ.get("Q3_PARTITION_COUNT", "1")),
            4: int(os.environ.get("Q4_PARTITION_COUNT", "1")),
            5: int(os.environ.get("Q5_PARTITION_COUNT", "1")),
        }
    )
