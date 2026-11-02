import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q1Config:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "q1_english_filter.input",
        )
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE",
            "result_sink.input",
        )
    )
    control_exchange: str = field(
        default_factory=lambda: os.environ.get(
            "CONTROL_EXCHANGE",
            "q1.control",
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
    until_2020_partition_count: int = field(
        default_factory=lambda: int(os.environ.get("UNTIL_2020_PARTITION_COUNT", "1"))
    )
