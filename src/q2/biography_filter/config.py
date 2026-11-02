import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q2BiographyFilterConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "q2_biography_filter.input",
        )
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE",
            "image_fetcher.input",
        )
    )
    control_exchange: str = field(
        default_factory=lambda: os.environ.get(
            "CONTROL_EXCHANGE",
            "q2_biography_filter.control",
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
    until_2020_partition_count: int = field(
        default_factory=lambda: int(os.environ.get("UNTIL_2020_PARTITION_COUNT", "1"))
    )
