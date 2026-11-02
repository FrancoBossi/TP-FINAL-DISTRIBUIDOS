import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Until2020FilterConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "until_2020.input",
        )
    )
    q1_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q1_OUTPUT_QUEUE",
            "q1_english_filter.input",
        )
    )
    q2_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q2_OUTPUT_QUEUE",
            "q2_biography_filter.input",
        )
    )
    control_exchange: str = field(
        default_factory=lambda: os.environ.get(
            "CONTROL_EXCHANGE",
            "until_2020.control",
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
    query_router_partition_count: int = field(
        default_factory=lambda: int(os.environ.get("QUERY_ROUTER_PARTITION_COUNT", "1"))
    )
