import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class QueryRouterConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "query_router.input",
        )
    )
    q1_q2_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q1_Q2_OUTPUT_QUEUE",
            "until_2020.input",
        )
    )
    text_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "TEXT_OUTPUT_QUEUE",
            "word_extractor.input",
        )
    )
