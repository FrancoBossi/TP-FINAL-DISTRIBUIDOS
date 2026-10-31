import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q4Q5WordProjectorConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "q4_q5_word_projector.input",
        )
    )
    q4_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q4_OUTPUT_QUEUE",
            "q4_word_counter.input.0",
        )
    )
    q5_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q5_OUTPUT_QUEUE",
            "q5_date_router.input",
        )
    )
