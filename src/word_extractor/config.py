import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class WordExtractorConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "word_extractor.input",
        )
    )
    q3_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q3_OUTPUT_QUEUE",
            "q3_word_projector.input",
        )
    )
    q4_q5_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "Q4_Q5_OUTPUT_QUEUE",
            "q4_q5_word_projector.input",
        )
    )
