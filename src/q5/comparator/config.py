import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q5ComparatorConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get("INPUT_QUEUE", "q5_comparator.input")
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get("OUTPUT_QUEUE", "result_sink.input")
    )
    buffer_directory: str = field(
        default_factory=lambda: os.environ.get(
            "Q5_BUFFER_DIRECTORY",
            "/tmp/q5-candidates",
        )
    )
