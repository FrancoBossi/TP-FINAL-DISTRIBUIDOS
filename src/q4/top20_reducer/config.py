import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q4Top20ReducerConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get("MOM_HOST", "localhost")
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get("INPUT_QUEUE", "q4_top20_reducer.input")
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get("OUTPUT_QUEUE", "result_sink.input")
    )
