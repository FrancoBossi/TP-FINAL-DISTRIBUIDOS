import logging
import os
from dataclasses import dataclass, field
from pathlib import Path


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q3WordAggregatorConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "q3_word_aggregator.input.0",
        )
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE",
            "result_sink.input",
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
    partition_count: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_COUNT", "1"))
    )
    projector_partition_count: int = field(
        default_factory=lambda: int(os.environ.get("PROJECTOR_PARTITION_COUNT", "1"))
    )
    state_root: Path = field(
        default_factory=lambda: Path(os.environ.get("STATE_ROOT", "/state"))
    )
