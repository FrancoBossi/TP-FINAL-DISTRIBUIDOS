import logging
import os
from dataclasses import dataclass, field
from pathlib import Path


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
    state_root: Path = field(
        default_factory=lambda: Path(os.environ.get("STATE_ROOT", "/state"))
    )
    input_batcher_queue_prefix: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_BATCHER_QUEUE_PREFIX", "input_batcher.input"
        )
    )
    input_batcher_partitions: int = field(
        default_factory=lambda: int(os.environ.get("INPUT_BATCHER_PARTITIONS", "1"))
    )
    cleanup_queues: tuple[str, ...] = field(
        default_factory=lambda: tuple(os.environ.get("CLEANUP_QUEUES", "").split())
    )
