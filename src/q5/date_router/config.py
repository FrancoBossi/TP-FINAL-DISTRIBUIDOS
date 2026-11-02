import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class Q5DateRouterConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "q5_date_router.input",
        )
    )
    historical_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "HISTORICAL_OUTPUT_QUEUE",
            "q5_stats_worker.input",
        )
    )
    candidate_output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "CANDIDATE_OUTPUT_QUEUE",
            "q5_comparator.input",
        )
    )
    historical_output_queue_prefix: str = field(default_factory=lambda: os.environ.get("HISTORICAL_OUTPUT_QUEUE_PREFIX", "q5_stats_worker.input"))
    historical_partition_count: int = field(default_factory=lambda: int(os.environ.get("HISTORICAL_PARTITION_COUNT", "1")))
