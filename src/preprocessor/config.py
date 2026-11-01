import logging
import os
from dataclasses import dataclass, field


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())


@dataclass
class PreprocessorConfig:
    mom_host: str = field(
        default_factory=lambda: os.environ.get(
            "MOM_HOST",
            "localhost",
        )
    )
    input_queue: str = field(
        default_factory=lambda: os.environ.get(
            "INPUT_QUEUE",
            "preprocessor.input",
        )
    )
    output_queue: str = field(
        default_factory=lambda: os.environ.get(
            "OUTPUT_QUEUE",
            "query_router.input",
        )
    )
    control_exchange: str = field(
        default_factory=lambda: os.environ.get(
            "CONTROL_EXCHANGE",
            "preprocessor.control",
        )
    )
    partition_id: int = field(
        default_factory=lambda: int(os.environ.get("PARTITION_ID", "0"))
    )
