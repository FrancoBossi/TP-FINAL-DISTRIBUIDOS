import logging
import os
from dataclasses import dataclass


def configure_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


@dataclass
class ClientConfig:
    host: str = os.environ.get("SERVER_HOST", "localhost")
    port: int = int(os.environ.get("SERVER_PORT", "5000"))
    input_file: str = os.environ.get("INPUT_FILE", "datasets/wikipedia_sample.parquet")
    output_dir: str = os.environ.get("OUTPUT_DIR", "output/distributed")
