from dataclasses import dataclass
from hashlib import sha256


@dataclass(frozen=True)
class PartitionRouter:
    queue_prefix: str
    partition_count: int

    def __post_init__(self) -> None:
        if self.partition_count <= 0:
            raise ValueError("partition_count must be greater than zero")

    def partition_for(self, key: str) -> int:
        b = sha256(key.encode("utf-8")).digest()
        hash_value = int.from_bytes(b, byteorder="big")
        return hash_value % self.partition_count

    def queue_for(self, key: str) -> str:
        partition = self.partition_for(key)
        return f"{self.queue_prefix}.{partition}"
