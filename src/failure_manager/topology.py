"""Static list of the processes the FailureManagers supervise.

The list is written down here and is the only source for detection and restarts.
Nothing is discovered at runtime: Docker is never asked which containers exist or
what state they are in. For every target the service name, the container name and
the ``HEALTH_NAME`` of the process are the same string, which is also its host
name inside the compose network.

The Gateway, RabbitMQ and the client are not supervised.
"""

from dataclasses import dataclass

from common.health import DEFAULT_HEALTH_PORT

FAILURE_MANAGER_PREFIX = "failure-manager"

# (service name prefix, number of replicas). Replicated stages are numbered from 0.
PIPELINE_GROUPS: tuple[tuple[str, int], ...] = (
    ("input-batcher", 3),
    ("preprocessor", 3),
    ("query-router", 3),
    ("q3-word-aggregator", 3),
    ("q3-word-projector", 3),
    ("word-extractor", 3),
    ("q4-q5-word-projector", 3),
    ("q4-word-counter", 3),
    ("q4-top20-reducer", 3),
    ("q5-date-router", 3),
    ("q5-stats-worker", 3),
    ("q5-average-reducer", 3),
    ("q5-comparator", 3),
    ("until-2020-filter", 3),
    ("q1", 3),
    ("q2-biography-filter", 3),
    ("image-fetcher", 2),
)

SINGLETONS: tuple[str, ...] = ("result-sink",)


@dataclass(frozen=True)
class Target:
    name: str
    host: str
    port: int
    container: str

    @classmethod
    def named(cls, name: str, port: int = DEFAULT_HEALTH_PORT) -> "Target":
        return cls(name=name, host=name, port=port, container=name)


def failure_manager_name(manager_id: int) -> str:
    return f"{FAILURE_MANAGER_PREFIX}-{manager_id}"


def pipeline_targets() -> tuple[Target, ...]:
    targets = [
        Target.named(f"{prefix}-{index}")
        for prefix, replicas in PIPELINE_GROUPS
        for index in range(replicas)
    ]
    targets.extend(Target.named(name) for name in SINGLETONS)
    return tuple(targets)


def failure_manager_targets(manager_ids: tuple[int, ...]) -> tuple[Target, ...]:
    return tuple(Target.named(failure_manager_name(i)) for i in sorted(manager_ids))


def targets_for(manager_id: int, manager_ids: tuple[int, ...]) -> tuple[Target, ...]:
    """Everything one FailureManager checks when it coordinates: the pipeline and
    the other FailureManagers, never itself."""
    others = tuple(i for i in manager_ids if i != manager_id)
    return pipeline_targets() + failure_manager_targets(others)
