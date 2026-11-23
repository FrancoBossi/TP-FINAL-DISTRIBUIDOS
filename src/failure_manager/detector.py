"""Failure detection by direct PING/PONG against a static list of targets.

Only the answers of the processes themselves are used. A target is suspected
after ``failure_threshold`` consecutive rounds without a healthy answer, and the
suspicion is checked once more right before any restart. Docker is not consulted.

A round probes every target in parallel, so several simultaneous failures cost
one timeout, not one per failed process.
"""

import itertools
import logging
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from common.control_wire import ProtocolError
from common.health import STATUS_DEGRADED, STATUS_OK, STATUS_STARTING, ping
from failure_manager.config import DetectionSettings
from failure_manager.topology import Target

logger = logging.getLogger(__name__)

_KNOWN_STATUSES = frozenset({STATUS_OK, STATUS_STARTING, STATUS_DEGRADED})
_nonces = itertools.count(1)


@dataclass(frozen=True)
class Probe:
    """Outcome of probing one target within a round."""

    reachable: bool
    status: str | None = None
    incarnation_id: str | None = None


UNREACHABLE = Probe(reachable=False)


@dataclass(frozen=True)
class Suspect:
    target: Target
    incarnation_id: str | None
    failures: int
    first_failure_at: float
    reason: str


Prober = Callable[[Target], Probe]


def probe_target(
    target: Target,
    timeout: float,
    attempts: int,
    ping_function: Callable[..., dict] = ping,
) -> Probe:
    """PING a target up to ``attempts`` times; any valid PONG ends the probe."""
    for _ in range(attempts):
        try:
            answer = ping_function(
                target.host, target.port, timeout, nonce=next(_nonces)
            )
        except (OSError, EOFError, ProtocolError):
            continue

        if answer.get("name") != target.name:
            continue

        status = answer.get("status")
        return Probe(
            reachable=True,
            status=status if status in _KNOWN_STATUSES else STATUS_DEGRADED,
            incarnation_id=answer.get("incarnation_id"),
        )

    return UNREACHABLE


class _Tracker:
    def __init__(self) -> None:
        self.failures = 0
        self.first_failure_at: float | None = None
        self.incarnation_id: str | None = None
        self.ever_seen = False
        self.starting_since: float | None = None
        self.cooldown_until = 0.0
        self.restarted_at: float | None = None
        self.last_reason = ""


class Detector:
    def __init__(
        self,
        targets: Iterable[Target],
        prober: Prober,
        settings: DetectionSettings,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._targets = tuple(targets)
        self._prober = prober
        self._settings = settings
        self._clock = clock
        self._created_at = clock()
        self._lock = threading.Lock()
        self._trackers = {target.name: _Tracker() for target in self._targets}
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, len(self._targets)),
            thread_name_prefix="probe",
        )

    @property
    def targets(self) -> tuple[Target, ...]:
        return self._targets

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def reset(self) -> None:
        """Forget earlier suspicions, as a newly elected coordinator must.

        What the previous coordinator observed is not trusted: every target is
        verified again over the network from scratch.
        """
        with self._lock:
            for tracker in self._trackers.values():
                tracker.failures = 0
                tracker.first_failure_at = None
                tracker.starting_since = None

    def check_round(self) -> list[Suspect]:
        futures = [
            (target, self._executor.submit(self._prober, target))
            for target in self._targets
        ]
        suspects = []

        for target, future in futures:
            try:
                probe = future.result()
            except Exception:
                logger.exception("Probe of %s raised", target.name)
                probe = UNREACHABLE

            with self._lock:
                suspect = self._evaluate(target, probe, self._clock())

            if suspect is not None:
                suspects.append(suspect)

        return suspects

    def confirm(self, suspect: Suspect) -> bool:
        """Probe once more, directly, and decide whether to still restart.

        A healthy answer, or an answer from an instance other than the one that
        was suspected, means the suspicion is stale.
        """
        probe = self._prober(suspect.target)

        if not probe.reachable:
            return True

        with self._lock:
            since = self._trackers[suspect.target.name].starting_since

        if (
            suspect.incarnation_id is not None
            and probe.incarnation_id != suspect.incarnation_id
        ):
            logger.info(
                "%s answers as a newer instance: restart not needed",
                suspect.target.name,
            )
            return False

        if probe.status == STATUS_OK:
            logger.info("%s answers healthy: false suspicion", suspect.target.name)
            return False

        if probe.status == STATUS_STARTING:
            expired = since is not None and (
                self._clock() - since > self._settings.startup_grace
            )
            return expired

        return True

    def mark_restart_attempted(self, target: Target) -> None:
        with self._lock:
            tracker = self._trackers[target.name]
            now = self._clock()
            tracker.cooldown_until = now + self._settings.restart_cooldown
            tracker.failures = 0
            tracker.starting_since = None
            tracker.restarted_at = now

    def _evaluate(self, target: Target, probe: Probe, now: float) -> Suspect | None:
        tracker = self._trackers[target.name]

        if probe.reachable:
            if probe.incarnation_id != tracker.incarnation_id:
                tracker.incarnation_id = probe.incarnation_id
                tracker.starting_since = None
                tracker.failures = 0
                tracker.first_failure_at = None

            tracker.ever_seen = True

        if now < tracker.cooldown_until:
            tracker.failures = 0
            return None

        if not probe.reachable:
            if not tracker.ever_seen and (
                now - self._created_at < self._settings.initial_grace
            ):
                return None

            return self._fail(target, tracker, now, "no answer to PING")

        if probe.status == STATUS_OK:
            self._healthy(target, tracker, now)
            return None

        if probe.status == STATUS_STARTING:
            if tracker.starting_since is None:
                tracker.starting_since = now

            if now - tracker.starting_since <= self._settings.startup_grace:
                tracker.failures = 0
                tracker.first_failure_at = None
                return None

            return self._fail(target, tracker, now, "stuck while starting")

        return self._fail(target, tracker, now, "reports itself degraded")

    def _healthy(self, target: Target, tracker: _Tracker, now: float) -> None:
        if tracker.restarted_at is not None:
            logger.info(
                "%s recovered %.2fs after its restart",
                target.name,
                now - tracker.restarted_at,
            )
            tracker.restarted_at = None

        tracker.failures = 0
        tracker.first_failure_at = None
        tracker.starting_since = None

    def _fail(
        self, target: Target, tracker: _Tracker, now: float, reason: str
    ) -> Suspect | None:
        if tracker.failures == 0:
            tracker.first_failure_at = now

        tracker.failures += 1
        tracker.last_reason = reason

        if tracker.failures < self._settings.failure_threshold:
            return None

        return Suspect(
            target=target,
            incarnation_id=tracker.incarnation_id,
            failures=tracker.failures,
            first_failure_at=tracker.first_failure_at
            if tracker.first_failure_at is not None
            else now,
            reason=reason,
        )
