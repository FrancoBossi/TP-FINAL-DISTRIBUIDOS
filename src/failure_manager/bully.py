"""Bully election among the FailureManagers, as a pure state machine.

The node never touches sockets, threads or the system clock. The caller feeds it
the messages it receives and the current time, and the node sends through an
injected callback. That makes every timeout deterministic under a fake clock.

Messages are dictionaries with ``type``, ``sender`` and ``epoch``:

* ``ELECTION``: sent to every node with a higher ID.
* ``OK``: a higher node answering an ``ELECTION``.
* ``COORDINATOR``: announcement of the winner of an election.
* ``HEARTBEAT``: the coordinator renewing its authority.

Every election that is won increases the ``coordinator_epoch``. Claims are
ordered by ``(epoch, id)``, so announcements from an earlier coordinator never
override a later one. The highest known epoch is reported through
``on_epoch_change`` so that it can be stored and survive a restart.
"""

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

ELECTION = "ELECTION"
OK = "OK"
COORDINATOR = "COORDINATOR"
HEARTBEAT = "HEARTBEAT"

BULLY_MESSAGE_TYPES = frozenset({ELECTION, OK, COORDINATOR, HEARTBEAT})


class State(Enum):
    STARTING = "STARTING"
    FOLLOWER = "FOLLOWER"
    ELECTING = "ELECTING"
    AWAITING_COORDINATOR = "AWAITING_COORDINATOR"
    COORDINATOR = "COORDINATOR"


@dataclass(frozen=True)
class BullyTimeouts:
    heartbeat_interval: float = 0.5
    coordinator_timeout: float = 2.0
    election_timeout: float = 0.75
    announcement_timeout: float = 2.0
    startup_listen: float = 1.0

    def __post_init__(self) -> None:
        for name, value in vars(self).items():
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")

        if self.coordinator_timeout <= self.heartbeat_interval:
            raise ValueError("coordinator_timeout must exceed heartbeat_interval")


Send = Callable[[int, dict[str, Any]], None]


class BullyNode:
    def __init__(
        self,
        node_id: int,
        member_ids: Iterable[int],
        send: Send,
        timeouts: BullyTimeouts | None = None,
        known_epoch: int = 0,
        on_epoch_change: Callable[[int], None] | None = None,
        on_role_change: Callable[[bool, int], None] | None = None,
    ) -> None:
        self._members = frozenset(member_ids)

        if node_id not in self._members:
            raise ValueError("node_id must be one of the members")

        if known_epoch < 0:
            raise ValueError("known_epoch cannot be negative")

        self._id = node_id
        self._others = sorted(self._members - {node_id})
        self._higher = [member for member in self._others if member > node_id]
        self._send = send
        self._timeouts = timeouts if timeouts is not None else BullyTimeouts()
        self._on_epoch_change = on_epoch_change
        self._on_role_change = on_role_change

        self._state = State.STARTING
        self._known_epoch = known_epoch
        self._coordinator_id: int | None = None
        self._coordinator_epoch = 0
        self._deadline = 0.0
        self._last_contact = 0.0
        self._next_heartbeat = 0.0

    @property
    def node_id(self) -> int:
        return self._id

    @property
    def state(self) -> State:
        return self._state

    @property
    def is_coordinator(self) -> bool:
        return self._state is State.COORDINATOR

    @property
    def coordinator_id(self) -> int | None:
        return self._coordinator_id

    @property
    def epoch(self) -> int:
        """Epoch of the coordinator this node obeys (its own when it leads)."""
        return self._coordinator_epoch

    @property
    def known_epoch(self) -> int:
        return self._known_epoch

    def start(self, now: float) -> None:
        """Listen for a live coordinator before claiming leadership.

        A restarted node learns the current epoch from the heartbeats it hears, so
        its own election yields a strictly newer epoch than the one it replaces.
        """
        self._state = State.STARTING
        self._deadline = now + self._timeouts.startup_listen

    def next_deadline(self) -> float | None:
        match self._state:
            case State.FOLLOWER:
                return self._last_contact + self._timeouts.coordinator_timeout
            case State.COORDINATOR:
                return self._next_heartbeat
            case _:
                return self._deadline

    def tick(self, now: float) -> None:
        deadline = self.next_deadline()

        if deadline is None or now < deadline:
            return

        match self._state:
            case State.STARTING | State.AWAITING_COORDINATOR | State.FOLLOWER:
                if self._state is State.FOLLOWER:
                    logger.warning(
                        "Coordinator %s sent nothing for %.2fs: starting election",
                        self._coordinator_id,
                        now - self._last_contact,
                    )
                self._begin_election(now)
            case State.ELECTING:
                self._win(now)
            case State.COORDINATOR:
                self._broadcast(HEARTBEAT, self._coordinator_epoch)
                self._next_heartbeat = now + self._timeouts.heartbeat_interval

    def handle(self, message: dict[str, Any], now: float) -> None:
        message_type = message.get("type")
        sender = message.get("sender")
        epoch = message.get("epoch")

        if message_type not in BULLY_MESSAGE_TYPES:
            return

        if type(sender) is not int or sender not in self._members:
            return

        if sender == self._id or type(epoch) is not int or epoch < 0:
            return

        match message_type:
            case "ELECTION":
                self._on_election(sender, epoch, now)
            case "OK":
                self._on_ok(sender, epoch, now)
            case _:
                self._on_claim(sender, epoch, now)

    def _on_election(self, sender: int, epoch: int, now: float) -> None:
        if sender > self._id:
            return

        self._note_epoch(epoch)
        self._send(sender, self._message(OK, self._known_epoch))

        if self._state is State.COORDINATOR:
            # Re-announce instead of electing again: a node that rejoins must not
            # cost the cluster an epoch.
            self._send(sender, self._message(COORDINATOR, self._coordinator_epoch))
        elif self._state in (State.STARTING, State.FOLLOWER):
            self._begin_election(now)

    def _on_ok(self, sender: int, epoch: int, now: float) -> None:
        self._note_epoch(epoch)

        if sender > self._id and self._state is State.ELECTING:
            self._state = State.AWAITING_COORDINATOR
            self._deadline = now + self._timeouts.announcement_timeout

    def _on_claim(self, sender: int, epoch: int, now: float) -> None:
        claim = (epoch, sender)

        if self._state is State.COORDINATOR:
            if claim > (self._coordinator_epoch, self._id):
                self._follow(sender, epoch, now)
            else:
                # Tell the stale coordinator to step down.
                self._send(sender, self._message(COORDINATOR, self._coordinator_epoch))
            return

        if epoch < self._known_epoch:
            return

        if sender < self._id:
            # A lower node cannot rule while a higher one is alive.
            self._note_epoch(epoch)

            if self._state in (State.STARTING, State.ELECTING):
                return

            if self._state is State.AWAITING_COORDINATOR:
                return

            current = (self._coordinator_epoch, self._coordinator_id)
            if self._coordinator_id is not None and claim <= current:
                return

            self._begin_election(now)
            return

        current_id = -1 if self._coordinator_id is None else self._coordinator_id
        if claim >= (self._coordinator_epoch, current_id):
            self._follow(sender, epoch, now)

    def _begin_election(self, now: float) -> None:
        was_coordinator = self._state is State.COORDINATOR
        self._state = State.ELECTING
        self._coordinator_id = None

        if was_coordinator:
            self._notify_role(False)

        if not self._higher:
            self._win(now)
            return

        self._deadline = now + self._timeouts.election_timeout
        for member in self._higher:
            self._send(member, self._message(ELECTION, self._known_epoch))

    def _win(self, now: float) -> None:
        epoch = self._known_epoch + 1
        self._note_epoch(epoch)
        self._state = State.COORDINATOR
        self._coordinator_id = self._id
        self._coordinator_epoch = epoch
        self._next_heartbeat = now + self._timeouts.heartbeat_interval
        logger.info("Node %s is coordinator for epoch %s", self._id, epoch)
        self._broadcast(COORDINATOR, epoch)
        self._notify_role(True)

    def _follow(self, coordinator_id: int, epoch: int, now: float) -> None:
        was_coordinator = self._state is State.COORDINATOR
        self._state = State.FOLLOWER
        self._coordinator_id = coordinator_id
        self._coordinator_epoch = epoch
        self._last_contact = now
        self._note_epoch(epoch)

        if was_coordinator:
            logger.info(
                "Node %s steps down for coordinator %s", self._id, coordinator_id
            )
            self._notify_role(False)

    def _note_epoch(self, epoch: int) -> None:
        if epoch <= self._known_epoch:
            return

        self._known_epoch = epoch

        if self._on_epoch_change is not None:
            self._on_epoch_change(epoch)

    def _notify_role(self, is_coordinator: bool) -> None:
        if self._on_role_change is not None:
            self._on_role_change(is_coordinator, self._coordinator_epoch)

    def _message(self, message_type: str, epoch: int) -> dict[str, Any]:
        return {"type": message_type, "sender": self._id, "epoch": epoch}

    def _broadcast(self, message_type: str, epoch: int) -> None:
        message = self._message(message_type, epoch)
        for member in self._others:
            self._send(member, dict(message))
