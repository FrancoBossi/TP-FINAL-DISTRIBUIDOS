"""Three real FailureManagers over real sockets, supervising fake workers.

Nothing here sleeps to synchronize: every step waits on an observable condition.
Restarts are simulated by replacing the target with a fresh instance (a new
``incarnation_id``), which is what ``docker restart`` produces for a process.
"""

import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from common.health import HealthServer, Liveness, ping
from failure_manager.config import FailureManagerConfig
from failure_manager.main import build_manager
from failure_manager.restarter import RestartOrder
from failure_manager.topology import Target, failure_manager_name
from failure_manager.transport import PeerTransport

MANAGER_IDS = (0, 1, 2)
FAST_SETTINGS = {
    "FM_HEARTBEAT_INTERVAL": "0.05",
    "FM_COORDINATOR_TIMEOUT": "0.4",
    "FM_ELECTION_TIMEOUT": "0.15",
    "FM_ANNOUNCEMENT_TIMEOUT": "0.4",
    "FM_STARTUP_LISTEN": "0.3",
    "FM_PING_INTERVAL": "0.05",
    "FM_PING_TIMEOUT": "0.3",
    "FM_PING_ATTEMPTS": "2",
    "FM_FAILURE_THRESHOLD": "2",
    "FM_RESTART_COOLDOWN": "0.5",
    "FM_STARTUP_GRACE": "5",
    "FM_INITIAL_GRACE": "0.5",
}
_config_lock = threading.Lock()


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_until(predicate, timeout=15.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class FakeWorker:
    """A supervised process: a real health endpoint with a controllable loop."""

    def __init__(self, name):
        self.name = name
        self.port = free_port()
        self.server = None
        self._stop_ticks = threading.Event()
        self.incarnations = []

    @property
    def target(self):
        return Target(self.name, "127.0.0.1", self.port, self.name)

    @property
    def incarnation(self):
        return self.incarnations[-1]

    def start(self):
        liveness = Liveness(idle_max_seconds=0.4)
        self.server = HealthServer(
            self.name, port=self.port, host="127.0.0.1", liveness=liveness
        )
        self.server.start()
        liveness.mark_ready()
        self.incarnations.append(self.server.incarnation_id)
        self._stop_ticks = threading.Event()
        stop = self._stop_ticks

        def tick():
            while not stop.wait(0.05):
                liveness.tick()

        threading.Thread(target=tick, daemon=True).start()

    def hang(self):
        """The process stays alive and reachable, but its loop stops advancing."""
        self._stop_ticks.set()

    def kill(self):
        self._stop_ticks.set()
        if self.server is not None:
            self.server.close()
            self.server = None

    def answers(self):
        try:
            return ping("127.0.0.1", self.port, 0.3)
        except (OSError, EOFError):
            return None


class FakeRestarter:
    def __init__(self, world, manager_id):
        self._world = world
        self._manager_id = manager_id

    def restart(self, order: RestartOrder):
        self._world.record(self._manager_id, order)
        self._world.revive(order.target_id)


class ManagerProcess:
    def __init__(self, world, manager_id, state_dir):
        self.world = world
        self.manager_id = manager_id
        self.state_dir = state_dir
        self.port = free_port()
        self.manager = None
        self.thread = None
        self._parts = None

    @property
    def node(self):
        return self.manager._node

    @property
    def alive(self):
        return self._parts is not None

    def start(self):
        peers = ",".join(
            f"{i}=127.0.0.1:{self.world.managers[i].port}" for i in MANAGER_IDS
        )
        environment = {
            **FAST_SETTINGS,
            "FM_ID": str(self.manager_id),
            "FM_PEERS": peers,
            "FM_STATE_DIR": str(self.state_dir),
        }
        with _config_lock, patch.dict(os.environ, environment, clear=True):
            config = FailureManagerConfig()

        targets = tuple(w.target for w in self.world.workers.values()) + tuple(
            Target(
                failure_manager_name(i),
                "127.0.0.1",
                self.world.managers[i].port,
                failure_manager_name(i),
            )
            for i in MANAGER_IDS
            if i != self.manager_id
        )
        liveness = Liveness(idle_max_seconds=5)
        transport = PeerTransport(config.peers, config.manager_id, 0.3)
        manager = build_manager(
            config,
            transport,
            targets=targets,
            restarter=FakeRestarter(self.world, self.manager_id),
            liveness=liveness,
        )
        health = HealthServer(
            failure_manager_name(self.manager_id),
            port=self.port,
            host="127.0.0.1",
            liveness=liveness,
            handler=manager.on_message,
        )
        health.start()
        transport.start()
        health.set_ready()
        thread = threading.Thread(target=manager.run, daemon=True)
        thread.start()
        self.manager, self.thread = manager, thread
        self._parts = (health, transport)

    def kill(self):
        if self._parts is None:
            return
        health, transport = self._parts
        self._parts = None
        self.world.note(f"manager {self.manager_id} killed")
        self.manager.stop()
        self.thread.join(10)
        health.close()
        transport.close()


class World:
    def __init__(self, worker_names, directory):
        self.orders = []
        self._baseline = 0
        self.trace = []
        self.t0 = time.monotonic()
        self.closed = False
        self._lock = threading.Lock()
        self._revive_lock = threading.Lock()
        self.workers = {name: FakeWorker(name) for name in worker_names}
        self.managers = {
            i: ManagerProcess(self, i, Path(directory) / f"fm-{i}") for i in MANAGER_IDS
        }

    def note(self, event):
        with self._lock:
            self.trace.append(f"{time.monotonic() - self.t0:8.3f}s {event}")

    def record(self, manager_id, order):
        with self._lock:
            self.orders.append((manager_id, order))
        self.note(f"manager {manager_id} orders restart of {order.target_id}")

    def mark_baseline(self):
        """Ignore orders issued so far: a scenario starts once the group is stable.

        An order emitted while the group was still settling (a late PONG under
        load) says nothing about how it reacts to the failure being injected.
        """
        with self._lock:
            self._baseline = len(self.orders)

    def ordered(self, target=None):
        with self._lock:
            return [
                (m, o)
                for m, o in self.orders[self._baseline :]
                if target is None or o.target_id == target
            ]

    def revive(self, name):
        with self._revive_lock:
            if self.closed:
                return
            if name in self.workers:
                worker = self.workers[name]
                worker.kill()
                worker.start()
                return
            manager = next(
                m
                for m in self.managers.values()
                if failure_manager_name(m.manager_id) == name
            )
            manager.kill()
            manager.start()

    def start(self):
        for worker in self.workers.values():
            worker.start()
        for manager in self.managers.values():
            manager.start()

    def stop(self):
        # A restart still in flight must not bring a process back after teardown.
        with self._revive_lock:
            self.closed = True
        for manager in self.managers.values():
            manager.kill()
        for worker in self.workers.values():
            worker.kill()

    def coordinators(self):
        return [
            m.manager_id
            for m in self.managers.values()
            if m.alive and m.node.is_coordinator
        ]

    def settled_on(self, manager_id):
        live = [m for m in self.managers.values() if m.alive]
        return self.coordinators() == [manager_id] and all(
            m.node.is_coordinator or m.node.coordinator_id == manager_id for m in live
        )


class SupervisionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.world = World(("w0", "w1", "w2", "w3"), directory.name)
        self.addCleanup(self.world.stop)
        self.world.start()
        self.assertTrue(
            wait_until(lambda: self.world.settled_on(2)), "managers did not elect 2"
        )
        self.world.mark_baseline()

    def restarted(self, name):
        return wait_until(lambda: self.world.ordered(name))

    def test_a_dead_worker_is_restarted_by_the_coordinator_only(self):
        worker = self.world.workers["w1"]
        old_incarnation = worker.incarnation
        epoch = self.world.managers[2].node.epoch

        worker.kill()

        self.assertTrue(self.restarted("w1"), "w1 was never restarted")
        (manager_id, order) = self.world.ordered("w1")[0]
        self.assertEqual(manager_id, 2)
        self.assertEqual(order.coordinator_epoch, epoch)
        self.assertEqual(order.incarnation_id, old_incarnation)
        self.assertTrue(wait_until(lambda: worker.answers() is not None))
        self.assertNotEqual(worker.answers()["incarnation_id"], old_incarnation)

    def test_healthy_processes_are_left_alone(self):
        self.world.workers["w0"].kill()
        self.assertTrue(self.restarted("w0"))

        self.assertTrue(wait_until(lambda: self.world.workers["w0"].answers()))
        restarted = {order.target_id for _m, order in self.world.ordered()}
        self.assertEqual(restarted, {"w0"})

    def test_an_alive_but_stuck_process_is_restarted(self):
        worker = self.world.workers["w2"]
        old_incarnation = worker.incarnation
        self.assertEqual(worker.answers()["status"], "OK")

        worker.hang()

        self.assertTrue(self.restarted("w2"), "a hung process was not replaced")
        self.assertEqual(self.world.ordered("w2")[0][1].incarnation_id, old_incarnation)
        self.assertTrue(
            wait_until(
                lambda: (
                    (worker.answers() or {}).get("incarnation_id") != old_incarnation
                )
            )
        )

    def test_several_simultaneous_failures_are_all_recovered(self):
        for name in ("w0", "w1", "w3"):
            self.world.workers[name].kill()

        for name in ("w0", "w1", "w3"):
            self.assertTrue(self.restarted(name), f"{name} was not restarted")

        self.assertEqual(
            {order.target_id for _m, order in self.world.ordered()}, {"w0", "w1", "w3"}
        )

    def test_a_new_coordinator_is_elected_and_keeps_supervising(self):
        first_epoch = self.world.managers[2].node.epoch

        self.world.managers[2].kill()

        self.assertTrue(wait_until(lambda: self.world.coordinators() == [1]))
        self.assertGreater(self.world.managers[1].node.epoch, first_epoch)

        self.world.workers["w0"].kill()
        self.assertTrue(self.restarted("w0"))
        self.assertEqual(self.world.ordered("w0")[0][0], 1)

    def test_the_new_coordinator_revives_the_failed_manager_which_rejoins(self):
        self.world.managers[2].kill()

        self.assertTrue(
            self.restarted("failure-manager-2"), "the dead manager was not restarted"
        )
        self.assertEqual(self.world.ordered("failure-manager-2")[0][0], 1)
        self.assertTrue(wait_until(lambda: self.world.settled_on(2)))
        self.assertGreaterEqual(self.world.managers[2].node.epoch, 3)

    def test_one_surviving_manager_recovers_the_others_and_a_worker(self):
        self.world.managers[2].kill()
        self.world.managers[1].kill()
        self.world.workers["w3"].kill()

        for name in ("failure-manager-1", "failure-manager-2", "w3"):
            self.assertTrue(self.restarted(name), f"{name} was not recovered")

        # Only manager 0 was alive when the two others went down, so it had to
        # take over and order their restarts itself.
        trace = "\n".join(self.world.trace)
        self.assertEqual(self.world.ordered("failure-manager-1")[0][0], 0, trace)
        self.assertEqual(self.world.ordered("failure-manager-2")[0][0], 0, trace)
        self.assertTrue(wait_until(lambda: self.world.settled_on(2)))

    def test_a_coordinator_that_loses_leadership_stops_ordering_restarts(self):
        self.world.managers[2].kill()
        self.world.managers[1].kill()
        self.assertTrue(self.restarted("failure-manager-2"))

        # Manager 2 is back and outranks manager 0, which must step down.
        self.assertTrue(wait_until(lambda: self.world.settled_on(2)))
        self.assertFalse(self.world.managers[0].node.is_coordinator)

        self.world.workers["w1"].kill()

        self.assertTrue(self.restarted("w1"))
        self.assertEqual({m for m, _o in self.world.ordered("w1")}, {2})


if __name__ == "__main__":
    unittest.main()
