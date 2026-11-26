import itertools
import tempfile
import unittest
from pathlib import Path

from failure_manager.bully import (
    COORDINATOR,
    ELECTION,
    HEARTBEAT,
    OK,
    BullyNode,
    BullyTimeouts,
    State,
)
from failure_manager.epoch_store import EpochStore

IDS = (0, 1, 2)


class Cluster:
    """Three nodes over an in-memory network driven by a fake clock."""

    LATENCY = 0.01
    STEP = 0.01

    def __init__(self, ids=IDS, timeouts=None):
        self.ids = tuple(ids)
        self.timeouts = timeouts or BullyTimeouts()
        self.now = 0.0
        self.nodes = {}
        self.stored_epoch = {node_id: 0 for node_id in self.ids}
        self.pending = []
        self.sequence = itertools.count()
        self.role_changes = []
        for node_id in self.ids:
            self.boot(node_id)

    def boot(self, node_id):
        node = BullyNode(
            node_id,
            self.ids,
            send=lambda destination, message, source=node_id: self._send(
                source, destination, message
            ),
            timeouts=self.timeouts,
            known_epoch=self.stored_epoch[node_id],
            on_epoch_change=lambda epoch, owner=node_id: self.stored_epoch.__setitem__(
                owner, epoch
            ),
            on_role_change=lambda leads, epoch, owner=node_id: self.role_changes.append(
                (round(self.now, 2), owner, leads, epoch)
            ),
        )
        node.start(self.now)
        self.nodes[node_id] = node

    def kill(self, node_id):
        del self.nodes[node_id]
        self.pending = [item for item in self.pending if item[2] != node_id]

    def _send(self, source, destination, message):
        self.pending.append(
            (self.now + self.LATENCY, next(self.sequence), destination, message)
        )

    def run(self, seconds):
        end = self.now + seconds
        while self.now < end - 1e-9:
            self._step()

    def run_until(self, predicate, limit):
        start = self.now
        while self.now - start < limit:
            if predicate():
                return self.now - start
            self._step()
        self.fail(f"condition not reached within {limit}s")

    def _step(self):
        self.now = round(self.now + self.STEP, 6)
        due = sorted(item for item in self.pending if item[0] <= self.now)
        self.pending = [item for item in self.pending if item[0] > self.now]
        for _at, _sequence, destination, message in due:
            node = self.nodes.get(destination)
            if node is not None:
                node.handle(message, self.now)
        for node_id in sorted(self.nodes):
            self.nodes[node_id].tick(self.now)

    def coordinators(self):
        return [i for i, node in sorted(self.nodes.items()) if node.is_coordinator]

    def settled_on(self, coordinator_id):
        return self.coordinators() == [coordinator_id] and all(
            node.is_coordinator or node.coordinator_id == coordinator_id
            for node in self.nodes.values()
        )

    fail = staticmethod(lambda message: (_ for _ in ()).throw(AssertionError(message)))


class TestElectionInACluster(unittest.TestCase):
    def test_cold_start_elects_the_highest_id(self):
        cluster = Cluster()

        cluster.run_until(lambda: cluster.settled_on(2), limit=3)

        self.assertEqual({n.epoch for n in cluster.nodes.values()}, {1})
        self.assertEqual(cluster.stored_epoch, {0: 1, 1: 1, 2: 1})

    def test_coordinator_failure_elects_the_next_highest_with_a_new_epoch(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)

        cluster.kill(2)
        elapsed = cluster.run_until(lambda: cluster.settled_on(1), limit=6)

        timeouts = cluster.timeouts
        bound = (
            timeouts.coordinator_timeout
            + timeouts.election_timeout
            + 4 * Cluster.LATENCY
            + timeouts.heartbeat_interval
        )
        self.assertLessEqual(elapsed, bound)
        self.assertEqual({n.epoch for n in cluster.nodes.values()}, {2})

    def test_restarted_highest_node_takes_over_with_a_strictly_newer_epoch(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)
        cluster.kill(2)
        cluster.run_until(lambda: cluster.settled_on(1), limit=6)

        cluster.boot(2)
        cluster.run_until(lambda: cluster.settled_on(2), limit=6)

        self.assertEqual({n.epoch for n in cluster.nodes.values()}, {3})
        self.assertEqual(cluster.stored_epoch[2], 3)
        won = [c for c in cluster.role_changes if c[1] == 2 and c[2]]
        stepped_down = [c for c in cluster.role_changes if c[1] == 1 and not c[2]]
        self.assertEqual([c[3] for c in won], [1, 3])
        self.assertEqual(len(stepped_down), 1)
        self.assertGreaterEqual(stepped_down[0][0], won[-1][0])

    def test_restarted_lower_node_rejoins_without_costing_an_epoch(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)
        elections_before = len(cluster.role_changes)

        cluster.kill(0)
        cluster.boot(0)
        cluster.run_until(lambda: cluster.settled_on(2), limit=6)
        cluster.run(3)

        self.assertEqual({n.epoch for n in cluster.nodes.values()}, {1})
        self.assertEqual(len(cluster.role_changes), elections_before)

    def test_a_single_survivor_becomes_coordinator(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)

        cluster.kill(2)
        cluster.kill(1)
        cluster.run_until(lambda: cluster.settled_on(0), limit=8)

        self.assertEqual(cluster.nodes[0].epoch, 2)

    def test_two_failures_in_a_row_leave_the_remaining_node_in_charge(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)
        cluster.kill(2)
        cluster.run_until(lambda: cluster.settled_on(1), limit=6)

        cluster.kill(1)
        cluster.run_until(lambda: cluster.settled_on(0), limit=8)

        self.assertEqual(cluster.nodes[0].epoch, 3)

    def test_everything_recovers_after_the_whole_group_restarts(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)

        for node_id in IDS:
            cluster.kill(node_id)
        for node_id in IDS:
            cluster.boot(node_id)
        cluster.run_until(lambda: cluster.settled_on(2), limit=6)

        self.assertEqual({n.epoch for n in cluster.nodes.values()}, {2})

    def test_epoch_never_decreases_across_leaders(self):
        cluster = Cluster()
        cluster.run_until(lambda: cluster.settled_on(2), limit=3)
        for victim in (2, 1, 0):
            cluster.kill(victim)
            cluster.boot(victim)
            cluster.run(6)

        epochs = [change[3] for change in cluster.role_changes if change[2]]
        self.assertEqual(epochs, sorted(epochs))
        self.assertEqual(len(set(epochs)), len(epochs))


def recorder():
    sent = []
    return sent, lambda destination, message: sent.append((destination, message))


class TestNodeBehavior(unittest.TestCase):
    def make(self, node_id=0, known_epoch=0, **kwargs):
        sent, send = recorder()
        node = BullyNode(node_id, IDS, send, known_epoch=known_epoch, **kwargs)
        return node, sent

    def test_election_timeline_is_driven_only_by_the_clock(self):
        node, sent = self.make(0)
        node.start(0.0)

        node.tick(0.99)
        self.assertEqual(node.state, State.STARTING)
        self.assertEqual(sent, [])

        node.tick(1.0)
        self.assertEqual(node.state, State.ELECTING)
        self.assertEqual(
            sorted((d, m["type"]) for d, m in sent), [(1, ELECTION), (2, ELECTION)]
        )

        sent.clear()
        node.tick(1.74)
        self.assertEqual(node.state, State.ELECTING)
        self.assertEqual(sent, [])

        node.tick(1.75)
        self.assertEqual(node.state, State.COORDINATOR)
        self.assertEqual(node.epoch, 1)
        self.assertEqual(
            sorted((d, m["type"], m["epoch"]) for d, m in sent),
            [(1, COORDINATOR, 1), (2, COORDINATOR, 1)],
        )

    def test_ok_from_a_higher_node_postpones_leadership_then_retries(self):
        node, sent = self.make(0)
        node.start(0.0)
        node.tick(1.0)
        sent.clear()

        node.handle({"type": OK, "sender": 2, "epoch": 0}, 1.2)
        self.assertEqual(node.state, State.AWAITING_COORDINATOR)

        node.tick(3.19)
        self.assertEqual(node.state, State.AWAITING_COORDINATOR)

        node.tick(3.2)
        self.assertEqual(node.state, State.ELECTING)
        self.assertEqual(sorted(d for d, _m in sent), [1, 2])

    def test_coordinator_announcement_is_accepted_and_ends_the_election(self):
        node, _sent = self.make(0)
        node.start(0.0)
        node.tick(1.0)

        node.handle({"type": COORDINATOR, "sender": 2, "epoch": 4}, 1.1)

        self.assertEqual(node.state, State.FOLLOWER)
        self.assertEqual((node.coordinator_id, node.epoch), (2, 4))
        self.assertEqual(node.known_epoch, 4)

    def test_follower_elects_when_the_coordinator_goes_silent(self):
        node, sent = self.make(1)
        node.start(0.0)
        node.handle({"type": COORDINATOR, "sender": 2, "epoch": 3}, 1.0)

        node.tick(2.99)
        self.assertEqual(node.state, State.FOLLOWER)

        node.handle({"type": HEARTBEAT, "sender": 2, "epoch": 3}, 2.5)
        node.tick(4.49)
        self.assertEqual(node.state, State.FOLLOWER)

        node.tick(4.5)
        self.assertEqual(node.state, State.ELECTING)
        self.assertEqual([(d, m["type"]) for d, m in sent], [(2, ELECTION)])

    def test_stale_heartbeat_neither_regresses_nor_extends_the_coordinator(self):
        node, _sent = self.make(0)
        node.start(0.0)
        node.handle({"type": COORDINATOR, "sender": 2, "epoch": 3}, 1.0)
        deadline = node.next_deadline()

        node.handle({"type": HEARTBEAT, "sender": 1, "epoch": 2}, 1.5)
        node.handle({"type": COORDINATOR, "sender": 1, "epoch": 2}, 1.6)

        self.assertEqual((node.coordinator_id, node.epoch), (2, 3))
        self.assertEqual(node.next_deadline(), deadline)

    def test_stale_election_cannot_raise_a_lower_epoch_over_a_known_one(self):
        node, _sent = self.make(0, known_epoch=5)

        node.handle({"type": COORDINATOR, "sender": 2, "epoch": 4}, 1.0)

        self.assertNotEqual(node.state, State.FOLLOWER)
        self.assertIsNone(node.coordinator_id)

    def test_duplicate_coordinators_at_the_same_epoch_resolve_to_the_higher_id(self):
        low, low_sent = self.make(1)
        low.start(0.0)
        low.tick(1.0)
        low.tick(1.75)
        self.assertTrue(low.is_coordinator)
        epoch = low.epoch

        low.handle({"type": COORDINATOR, "sender": 2, "epoch": epoch}, 2.0)
        self.assertEqual(low.state, State.FOLLOWER)
        self.assertEqual(low.coordinator_id, 2)

        high, high_sent = self.make(2)
        high.start(0.0)
        high.tick(1.0)
        self.assertTrue(high.is_coordinator)
        high_sent.clear()

        high.handle({"type": COORDINATOR, "sender": 1, "epoch": high.epoch}, 2.0)
        self.assertTrue(high.is_coordinator)
        self.assertEqual(
            [(d, m["type"], m["epoch"]) for d, m in high_sent],
            [(1, COORDINATOR, high.epoch)],
        )

    def test_coordinator_answers_a_rejoining_node_without_a_new_epoch(self):
        node, sent = self.make(2)
        node.start(0.0)
        node.tick(1.0)
        sent.clear()

        node.handle({"type": ELECTION, "sender": 0, "epoch": 0}, 2.0)

        self.assertEqual(
            [(d, m["type"], m["epoch"]) for d, m in sent],
            [(0, OK, 1), (0, COORDINATOR, 1)],
        )
        self.assertEqual(node.epoch, 1)

    def test_coordinator_emits_heartbeats_on_schedule(self):
        node, sent = self.make(2)
        node.start(0.0)
        node.tick(1.0)
        sent.clear()

        node.tick(1.49)
        self.assertEqual(sent, [])

        node.tick(1.5)
        self.assertEqual(
            sorted((d, m["type"]) for d, m in sent), [(0, HEARTBEAT), (1, HEARTBEAT)]
        )
        sent.clear()

        node.tick(1.99)
        self.assertEqual(sent, [])
        node.tick(2.0)
        self.assertEqual(len(sent), 2)

    def test_lower_node_claiming_leadership_is_challenged_by_a_live_higher_node(self):
        node, sent = self.make(1)
        node.start(0.0)
        node.handle({"type": COORDINATOR, "sender": 2, "epoch": 3}, 1.0)
        sent.clear()

        node.handle({"type": COORDINATOR, "sender": 0, "epoch": 3}, 1.1)
        self.assertEqual(node.state, State.FOLLOWER)
        self.assertEqual(sent, [])

        node.handle({"type": COORDINATOR, "sender": 0, "epoch": 5}, 1.2)
        self.assertEqual(node.state, State.ELECTING)
        self.assertEqual([(d, m["type"]) for d, m in sent], [(2, ELECTION)])

    def test_restarted_highest_node_waits_to_learn_the_current_epoch(self):
        node, sent = self.make(2)
        node.start(0.0)

        node.handle({"type": HEARTBEAT, "sender": 1, "epoch": 6}, 0.4)
        self.assertEqual(node.state, State.STARTING)
        self.assertEqual(sent, [])

        node.tick(1.0)

        self.assertTrue(node.is_coordinator)
        self.assertEqual(node.epoch, 7)

    def test_starting_node_follows_a_higher_coordinator_it_hears_at_once(self):
        node, _sent = self.make(0)
        node.start(0.0)

        node.handle({"type": HEARTBEAT, "sender": 2, "epoch": 6}, 0.2)

        self.assertEqual(node.state, State.FOLLOWER)
        self.assertEqual(node.coordinator_id, 2)

    def test_invalid_messages_are_ignored(self):
        node, sent = self.make(1)
        node.start(0.0)
        invalid = [
            {},
            {"type": "PING", "sender": 0, "epoch": 0},
            {"type": ELECTION, "sender": 9, "epoch": 0},
            {"type": ELECTION, "sender": 1, "epoch": 0},
            {"type": ELECTION, "sender": True, "epoch": 0},
            {"type": ELECTION, "sender": "0", "epoch": 0},
            {"type": ELECTION, "sender": 0, "epoch": -1},
            {"type": ELECTION, "sender": 0, "epoch": "1"},
            {"type": ELECTION, "sender": 0},
            {"type": OK, "epoch": 0},
            {"type": COORDINATOR, "sender": 2, "epoch": 1.5},
        ]

        for message in invalid:
            with self.subTest(message=message):
                node.handle(message, 0.1)

        self.assertEqual(sent, [])
        self.assertEqual(node.state, State.STARTING)
        self.assertEqual(node.known_epoch, 0)

    def test_election_from_a_higher_node_is_ignored(self):
        node, sent = self.make(0)
        node.start(0.0)

        node.handle({"type": ELECTION, "sender": 2, "epoch": 0}, 0.1)

        self.assertEqual(sent, [])

    def test_known_epoch_survives_restart_and_is_reported_when_it_grows(self):
        changes = []
        node, _sent = self.make(2, known_epoch=9, on_epoch_change=changes.append)
        node.start(0.0)

        node.tick(1.0)

        self.assertEqual(node.epoch, 10)
        self.assertEqual(changes, [10])

    def test_rejects_invalid_configuration(self):
        with self.assertRaises(ValueError):
            BullyNode(5, IDS, lambda *_: None)

        with self.assertRaises(ValueError):
            BullyNode(0, IDS, lambda *_: None, known_epoch=-1)

        with self.assertRaises(ValueError):
            BullyTimeouts(heartbeat_interval=0)

        with self.assertRaises(ValueError):
            BullyTimeouts(heartbeat_interval=2.0, coordinator_timeout=2.0)


class TestEpochStore(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "state" / "epoch"

    def test_missing_file_means_epoch_zero(self):
        self.assertEqual(EpochStore(self.path).load(), 0)

    def test_saved_epoch_is_loaded_by_a_new_instance(self):
        EpochStore(self.path).save(7)

        self.assertEqual(EpochStore(self.path).load(), 7)

    def test_save_replaces_atomically_without_leaving_temporary_files(self):
        store = EpochStore(self.path)
        store.save(1)
        store.save(2)

        self.assertEqual([p.name for p in self.path.parent.iterdir()], ["epoch"])
        self.assertEqual(store.load(), 2)

    def test_unreadable_content_is_treated_as_zero(self):
        self.path.parent.mkdir(parents=True)
        self.path.write_text("not a number")

        self.assertEqual(EpochStore(self.path).load(), 0)

    def test_negative_epochs_are_rejected(self):
        with self.assertRaises(ValueError):
            EpochStore(self.path).save(-1)


if __name__ == "__main__":
    unittest.main()
