import os
import re
import socket
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from common.control_wire import recv_frame
from failure_manager.config import FailureManagerConfig, parse_peers
from failure_manager.detector import Suspect
from failure_manager.manager import FailureManager
from failure_manager.topology import (
    PIPELINE_GROUPS,
    Target,
    failure_manager_name,
    pipeline_targets,
    targets_for,
)
from failure_manager.transport import PeerTransport

COMPOSE = Path(__file__).resolve().parent.parent / "docker-compose.yaml"


class TestTopology(unittest.TestCase):
    def test_targets_are_unique_and_named_by_group_and_index(self):
        names = [target.name for target in pipeline_targets()]

        self.assertEqual(len(names), len(set(names)))
        self.assertIn("preprocessor-2", names)
        self.assertIn("image-fetcher-1", names)
        self.assertNotIn("image-fetcher-2", names)
        self.assertEqual(names[-1], "result-sink")
        self.assertEqual(len(names), sum(n for _g, n in PIPELINE_GROUPS) + 1)

    def test_gateway_rabbitmq_and_client_are_not_supervised(self):
        names = {target.name for target in pipeline_targets()}

        self.assertFalse(names & {"gateway", "rabbitmq", "client"})

    def test_host_container_and_name_are_the_same_static_string(self):
        for target in pipeline_targets():
            self.assertEqual({target.host, target.container}, {target.name})
            self.assertEqual(target.port, 7000)

    def test_a_manager_checks_the_other_managers_but_never_itself(self):
        names = {target.name for target in targets_for(1, (0, 1, 2))}

        self.assertIn("failure-manager-0", names)
        self.assertIn("failure-manager-2", names)
        self.assertNotIn("failure-manager-1", names)

    def test_matches_the_compose_deployment(self):
        text = COMPOSE.read_text()
        services = {}
        current = None
        for line in text.splitlines():
            match = re.match(r"^  ([a-z0-9][a-z0-9_-]*):\s*$", line)
            if match:
                current = match.group(1)
                services[current] = []
            elif current:
                services[current].append(line)

        supervised = {
            name
            for name, body in services.items()
            if any(re.search(r"^\s+HEALTH_NAME:", row) for row in body)
        }
        expected = {t.name for t in pipeline_targets()} | {
            failure_manager_name(i) for i in (0, 1, 2)
        }
        self.assertEqual(supervised, expected)

        for name in supervised:
            body = "\n".join(services[name])
            with self.subTest(service=name):
                self.assertRegex(
                    body, r"(?m)" + rf'HEALTH_NAME:\s*"?{re.escape(name)}"?\s*$'
                )
                self.assertRegex(
                    body, r"(?m)" + rf"container_name:\s*{re.escape(name)}\s*$"
                )

        for name in ("gateway", "rabbitmq", "client"):
            self.assertNotIn(name, supervised)

    def test_only_the_failure_managers_mount_the_docker_socket(self):
        text = COMPOSE.read_text()
        owners = set()
        current = None
        for line in text.splitlines():
            match = re.match(r"^  ([a-z0-9][a-z0-9_-]*):\s*$", line)
            if match:
                current = match.group(1)
            if "docker.sock" in line and current:
                owners.add(current)

        self.assertEqual(
            owners - {"cadvisor"},
            {failure_manager_name(i) for i in (0, 1, 2)},
        )

    def test_failure_manager_membership_in_compose_is_parseable(self):
        text = COMPOSE.read_text()
        expected = {
            0: ("failure-manager-0", 7000),
            1: ("failure-manager-1", 7000),
            2: ("failure-manager-2", 7000),
        }
        memberships = re.findall(r"^\s+FM_PEERS:\s*(.+)$", text, re.MULTILINE)

        self.assertEqual(len(memberships), 3)
        for membership in memberships:
            self.assertEqual(parse_peers(membership.strip()), expected)


class TestConfig(unittest.TestCase):
    ENV = {
        "FM_ID": "1",
        "FM_PEERS": "0=failure-manager-0:7000, 1=failure-manager-1:7000,2=failure-manager-2:7000",
    }

    def test_parses_peers(self):
        self.assertEqual(
            parse_peers("0=a:7000,2=b:7001"), {0: ("a", 7000), 2: ("b", 7001)}
        )

    def test_rejects_malformed_peers(self):
        for raw in ("", "x=a:1", "0=a", "0=:1", "0=a:b", "0"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_peers(raw)

    def test_reads_defaults_and_overrides_from_the_environment(self):
        with patch.dict(os.environ, {**self.ENV, "FM_PING_TIMEOUT": "0.2"}, clear=True):
            config = FailureManagerConfig()

        self.assertEqual(config.manager_ids, (0, 1, 2))
        self.assertEqual(config.detection.ping_timeout, 0.2)
        self.assertEqual(config.detection.failure_threshold, 2)
        self.assertEqual(config.bully.heartbeat_interval, 0.5)
        self.assertEqual(config.docker_socket, "/var/run/docker.sock")

    def test_the_manager_must_be_listed_among_the_peers(self):
        with patch.dict(os.environ, {**self.ENV, "FM_ID": "7"}, clear=True):
            with self.assertRaises(ValueError):
                FailureManagerConfig()

    def test_non_positive_timings_are_rejected(self):
        for name in (
            "FM_HEARTBEAT_INTERVAL",
            "FM_PING_INTERVAL",
            "FM_COORDINATOR_TIMEOUT",
        ):
            with self.subTest(name=name):
                with patch.dict(os.environ, {**self.ENV, name: "0"}, clear=True):
                    with self.assertRaises(ValueError):
                        FailureManagerConfig()


class TestManagerShutdown(unittest.TestCase):
    def test_stop_revokes_authority_before_an_in_flight_restart(self):
        entered_confirmation = threading.Event()
        release_confirmation = threading.Event()
        detector = Mock()

        def confirm(_suspect):
            entered_confirmation.set()
            release_confirmation.wait(timeout=2)
            return True

        detector.confirm.side_effect = confirm
        restarter = Mock()
        manager = FailureManager(Mock(), detector, restarter, ping_interval=1)
        manager.on_role_change(True, 7)
        suspect = Suspect(
            target=Target.named("worker-0"),
            incarnation_id="old-incarnation",
            failures=2,
            first_failure_at=0,
            reason="no answer to PING",
        )

        manager._dispatch(suspect, 7)
        self.assertTrue(entered_confirmation.wait(timeout=2))
        manager.stop()
        release_confirmation.set()
        manager._restarts.shutdown(wait=True)

        restarter.restart.assert_not_called()


class TestPeerTransport(unittest.TestCase):
    def listener(self):
        server = socket.socket()
        self.addCleanup(server.close)
        server.bind(("127.0.0.1", 0))
        server.listen(8)
        return server

    def receive(self, server, count):
        messages = []
        server.settimeout(3)
        for _ in range(count):
            connection, _ = server.accept()
            with connection:
                connection.settimeout(3)
                messages.append(recv_frame(connection))
        return messages

    def test_delivers_to_the_right_peer_and_never_to_itself(self):
        one, two = self.listener(), self.listener()
        peers = {
            0: ("127.0.0.1", 1),
            1: ("127.0.0.1", one.getsockname()[1]),
            2: ("127.0.0.1", two.getsockname()[1]),
        }
        transport = PeerTransport(peers, own_id=0)
        transport.start()
        self.addCleanup(transport.close)

        transport.send(1, {"type": "ELECTION", "sender": 0, "epoch": 2})
        transport.send(2, {"type": "HEARTBEAT", "sender": 0, "epoch": 2})
        transport.send(0, {"type": "OK", "sender": 0, "epoch": 2})

        self.assertEqual(self.receive(one, 1)[0]["type"], "ELECTION")
        self.assertEqual(self.receive(two, 1)[0]["type"], "HEARTBEAT")

    def test_a_dead_peer_does_not_delay_the_others(self):
        dead = socket.socket()
        dead.bind(("127.0.0.1", 0))
        dead_port = dead.getsockname()[1]
        dead.close()
        alive = self.listener()
        peers = {
            0: ("127.0.0.1", 1),
            1: ("127.0.0.1", dead_port),
            2: ("127.0.0.1", alive.getsockname()[1]),
        }
        transport = PeerTransport(peers, own_id=0)
        transport.start()
        self.addCleanup(transport.close)

        started = time.monotonic()
        for _ in range(100):
            transport.send(1, {"type": "ELECTION", "sender": 0, "epoch": 1})
        transport.send(2, {"type": "COORDINATOR", "sender": 0, "epoch": 1})

        self.assertEqual(self.receive(alive, 1)[0]["type"], "COORDINATOR")
        self.assertLess(time.monotonic() - started, 2)

    def test_send_never_blocks_when_the_queue_overflows(self):
        blocked = threading.Event()
        peers = {0: ("127.0.0.1", 1), 1: ("10.255.255.1", 9)}
        transport = PeerTransport(peers, own_id=0)
        self.addCleanup(blocked.set)

        started = time.monotonic()
        for index in range(500):
            transport.send(1, {"type": "HEARTBEAT", "sender": 0, "epoch": index})

        self.assertLess(time.monotonic() - started, 1)
        self.assertLessEqual(transport._queues[1].qsize(), 32)
        self.assertEqual(transport._queues[1].queue[-1]["epoch"], 499)


class TestTarget(unittest.TestCase):
    def test_named_targets_use_the_default_port(self):
        self.assertEqual(Target.named("q1-0"), Target("q1-0", "q1-0", 7000, "q1-0"))


if __name__ == "__main__":
    unittest.main()
