import os
import socket
import threading
import unittest
from unittest.mock import patch

from common.control_wire import ProtocolError, recv_frame, send_frame
from common.health import (
    STATUS_DEGRADED,
    STATUS_OK,
    STATUS_STARTING,
    HealthServer,
    Liveness,
    ping,
)


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class TestLiveness(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.liveness = Liveness(
            idle_max_seconds=5, busy_max_seconds=60, clock=self.clock
        )

    def test_reports_starting_until_marked_ready(self):
        self.assertEqual(self.liveness.status()[0], STATUS_STARTING)
        self.liveness.mark_ready()
        self.assertEqual(self.liveness.status()[0], STATUS_OK)

    def test_degrades_without_progress_and_recovers_with_a_tick(self):
        self.liveness.mark_ready()
        self.clock.advance(5)
        self.assertEqual(self.liveness.status()[0], STATUS_OK)

        self.clock.advance(0.5)
        status, detail = self.liveness.status()
        self.assertEqual(status, STATUS_DEGRADED)
        self.assertIn("no consumption progress", detail)

        self.liveness.tick()
        self.assertEqual(self.liveness.status()[0], STATUS_OK)

    def test_long_handler_is_ok_until_its_own_limit(self):
        self.liveness.mark_ready()

        with self.liveness.busy():
            self.clock.advance(60)
            self.assertEqual(self.liveness.status()[0], STATUS_OK)

            self.clock.advance(1)
            status, detail = self.liveness.status()
            self.assertEqual(status, STATUS_DEGRADED)
            self.assertIn("handler running", detail)

        self.assertEqual(self.liveness.status()[0], STATUS_OK)

    def test_nested_handlers_count_from_the_outermost(self):
        self.liveness.mark_ready()

        with self.liveness.busy():
            self.clock.advance(40)
            with self.liveness.busy():
                self.clock.advance(30)
                self.assertEqual(self.liveness.status()[0], STATUS_DEGRADED)
            self.assertEqual(self.liveness.status()[0], STATUS_DEGRADED)

        self.assertEqual(self.liveness.status()[0], STATUS_OK)

    def test_idle_clock_restarts_when_the_last_handler_ends(self):
        self.liveness.mark_ready()

        with self.liveness.busy():
            self.clock.advance(30)

        self.clock.advance(4)
        self.assertEqual(self.liveness.status()[0], STATUS_OK)

    def test_failed_or_raising_check_degrades_the_process(self):
        self.liveness.mark_ready()
        self.liveness.add_check("thread", lambda: False)
        self.assertEqual(
            self.liveness.status(), (STATUS_DEGRADED, "check failed: thread")
        )

        self.liveness.remove_check("thread")
        self.assertEqual(self.liveness.status()[0], STATUS_OK)

        def explode():
            raise RuntimeError("boom")

        self.liveness.add_check("explode", explode)
        self.assertEqual(self.liveness.status()[0], STATUS_DEGRADED)

    def test_rejects_non_positive_thresholds(self):
        with self.assertRaises(ValueError):
            Liveness(idle_max_seconds=0)

        with self.assertRaises(ValueError):
            Liveness(busy_max_seconds=-1)


class TestHealthServer(unittest.TestCase):
    def start_server(self, **kwargs):
        liveness = kwargs.pop("liveness", Liveness())
        server = HealthServer(
            name=kwargs.pop("name", "worker-0"),
            port=0,
            host="127.0.0.1",
            liveness=liveness,
            **kwargs,
        )
        server.start()
        self.addCleanup(server.close)
        return server, liveness

    def test_answers_ping_with_identity_and_status(self):
        server, liveness = self.start_server()

        answer = ping("127.0.0.1", server.port, timeout=2, nonce=11)

        self.assertEqual(answer["type"], "PONG")
        self.assertEqual(answer["name"], "worker-0")
        self.assertEqual(answer["incarnation_id"], server.incarnation_id)
        self.assertEqual(answer["status"], STATUS_STARTING)
        self.assertEqual(answer["nonce"], 11)

        liveness.mark_ready()
        self.assertEqual(ping("127.0.0.1", server.port, timeout=2)["status"], STATUS_OK)

    def test_each_process_instance_has_its_own_incarnation_id(self):
        first, _ = self.start_server()
        second, _ = self.start_server()

        self.assertNotEqual(first.incarnation_id, second.incarnation_id)

    def test_other_frames_reach_the_handler_without_an_answer(self):
        received = threading.Event()
        messages = []

        def handler(message):
            messages.append(message)
            received.set()

        server, _ = self.start_server(handler=handler)

        with socket.create_connection(("127.0.0.1", server.port), timeout=2) as peer:
            send_frame(peer, {"type": "HEARTBEAT", "epoch": 4})
            peer.settimeout(2)
            self.assertEqual(peer.recv(1), b"")

        self.assertTrue(received.wait(2))
        self.assertEqual(messages, [{"type": "HEARTBEAT", "epoch": 4}])

    def test_garbage_does_not_stop_the_endpoint(self):
        server, _ = self.start_server()

        with socket.create_connection(("127.0.0.1", server.port), timeout=2) as peer:
            peer.sendall(b"\x00\x00\x00\x05garbage")

        with socket.create_connection(("127.0.0.1", server.port), timeout=2) as peer:
            peer.sendall(b"\x00")

        self.assertEqual(ping("127.0.0.1", server.port, timeout=2)["type"], "PONG")

    def test_ping_to_a_closed_port_fails(self):
        server, _ = self.start_server()
        port = server.port
        server.close()

        with self.assertRaises(OSError):
            ping("127.0.0.1", port, timeout=1)

    def test_ping_rejects_an_answer_for_another_request(self):
        listener = socket.socket()
        self.addCleanup(listener.close)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)

        def answer_with_wrong_nonce():
            connection, _ = listener.accept()
            with connection:
                recv_frame(connection)
                send_frame(connection, {"type": "PONG", "nonce": 999})

        thread = threading.Thread(target=answer_with_wrong_nonce, daemon=True)
        thread.start()

        with self.assertRaises(ProtocolError):
            ping("127.0.0.1", listener.getsockname()[1], timeout=2, nonce=1)

        thread.join(2)

    def test_disabled_without_a_component_name(self):
        with patch.dict(os.environ, {}, clear=True):
            server = HealthServer.from_env()

        server.start()
        self.addCleanup(server.close)

        self.assertIsNone(server._server)

    def test_enabled_from_environment(self):
        environment = {"HEALTH_NAME": "q1-2", "HEALTH_PORT": "0"}
        with patch.dict(os.environ, environment, clear=True):
            server = HealthServer.from_env(liveness=Liveness())

        server._host = "127.0.0.1"
        server.start()
        self.addCleanup(server.close)

        self.assertEqual(ping("127.0.0.1", server.port, timeout=2)["name"], "q1-2")


if __name__ == "__main__":
    unittest.main()
