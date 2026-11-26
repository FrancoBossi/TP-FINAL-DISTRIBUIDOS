import http.server
import os
import socket
import socketserver
import tempfile
import threading
import unittest

from failure_manager.restarter import DockerRestarter, RestartError, RestartOrder

ORDER = RestartOrder(
    target_id="preprocessor-1", incarnation_id="abc", coordinator_epoch=4
)


class FakeDockerDaemon:
    """A real HTTP server on a unix socket standing in for the Docker daemon."""

    def __init__(self, directory, status=204, body=b""):
        self.requests = []
        self.path = os.path.join(directory, "docker.sock")
        daemon = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                daemon.requests.append((self.command, self.path))
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                daemon.requests.append((self.command, self.path))
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_args):
                pass

        self.server = socketserver.ThreadingUnixStreamServer(self.path, Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


@unittest.skipUnless(hasattr(socket, "AF_UNIX"), "needs unix sockets")
class TestDockerRestarter(unittest.TestCase):
    def daemon(self, **kwargs):
        # Unix socket paths are short, so avoid a long temporary directory name.
        directory = tempfile.TemporaryDirectory(dir="/tmp")
        self.addCleanup(directory.cleanup)
        daemon = FakeDockerDaemon(directory.name, **kwargs)
        self.addCleanup(daemon.close)
        return daemon

    def test_issues_exactly_one_static_restart_call(self):
        daemon = self.daemon()
        restarter = DockerRestarter(daemon.path, ["preprocessor-1", "q1-0"])

        restarter.restart(ORDER)

        self.assertEqual(
            daemon.requests, [("POST", "/containers/preprocessor-1/restart?t=0")]
        )

    def test_the_kill_delay_is_configurable(self):
        daemon = self.daemon()
        restarter = DockerRestarter(
            daemon.path, ["preprocessor-1"], kill_after_seconds=7
        )

        restarter.restart(ORDER)

        self.assertEqual(
            daemon.requests[0][1], "/containers/preprocessor-1/restart?t=7"
        )

    def test_repeating_the_same_order_is_allowed(self):
        daemon = self.daemon()
        restarter = DockerRestarter(daemon.path, ["preprocessor-1"])

        restarter.restart(ORDER)
        restarter.restart(ORDER)

        self.assertEqual(len(daemon.requests), 2)

    def test_refuses_containers_outside_the_static_topology_without_calling_docker(
        self,
    ):
        daemon = self.daemon()
        restarter = DockerRestarter(daemon.path, ["preprocessor-1"])

        with self.assertRaises(RestartError):
            restarter.restart(RestartOrder("rabbitmq", None, 1))

        self.assertEqual(daemon.requests, [])

    def test_unexpected_answers_are_reported_with_the_daemon_message(self):
        daemon = self.daemon(status=404, body=b'{"message":"No such container"}')
        restarter = DockerRestarter(daemon.path, ["preprocessor-1"])

        with self.assertRaisesRegex(RestartError, "404.*No such container"):
            restarter.restart(ORDER)

    def test_a_server_error_is_reported(self):
        daemon = self.daemon(status=500, body=b"boom")
        restarter = DockerRestarter(daemon.path, ["preprocessor-1"])

        with self.assertRaisesRegex(RestartError, "500"):
            restarter.restart(ORDER)

    def test_a_missing_socket_is_reported(self):
        restarter = DockerRestarter("/tmp/no-such-docker.sock", ["preprocessor-1"])

        with self.assertRaises(RestartError):
            restarter.restart(ORDER)

    def test_only_restart_requests_are_ever_made(self):
        daemon = self.daemon()
        restarter = DockerRestarter(daemon.path, ["preprocessor-1"])

        restarter.restart(ORDER)

        self.assertTrue(
            all(
                method == "POST" and path.endswith("/restart?t=0")
                for method, path in daemon.requests
            )
        )

    def test_rejects_a_negative_kill_delay(self):
        with self.assertRaises(ValueError):
            DockerRestarter("/x", [], kill_after_seconds=-1)


if __name__ == "__main__":
    unittest.main()
