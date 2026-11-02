import logging
import socket
import threading
from collections.abc import Callable

from gateway.handler import handle_client
from gateway.messaging import GatewayMessaging

ACCEPT_TIMEOUT_SECONDS = 1.0


class ClientSession:
    def __init__(
        self,
        client_socket: socket.socket,
        address,
        messaging_factory: Callable[[str], GatewayMessaging],
        stop_event: threading.Event,
    ) -> None:
        self._socket = client_socket
        self._address = address
        self._messaging_factory = messaging_factory
        self._stop_event = stop_event
        self._messaging: GatewayMessaging | None = None
        self._lock = threading.Lock()
        self._stopped = False
        self._thread = threading.Thread(target=self._run)

    def _create_messaging(self, execution_id: str) -> GatewayMessaging:
        messaging = self._messaging_factory(execution_id)
        try:
            with self._lock:
                self._messaging = messaging
                if self._stopped or self._stop_event.is_set():
                    messaging.stop_consuming_results()
        except Exception:
            with self._lock:
                self._messaging = None
            messaging.close()
            raise
        return messaging

    def _run(self) -> None:
        try:
            serve_client(
                self._socket,
                self._address,
                self._create_messaging,
                self._stop_event,
            )
        finally:
            with self._lock:
                self._messaging = None

    def start(self) -> None:
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            try:
                if self._messaging is not None:
                    self._messaging.stop_consuming_results()
            finally:
                try:
                    self._socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    # A completed session may already have closed its socket.
                    logging.debug("Client socket already closed: %s", self._address)

    def join(self) -> None:
        self._thread.join()


def serve(
    host: str,
    port: int,
    messaging_factory: Callable[[str], GatewayMessaging],
    stop_event: threading.Event,
) -> None:
    sessions: list[ClientSession] = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()
        server.settimeout(ACCEPT_TIMEOUT_SECONDS)

        logging.info("Gateway listening on %s:%s", host, port)

        try:
            while not stop_event.is_set():
                try:
                    client_socket, address = server.accept()
                except socket.timeout:
                    continue
                session = ClientSession(
                    client_socket, address, messaging_factory, stop_event
                )
                active_sessions = []
                for previous in sessions:
                    if previous.is_alive():
                        active_sessions.append(previous)
                    else:
                        previous.join()
                sessions = active_sessions
                try:
                    session.start()
                except Exception:
                    client_socket.close()
                    raise
                sessions.append(session)
        finally:
            stop_event.set()
            try:
                for session in sessions:
                    try:
                        session.stop()
                    except Exception:
                        logging.exception("Failed to request client session shutdown")
            finally:
                for session in sessions:
                    session.join()


def serve_client(
    client_socket, address, messaging_factory, stop_event: threading.Event
) -> None:
    with client_socket:
        logging.info("Client connected: %s", address)
        try:
            handle_client(client_socket, messaging_factory, stop_event=stop_event)
        except Exception:
            logging.exception("Closing failed client connection: %s", address)
