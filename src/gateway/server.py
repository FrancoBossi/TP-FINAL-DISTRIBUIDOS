import logging
import socket
import threading
from collections.abc import Callable

from gateway.handler import handle_client
from gateway.messaging import GatewayMessaging


def serve(
    host: str,
    port: int,
    messaging_factory: Callable[[str], GatewayMessaging],
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()

        logging.info("Gateway listening on %s:%s", host, port)

        while True:
            client_socket, address = server.accept()

            threading.Thread(
                target=serve_client,
                args=(client_socket, address, messaging_factory),
                daemon=True,
            ).start()


def serve_client(client_socket, address, messaging_factory) -> None:
    with client_socket:
        logging.info("Client connected: %s", address)
        try:
            handle_client(client_socket, messaging_factory)
        except Exception:
            logging.exception("Closing failed client connection: %s", address)
