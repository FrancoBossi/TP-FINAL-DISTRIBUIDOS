import logging
import socket
from collections.abc import Callable

from gateway.broker import RabbitMQBroker
from gateway.handler import handle_client


def serve(
    host: str, port: int, broker_factory: Callable[[str], RabbitMQBroker]
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(1)
        logging.info("Gateway listening on %s:%s", host, port)
        while True:
            client_socket, address = server.accept()
            with client_socket:
                logging.info("Client connected: %s", address)
                try:
                    handle_client(client_socket, broker_factory)
                except Exception:
                    logging.debug("Closing failed client connection: %s", address)
