from abc import ABC, abstractmethod


class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddleware(ABC):
    @abstractmethod
    def start_consuming(self, on_message_callback) -> None:
        pass

    @abstractmethod
    def stop_consuming(self) -> None:
        pass

    @abstractmethod
    def send(self, message: bytes) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class MessageMiddlewareQueue(MessageMiddleware):
    @abstractmethod
    def __init__(self, host: str, queue_name: str) -> None:
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    @abstractmethod
    def __init__(
        self,
        host: str,
        exchange_name: str,
        routing_keys: list[str],
    ) -> None:
        pass
