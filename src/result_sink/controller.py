import logging

from common.message_protocol.external import validate_result
from common.message_protocol.message import Message
from common.middleware.middleware import MessageMiddlewareUnroutableError
from result_sink.messaging import Ack, Nack, ResultSinkMessaging

logger = logging.getLogger(__name__)


class ResultSinkController:
    """Forward results immediately; upstream currently emits one end per query."""

    def __init__(self, messaging: ResultSinkMessaging) -> None:
        self._messaging = messaging

    def process_message(self, message: Message, ack: Ack, nack: Nack) -> None:
        try:
            validate_result(message, set())
            if not isinstance(message.execution_id, str) or not message.execution_id:
                raise ValueError("Result requires a nonempty execution_id")
            self._messaging.publish(message)
        except MessageMiddlewareUnroutableError:
            logger.warning(
                "Discarding result for execution %s: destination results.%s is gone",
                message.execution_id,
                message.execution_id,
            )
        except Exception:
            nack()
            raise
        ack()

    def start(self) -> None:
        self._messaging.start_consuming(self.process_message)

    def stop(self) -> None:
        self._messaging.stop_consuming()
