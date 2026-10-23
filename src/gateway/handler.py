import logging
import socket
from collections.abc import Callable
from uuid import uuid4

from common.message_protocol.external import (
    QUERY_IDS,
    recv_msg,
    send_msg,
    validate_result,
)
from common.message_protocol.message import (
    AckMessage,
    ArticleMessage,
    ErrorMessage,
    InputEndMessage,
    QueryEndMessage,
    ResultMessage,
    RoutedArticleMessage,
    RoutedInputEndMessage,
)
from gateway.messaging import Acknowledgement, GatewayMessaging

LOG_BATCH_SIZE = 1000
logger = logging.getLogger(__name__)


def forward_message(
    client_socket: socket.socket,
    messaging: GatewayMessaging,
    execution_id: str,
    published: int,
) -> tuple[int, bool]:
    input_finished = False
    message = recv_msg(client_socket)

    match message:
        case ArticleMessage(article=dict() as article):
            routed = RoutedArticleMessage(
                article=article,
                execution_id=execution_id,
                reply_queue=messaging.result_queue_name,
            )
        case ArticleMessage():
            raise ValueError("ARTICLE requires an article object")
        case InputEndMessage():
            routed = RoutedInputEndMessage(
                execution_id=execution_id,
                reply_queue=messaging.result_queue_name,
            )
            input_finished = True
        case _:
            raise ValueError("Expected ARTICLE or INPUT_END")

    messaging.publish(routed)

    if not input_finished:
        published += 1

    logger.debug(
        "Execution %s: published %s",
        execution_id,
        routed.type,
    )
    send_msg(client_socket, AckMessage())

    if input_finished:
        logger.info(
            "Execution %s: upload complete; %d articles and INPUT_END published",
            execution_id,
            published,
        )
    elif published % LOG_BATCH_SIZE == 0:
        logger.info(
            "Execution %s: %d articles published",
            execution_id,
            published,
        )

    return published, input_finished


def send_error_message(
    client_socket: socket.socket,
    error: Exception,
    execution_id: str,
) -> None:
    try:
        send_msg(client_socket, ErrorMessage(message=str(error)))
    except OSError:
        logger.debug(
            "Execution %s: could not send error to client",
            execution_id,
        )


def forward_results(
    client_socket: socket.socket,
    messaging: GatewayMessaging,
    execution_id: str,
) -> None:
    completed: set[int] = set()

    logger.info(
        "Execution %s: waiting for results on queue %s",
        execution_id,
        messaging.result_queue_name,
    )

    def on_result(
        message,
        ack: Acknowledgement,
        nack: Acknowledgement,
    ) -> None:
        try:
            if not isinstance(message, (ResultMessage, QueryEndMessage)):
                raise ValueError("Expected RESULT or QUERY_END")

            if message.execution_id != execution_id:
                raise ValueError("Result belongs to a different execution")

            query_id = validate_result(message, completed)
            send_msg(client_socket, message)

            if not isinstance(recv_msg(client_socket), AckMessage):
                raise ValueError("Expected ACK for result")
        except Exception:
            nack()
            messaging.stop_consuming_results()
            raise

        ack()

        if isinstance(message, QueryEndMessage):
            completed.add(query_id)
            logger.info(
                "Execution %s: query %d complete (%d/5)",
                execution_id,
                query_id,
                len(completed),
            )

        if completed == QUERY_IDS:
            messaging.stop_consuming_results()

    messaging.start_consuming_results(on_result)

    if completed != QUERY_IDS:
        raise EOFError("Results ended before all queries completed")


def handle_client(
    client_socket: socket.socket,
    messaging_factory: Callable[[str], GatewayMessaging],
) -> None:
    execution_id = uuid4().hex
    messaging = None
    published = 0

    logger.info("Execution %s: starting upload", execution_id)

    try:
        messaging = messaging_factory(execution_id)
        logger.debug(
            "Execution %s: messaging connections ready",
            execution_id,
        )

        input_finished = False

        while not input_finished:
            published, input_finished = forward_message(
                client_socket,
                messaging,
                execution_id,
                published,
            )

        forward_results(
            client_socket,
            messaging,
            execution_id,
        )
    except Exception as error:
        logger.exception(
            "Execution %s: failed after %d articles published",
            execution_id,
            published,
        )
        send_error_message(
            client_socket,
            error,
            execution_id,
        )
        raise
    finally:
        if messaging is not None:
            messaging.close()
            logger.debug(
                "Execution %s: messaging connections closed",
                execution_id,
            )
