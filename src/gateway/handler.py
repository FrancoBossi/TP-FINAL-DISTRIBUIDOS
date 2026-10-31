import logging
import socket
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
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
    Message,
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
    try:
        send_msg(client_socket, AckMessage())
    except OSError:
        if not input_finished:
            raise

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


@dataclass
class ResultForwardingState:
    discard: bool = False
    completed: set[int] = field(default_factory=set)


def validate_execution_result(
    message: Message, execution_id: str, completed: set[int]
) -> int:
    if not isinstance(message, (ResultMessage, QueryEndMessage)):
        raise ValueError("Expected RESULT or QUERY_END")
    if message.execution_id != execution_id:
        raise ValueError("Result belongs to a different execution")
    return validate_result(message, completed)


def deliver_result(
    client_socket: socket.socket,
    message: Message,
    execution_id: str,
    state: ResultForwardingState,
) -> None:
    if state.discard:
        return
    try:
        send_msg(client_socket, message)
        if not isinstance(recv_msg(client_socket), AckMessage):
            raise ValueError("Expected ACK for result")
    except (EOFError, OSError):
        state.discard = True
        logger.info("Execution %s: discarding remaining results", execution_id)


def on_result(
    message: Message,
    ack: Acknowledgement,
    nack: Acknowledgement,
    *,
    client_socket: socket.socket,
    messaging: GatewayMessaging,
    execution_id: str,
    state: ResultForwardingState,
) -> None:
    try:
        query_id = validate_execution_result(message, execution_id, state.completed)
        deliver_result(client_socket, message, execution_id, state)
    except Exception:
        nack()
        messaging.stop_consuming_results()
        raise

    ack()

    if isinstance(message, QueryEndMessage):
        state.completed.add(query_id)
        logger.info(
            "Execution %s: query %d complete (%d/5)",
            execution_id,
            query_id,
            len(state.completed),
        )

    if state.completed == QUERY_IDS:
        messaging.stop_consuming_results()


def forward_results(
    client_socket: socket.socket,
    messaging: GatewayMessaging,
    execution_id: str,
    *,
    discard: bool = False,
) -> None:
    state = ResultForwardingState(discard=discard)

    logger.info(
        "Execution %s: waiting for results on queue %s",
        execution_id,
        messaging.result_queue_name,
    )

    callback = partial(
        on_result,
        client_socket=client_socket,
        messaging=messaging,
        execution_id=execution_id,
        state=state,
    )

    messaging.start_consuming_results(callback)

    if state.completed != QUERY_IDS:
        raise EOFError("Results ended before all queries completed")


def handle_client(
    client_socket: socket.socket,
    messaging_factory: Callable[[str], GatewayMessaging],
) -> None:
    execution_id = uuid4().hex
    messaging = None
    published = 0
    input_finished = False

    logger.info("Execution %s: starting upload", execution_id)

    try:
        messaging = messaging_factory(execution_id)
        logger.debug(
            "Execution %s: messaging connections ready",
            execution_id,
        )

        discard = False
        try:
            while not input_finished:
                published, input_finished = forward_message(
                    client_socket,
                    messaging,
                    execution_id,
                    published,
                )
        except (EOFError, OSError):
            messaging.publish(
                RoutedInputEndMessage(
                    execution_id=execution_id,
                    reply_queue=messaging.result_queue_name,
                )
            )
            discard = True
            logger.info(
                "Execution %s: client disconnected; finishing input", execution_id
            )

        forward_results(client_socket, messaging, execution_id, discard=discard)
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
