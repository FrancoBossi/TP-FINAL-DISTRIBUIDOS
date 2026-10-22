import logging
import select
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
from gateway.broker import RabbitMQBroker

LOG_BATCH_SIZE = 1000
logger = logging.getLogger(__name__)


def forward_message(
    client_socket: socket.socket,
    broker: RabbitMQBroker,
    run_id: str,
    published: int,
) -> tuple[int, bool]:
    input_finished = False
    message = recv_msg(client_socket)
    match message:
        case ArticleMessage(article=dict() as article):
            routed = RoutedArticleMessage(article, run_id, broker.reply_queue)
        case ArticleMessage():
            raise ValueError("ARTICLE requires an article object")
        case InputEndMessage():
            routed = RoutedInputEndMessage(run_id, broker.reply_queue)
            input_finished = True
        case _:
            raise ValueError("Expected ARTICLE or INPUT_END")
    broker.publish(routed)
    if not input_finished:
        published += 1
    logger.debug("Run %s: published %s", run_id, routed.type)
    send_msg(client_socket, AckMessage())
    if input_finished:
        logger.info(
            "Run %s: upload complete; %d articles and INPUT_END published",
            run_id,
            published,
        )
    elif published % LOG_BATCH_SIZE == 0:
        logger.info("Run %s: %d articles published", run_id, published)
    return published, input_finished


def send_error_message(
    client_socket: socket.socket, error: Exception, run_id: str
) -> None:
    try:
        send_msg(client_socket, ErrorMessage(message=str(error)))
    except OSError:
        logger.debug("Run %s: could not send error to client", run_id)


def forward_results(
    client_socket: socket.socket, broker: RabbitMQBroker, run_id: str
) -> None:
    completed: set[int] = set()
    logger.info("Run %s: waiting for results on queue %s", run_id, broker.reply_queue)
    for delivery in broker.results():
        if delivery is None:
            if select.select([client_socket], [], [], 0)[0]:
                raise EOFError("Client disconnected or sent unexpected data")
            continue
        delivery_tag, message = delivery
        if not isinstance(message, (ResultMessage, QueryEndMessage)):
            raise ValueError("Expected RESULT or QUERY_END")
        if message.run_id != run_id:
            raise ValueError("Result belongs to a different run")
        query_id = validate_result(message, completed)
        send_msg(client_socket, message)
        if not isinstance(recv_msg(client_socket), AckMessage):
            raise ValueError("Expected ACK for result")
        broker.acknowledge_result(delivery_tag)
        if isinstance(message, QueryEndMessage):
            completed.add(query_id)
            logger.info(
                "Run %s: query %d complete (%d/5)", run_id, query_id, len(completed)
            )
        if completed == QUERY_IDS:
            return
    raise EOFError("Results ended before all queries completed")


def handle_client(
    client_socket: socket.socket, broker_factory: Callable[[str], RabbitMQBroker]
) -> None:
    run_id = uuid4().hex
    broker = None
    published = 0
    logger.info("Run %s: starting upload", run_id)
    try:
        broker = broker_factory(run_id)
        logger.debug("Run %s: broker connection ready", run_id)
        input_finished = False
        while not input_finished:
            published, input_finished = forward_message(
                client_socket, broker, run_id, published
            )
        forward_results(client_socket, broker, run_id)

    except Exception as error:
        logger.exception(
            "Run %s: failed after %d articles published", run_id, published
        )
        send_error_message(client_socket, error, run_id)
        raise
    finally:
        if broker is not None:
            broker.close()
            logger.debug("Run %s: broker connection closed", run_id)
