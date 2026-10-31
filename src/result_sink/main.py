import logging
import signal
from functools import partial
from types import FrameType

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import MessagePublisherRabbitMQ
from result_sink.config import ResultSinkConfig, configure_logging
from result_sink.controller import ResultSinkController
from result_sink.messaging import ResultSinkMessaging

logger = logging.getLogger(__name__)


def create_messaging(
    mom_host: str,
    input_queue_name: str,
) -> ResultSinkMessaging:
    input_queue = MessageMiddlewareQueueRabbitMQ(
        host=mom_host,
        queue_name=input_queue_name,
    )
    try:
        publisher = MessagePublisherRabbitMQ(host=mom_host)
    except Exception:
        input_queue.close()
        raise

    return ResultSinkMessaging(
        input_queue=input_queue,
        publisher=publisher,
    )


def handle_sigterm(
    controller: ResultSinkController,
    signum: int,
    frame: FrameType | None,
) -> None:
    controller.stop()


def main() -> int:
    configure_logging()
    config = ResultSinkConfig()

    messaging = create_messaging(
        mom_host=config.mom_host,
        input_queue_name=config.input_queue,
    )
    controller = ResultSinkController(
        messaging=messaging,
    )

    signal.signal(
        signal.SIGTERM,
        partial(handle_sigterm, controller),
    )

    logger.info(
        "ResultSink consuming from %s and streaming to results.<execution_id>",
        config.input_queue,
    )

    try:
        controller.start()
    except KeyboardInterrupt:
        return 0
    finally:
        messaging.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
