import logging
import signal
from functools import partial

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q4.top20_reducer.config import Q4Top20ReducerConfig, configure_logging
from q4.top20_reducer.controller import Q4Top20ReducerController
from q4.top20_reducer.messaging import Q4Top20ReducerMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    config = Q4Top20ReducerConfig()
    messaging = Q4Top20ReducerMessaging(
        MessageMiddlewareQueueRabbitMQ(config.mom_host, config.input_queue),
        MessageMiddlewareQueueRabbitMQ(config.mom_host, config.output_queue),
    )
    controller = Q4Top20ReducerController(messaging)
    signal.signal(signal.SIGTERM, partial(lambda c, _s, _f: c.stop(), controller))
    logger.info("Q4Top20Reducer consuming from %s", config.input_queue)
    try:
        controller.start()
    except KeyboardInterrupt:
        return 0
    finally:
        messaging.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
