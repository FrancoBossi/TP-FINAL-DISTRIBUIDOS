import logging
import signal
from functools import partial

from common.middleware import MessageMiddlewareQueueRabbitMQ
from q5.comparator.buffer import CandidateBuffer
from q5.comparator.config import Q5ComparatorConfig, configure_logging
from q5.comparator.controller import Q5ComparatorController
from q5.comparator.messaging import Q5ComparatorMessaging

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    config = Q5ComparatorConfig()

    messaging = Q5ComparatorMessaging(
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.input_queue,
        ),
        MessageMiddlewareQueueRabbitMQ(
            config.mom_host,
            config.output_queue,
        ),
    )
    candidate_buffer = CandidateBuffer(config.buffer_directory)
    controller = Q5ComparatorController(
        messaging,
        candidate_buffer,
    )

    signal.signal(
        signal.SIGTERM,
        partial(lambda c, _s, _f: c.stop(), controller),
    )
    logger.info(
        "Q5Comparator consuming from %s with buffer in %s",
        config.input_queue,
        config.buffer_directory,
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
