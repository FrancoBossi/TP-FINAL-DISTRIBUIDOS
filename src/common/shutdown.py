"""Signal handlers request a drain. Resource cleanup stays on the owner thread."""

import signal
from collections.abc import Callable
from contextlib import contextmanager


@contextmanager
def shutdown_signals(stop: Callable[[], None]):
    requested = False

    def request_stop(_signum, _frame):
        nonlocal requested
        if not requested:
            requested = True
            stop()

    previous = {}
    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, request_stop)
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
