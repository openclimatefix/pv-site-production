"""Profiling utils."""

import contextlib
import logging
import time

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def profile(msg: str | None = None, level: str = "info"):
    """Context manager that logs the execution time.

    Arguments:
    ---------
    msg: (Optional) message to log before the execution.
    level: Log level (e.g. "info" or "debug") for both the message and the execusion time log.
    """
    log_func = getattr(logger, level.lower())

    if msg:
        log_func(msg)

    t0 = time.time()
    yield
    t1 = time.time()

    done_line = f"Done in {t1 - t0:.3f}s"

    # Repeating the `msg` in case of nested calls, where it's hard to know what it corresponds to.
    if msg:
        done_line = msg + " - " + done_line

    log_func(done_line)
