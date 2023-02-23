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

    log_func(f"Done in {t1 - t0:.3f}s")
