from contextlib import contextmanager
from functools import wraps
import logging
import logging.handlers
from pathlib import Path
import sys
from typing import Callable

from spells.cache import data_home


def setup_logging(
    log_level=logging.DEBUG,
    log_file_name="spells.log",
    max_bytes=5_000_000,  # 5MB
    backup_count=3,
    log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
):
    log_dir = Path(data_home()) / ".logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir / log_file_name

    handler = logging.handlers.RotatingFileHandler(
        filename=log_file_path, maxBytes=max_bytes, backupCount=backup_count
    )

    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    root_logger.handlers.clear()
    root_logger.addHandler(handler)


def rotate_logs():
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            handler.doRollover()
            logging.debug("Log file manually rotated")


def add_console_handler(log_level):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(console_handler)
    return logger, console_handler


@contextmanager
def console_logging(log_level):
    logger, console_handler = add_console_handler(log_level)
    try:
        yield
    finally:
        logger.removeHandler(console_handler)


def make_verbose(level: int | None = None) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapped(*args, log_to_console: int | None = level, **kwargs):
            if log_to_console is None:
                return func(*args, **kwargs)
            with console_logging(log_to_console):
                return func(*args, **kwargs)

        return wrapped

    return decorator
