from contextlib import contextmanager
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
    log_dir.mkdir(exist_ok=True)
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


@contextmanager
def add_console_logging():
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger = logging.getLogger() 
    logger.addHandler(console_handler)

    try:
        yield
    finally:
        logger.removeHandler(console_handler)


def make_verbose(callable: Callable) -> Callable:
    def wrapped(*args, verbose=False, **kwargs):
        if verbose:
            with add_console_logging():
                return callable(*args, **kwargs)
        else:
            return callable(*args, **kwargs)

    return wrapped
