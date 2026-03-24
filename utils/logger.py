import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger that writes to stdout.
    Format: [LEVEL] YYYY-MM-DD HH:MM:SS | name | message
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="[%(levelname)s] %(asctime)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
