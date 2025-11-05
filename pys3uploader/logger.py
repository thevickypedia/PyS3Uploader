"""Loads a default logger with StreamHandler set to DEBUG mode.

>>> logging.Logger

"""

import logging
import os
from datetime import datetime
from enum import IntEnum, StrEnum


class LogHandler(StrEnum):
    """Logging handlers to choose from when default logger is used.

    >>> LogHandler

    """

    file = "file"
    stdout = "stdout"


class LogLevel(IntEnum):
    """Logging levels to choose from when default logger is used.

    >>> LogLevel

    """

    debug = logging.DEBUG
    info = logging.INFO
    warning = logging.WARNING
    error = logging.ERROR

    @classmethod
    def _missing_(cls, value):
        """Allow constructing from string names."""
        if isinstance(value, str):
            value = value.lower()
            for member in cls:
                if member.name == value:
                    return member
        return None


def stream_handler() -> logging.StreamHandler:
    """Creates a ``StreamHandler`` and assigns a default format to it.

    Returns:
        logging.StreamHandler:
        Returns an instance of the ``StreamHandler`` object.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(fmt=default_format())
    return handler


def file_handler() -> logging.FileHandler:
    """Creates a ``StreamHandler`` and assigns a default format to it.

    Returns:
        logging.StreamHandler:
        Returns an instance of the ``StreamHandler`` object.
    """
    os.makedirs("logs", exist_ok=True)
    filename = os.path.join("logs", datetime.now().strftime("PyS3Uploader_%d-%m-%Y_%H:%M.log"))
    handler = logging.FileHandler(filename, mode="a")
    handler.setFormatter(fmt=default_format())
    return handler


def default_format() -> logging.Formatter:
    """Creates a logging ``Formatter`` with a custom message and datetime format.

    Returns:
        logging.Formatter:
        Returns an instance of the ``Formatter`` object.
    """
    return logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(funcName)s - %(message)s",
        datefmt="%b-%d-%Y %I:%M:%S %p",
    )


def setup_logger(handler: LogHandler, level: LogLevel) -> logging.Logger:
    """Creates a default logger with debug mode enabled.

    Args:
        handler: Logging handler to use.
        level: Logging level to use.

    Returns:
        logging.Logger:
        Returns an instance of the ``Logger`` object.
    """
    logger = logging.getLogger(__name__)
    if handler == LogHandler.file:
        logger.addHandler(hdlr=file_handler())
    elif handler == LogHandler.stdout:
        logger.addHandler(hdlr=stream_handler())

    logger.setLevel(level)
    return logger
