import logging
import time
import os

LOGGING_ENV_EQUIVALENCE = {
    "INFO": logging.INFO,
    "WARN": logging.WARN,
    "ERROR": logging.ERROR,
    "DEBUG": logging.DEBUG,
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
    "NOTSET": logging.NOTSET
}


def get_logging_level(level: str) -> logging:
    logging_level = LOGGING_ENV_EQUIVALENCE.get(level)
    if logging_level is None:
        raise ValueError("Logging level {} is not supported".format(level))

    return logging_level


def get_handler(formatter):
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    return handler


def create_logger() -> logging.Logger:
    log_level = get_logging_level(os.getenv("LOG_LEVEL", "INFO"))
    log_format = "[%(asctime)s] [%(levelname)s] [%(lineno)s] [%(module)s] %(message)s"
    log_timestamp_format = "%Y-%m-%dT%H:%M:%S%z"

    formatter = logging.Formatter(log_format, log_timestamp_format)
    formatter.converter = time.gmtime

    handler = get_handler(formatter)
    default_logger = logging.getLogger(__name__)
    default_logger.setLevel(log_level)
    default_logger.addHandler(handler)

    return default_logger


logger = create_logger()