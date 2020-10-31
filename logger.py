import logging

from dataclasses import dataclass
from logging import Logger


@dataclass
class Logger(object):
    """
    This logger will be used to log search results.
    """
    @staticmethod
    def get_info_logger() -> Logger:
        logger = logging.getLogger()
        logger.setLevel(level=logging.INFO)

        return logger
