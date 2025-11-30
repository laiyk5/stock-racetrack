import logging
import logging.config
import sys

from rich.logging import RichHandler

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "rich.logging.RichHandler",
            "level": "INFO",
            "formatter": "simple",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "srt.log",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
        },
    },
    "loggers": {
        "srt": {"level": "DEBUG", "handlers": ["console", "file"], "propagate": False}
    },
}


def config_logger():
    logging.config.dictConfig(LOGGING_CONFIG)
