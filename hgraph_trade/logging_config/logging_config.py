"""
logging_config.py

Centralised logging configuration for all hgraph_platform_tools modules.

Call ``setup_logging()`` once at application startup (e.g. in main.py or a CLI
entry point). Individual modules should simply do::

    import logging
    logger = logging.getLogger(__name__)

The root logger's level can be overridden after setup by passing ``level``
to ``setup_logging()``, or by setting the ``LOG_LEVEL`` environment variable.
"""

import logging
import logging.config
import os
from typing import Dict, Optional


# Consistent format across the entire platform
_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    config: Optional[Dict] = None,
) -> None:
    """
    Configure logging for the entire application.

    :param level: Log level string (e.g. "DEBUG", "INFO"). Falls back to the
                  ``LOG_LEVEL`` environment variable, then defaults to "INFO".
    :param log_file: Optional file path. If provided, a file handler is added
                     alongside the console handler.
    :param config: An optional full ``dictConfig`` dictionary. If provided,
                   ``level`` and ``log_file`` are ignored.
    """
    if config is not None:
        logging.config.dictConfig(config)
        return

    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()

    handlers: Dict[str, dict] = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "DEBUG",
        }
    }

    if log_file is not None:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "formatter": "standard",
            "level": "DEBUG",
            "filename": log_file,
            "mode": "a",
            "encoding": "utf-8",
        }

    dict_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": _DEFAULT_FORMAT,
                "datefmt": _DEFAULT_DATE_FORMAT,
            }
        },
        "handlers": handlers,
        "root": {
            "handlers": list(handlers.keys()),
            "level": level,
        },
    }

    logging.config.dictConfig(dict_config)
