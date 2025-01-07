# logging_config/logging_config.py

import logging
import logging.config
from typing import Optional, Dict

def setup_logging(config: Optional[Dict] = None):
    """
    Set up logging configuration for the entire application.

    :param config: An optional dictionary for custom logging configuration.
                   If not provided, a default configuration is used.
    """

    # If a custom config dictionary is not provided, use a default setup
    if config is None:
        config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "level": "DEBUG"
                }
            },
            "root": {
                "handlers": ["console"],
                "level": "INFO"
            },
            # If you want different loggers for certain modules, define them here:
            # e.g. "hgraph_trade_booker": { ... }, "hgraph_trade_model": { ... }
        }

    logging.config.dictConfig(config)