from __future__ import annotations

import logging
import os
from typing import Optional


_NOISY_LOGGERS = (
    "selenium",
    "selenium.webdriver",
    "selenium.webdriver.remote.remote_connection",
    "selenium.webdriver.common.selenium_manager",
    "urllib3",
    "urllib3.connectionpool",
)


def setup_logging(
    level: Optional[str] = None,
    logger_name: str = "dashboard_sei",
) -> logging.Logger:
    """Configure global logging and reduce third-party noise."""
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )

    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(logger_name)


def setup_logger(logger_name: str = "dashboard_sei") -> logging.Logger:
    """Compatibility wrapper used by modules that import setup_logger."""
    return logging.getLogger(logger_name)
