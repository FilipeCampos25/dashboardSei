from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional


_NOISY_LOGGERS = (
    "selenium",
    "selenium.webdriver",
    "selenium.webdriver.remote.remote_connection",
    "selenium.webdriver.common.selenium_manager",
    "urllib3",
    "urllib3.connectionpool",
)


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
    level: Optional[str] = None,
    logger_name: str = "dashboard_sei",
) -> logging.Logger:
    """Configure global logging and reduce third-party noise."""
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    output_dir = Path(os.getenv("OUTPUT_DIR", "output")).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_log_path = output_dir / "execution_log_latest.json"

    text_handler = logging.StreamHandler()
    text_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )

    json_handler = logging.FileHandler(json_log_path, mode="w", encoding="utf-8")
    json_handler.setFormatter(JsonLineFormatter())

    logging.basicConfig(
        level=numeric_level,
        handlers=[text_handler, json_handler],
        force=True,
    )

    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logger = logging.getLogger(logger_name)
    logger.info("Log JSON habilitado em: %s", json_log_path)
    return logger


def setup_logger(logger_name: str = "dashboard_sei") -> logging.Logger:
    """Compatibility wrapper used by modules that import setup_logger."""
    return logging.getLogger(logger_name)
