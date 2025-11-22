"""Logging utilities."""

from __future__ import annotations

import logging
import os
from typing import Any


def get_logger(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    return logging.getLogger(name)


def log_dataclass(instance: Any, logger: logging.Logger | None = None) -> None:
    logger = logger or get_logger(__name__)
    logger.debug("Dataclass configuration", extra={"config": instance})


__all__ = ["get_logger", "log_dataclass"]
