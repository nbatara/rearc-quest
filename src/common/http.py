"""HTTP session helpers with required User-Agent."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict

import requests

from common.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class BaseRequestSession:
    contact_email: str

    def _headers(self) -> Dict[str, str]:
        return {"User-Agent": f"rearc-quest/1.0 ({self.contact_email})"}

    def get_json(self, url: str) -> Dict[str, Any]:
        LOGGER.debug("Fetching JSON", extra={"url": url})
        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return json.loads(response.text)

    def get_text(self, url: str) -> str:
        LOGGER.debug("Fetching text", extra={"url": url})
        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.text

    def get_bytes(self, url: str) -> bytes:
        LOGGER.debug("Fetching bytes", extra={"url": url})
        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.content


@dataclass
class BLSRequestSession(BaseRequestSession):
    """Session configured for BLS access."""


@dataclass
class DataUSARequestSession(BaseRequestSession):
    """Session configured for DataUSA access."""


__all__ = [
    "BLSRequestSession",
    "DataUSARequestSession",
    "BaseRequestSession",
]
