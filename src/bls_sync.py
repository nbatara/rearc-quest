"""BLS dataset synchronizer for Lambda.

This module crawls the BLS time series index, compares against existing S3
objects, and uploads new or changed files while removing stale ones.
Only lightweight logic is provided here; connect AWS services via the
helpers in :mod:`common.aws`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from common.aws import InMemoryS3Client, S3Location, S3SyncResult, ensure_bucket_prefix, sync_s3_objects
from common.http import BLSRequestSession
from common.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class BLSSyncConfig:
    """Configuration for the BLS sync job."""

    bucket: str
    prefix: str
    contact_email: str
    index_url: str = "https://download.bls.gov/pub/time.series/pr/"

    def destination(self) -> S3Location:
        """Return the base S3 location for the sync."""

        return S3Location(bucket=self.bucket, prefix=self.prefix)


def crawl_index(session: BLSRequestSession, base_url: str) -> List[str]:
    """List available objects from the BLS index page.

    The current implementation returns an empty list placeholder. Replace this
    logic with HTML parsing or directory listing as appropriate for the data
    source.
    """

    LOGGER.info("Crawling BLS index", extra={"base_url": base_url})
    # In the real solution you would parse the directory listing. Here we
    # return the two core files the analytics step expects so the flow is easy
    # to understand end-to-end.
    return ["pr.data.0.Current", "pr.series"]


def perform_sync(config: BLSSyncConfig, client: InMemoryS3Client | None = None) -> S3SyncResult:
    """Run the BLS sync workflow.

    This function initializes the HTTP session with the required User-Agent,
    collects index metadata, and delegates synchronization to the shared S3
    helpers.
    """

    session = BLSRequestSession(contact_email=config.contact_email)
    ensure_bucket_prefix(config.bucket, config.prefix)
    objects: Iterable[str] = crawl_index(session, config.index_url)
    LOGGER.info(
        "Starting S3 sync",
        extra={"bucket": config.bucket, "prefix": config.prefix, "objects": list(objects)},
    )
    return sync_s3_objects(destination=config.destination(), object_keys=objects, client=client)


__all__ = [
    "BLSSyncConfig",
    "perform_sync",
    "crawl_index",
]
