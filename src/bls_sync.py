"""BLS dataset synchronizer for Lambda.

This module crawls the BLS time series index, compares against existing S3
objects, and uploads new or changed files while removing stale ones.
Only lightweight logic is provided here; connect AWS services via the
helpers in :mod:`common.aws`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List
import boto3

from common.aws import S3Location, S3SyncResult, ensure_bucket_prefix
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
    """List available objects from the BLS index page."""

    LOGGER.info("Crawling BLS index", extra={"base_url": base_url})
    html_content = session.get_text(base_url)
    filenames = re.findall(r">(pr\..*?)<\/a>", html_content, re.IGNORECASE)
    LOGGER.info(f"Found {len(filenames)} files in index.", extra={"files": filenames})
    return filenames


def perform_sync(config: BLSSyncConfig) -> S3SyncResult:
    """Run the BLS sync workflow.

    This function initializes the HTTP session with the required User-Agent,
    collects index metadata, and synchronizes the files with S3.
    """
    session = BLSRequestSession(contact_email=config.contact_email)
    ensure_bucket_prefix(config.bucket, config.prefix)
    
    desired_keys = crawl_index(session, config.index_url)
    desired_set = set(desired_keys)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=config.bucket, Prefix=config.prefix)
    existing_keys = set(obj["Key"].split("/")[-1] for page in pages for obj in page.get("Contents", []))

    uploads = sorted(desired_set - existing_keys)
    deletes = sorted(existing_keys - desired_set)

    for key in uploads:
        file_url = f"{config.index_url}{key}"
        LOGGER.info(f"Uploading {key} from {file_url}")
        content = session.get_bytes(file_url)
        s3.put_object(
            Bucket=config.bucket,
            Key=f"{config.prefix}{key}",
            Body=content,
        )

    for key in deletes:
        LOGGER.info(f"Deleting stale object {key}")
        s3.delete_object(Bucket=config.bucket, Key=f"{config.prefix}{key}")

    return S3SyncResult(uploaded=uploads, deleted=deletes)


__all__ = [
    "BLSSyncConfig",
    "perform_sync",
    "crawl_index",
]

if __name__ == "__main__":
    from moto import mock_aws
    import boto3
    import responses

    # This block is for local testing only
    responses.add_passthru("https://download.bls.gov")

    with mock_aws():
        config = BLSSyncConfig(
            bucket="rearc-quest-testing-bucket",
            prefix="bls_data/",
            contact_email="test@test.com",
        )
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=config.bucket)
        
        result = perform_sync(config)
        print(f"Sync complete. Uploaded: {len(result.uploaded)}, Deleted: {len(result.deleted)}")