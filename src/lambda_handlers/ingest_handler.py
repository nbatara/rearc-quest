"""Lambda entrypoint for ingest workflow."""

from __future__ import annotations

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from bls_sync import BLSSyncConfig, perform_sync
from datausa_fetch import DataUSAConfig, fetch_and_store
from common.logging import get_logger

LOGGER = get_logger(__name__)


def handler(event, context):
    """AWS Lambda handler to run BLS sync and population fetch."""

    contact_email = os.environ.get("CONTACT_EMAIL", "data@example.com")
    bucket = os.environ.get("DATA_BUCKET", "placeholder-bucket")
    bls_prefix = os.environ.get("BLS_PREFIX", "bls/")
    population_raw_prefix = os.environ.get("POPULATION_RAW_PREFIX", "population/raw/")
    population_table_prefix = os.environ.get(
        "POPULATION_TABLE_PREFIX", "population/tables/"
    )

    bls_config = BLSSyncConfig(
        bucket=bucket, prefix=bls_prefix, contact_email=contact_email
    )
    datausa_config = DataUSAConfig(
        bucket=bucket,
        raw_prefix=population_raw_prefix,
        table_prefix=population_table_prefix,
        contact_email=contact_email,
    )

    LOGGER.info("Starting ingest workflow")
    bls_result = perform_sync(bls_config)
    LOGGER.info(
        "BLS sync complete",
        extra={
            "uploaded": len(bls_result.uploaded),
            "deleted": len(bls_result.deleted),
        },
    )
    fetch_and_store(datausa_config)
    LOGGER.info("Population fetch complete")
    return {"status": "ok", "bls": bls_result.__dict__}


if __name__ == "__main__":
    # This block is for local testing only
    os.environ["CONTACT_EMAIL"] = "test@test.com"
    os.environ["DATA_BUCKET"] = "rearc-quest-testing-bucket"
    os.environ["BLS_PREFIX"] = "bls_data/"
    os.environ["POPULATION_RAW_PREFIX"] = "population_data/raw/"
    os.environ["POPULATION_TABLE_PREFIX"] = "population_data/tables/"
    print(handler(None, None))


__all__ = ["handler"]
