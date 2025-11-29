"""Lambda entrypoint for analytics workflow."""

from __future__ import annotations

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from analytics import AnalyticsConfig, run_analytics
from common.aws import S3Location
from common.logging import get_logger

LOGGER = get_logger(__name__)


def handler(event, context):
    """AWS Lambda handler that processes analytics when triggered."""

    bucket = os.environ.get("DATA_BUCKET", "placeholder-bucket")
    bls_prefix = os.environ.get("BLS_PREFIX", "bls/")
    population_table_prefix = os.environ.get(
        "POPULATION_TABLE_PREFIX", "population/tables/"
    )

    config = AnalyticsConfig(
        bls_location=S3Location(bucket=bucket, prefix=bls_prefix),
        population_location=S3Location(bucket=bucket, prefix=population_table_prefix),
    )
    LOGGER.info("Running analytics workflow")
    outputs = run_analytics(config)
    summary = {name: len(df) for name, df in outputs.items()}
    LOGGER.info("Analytics complete", extra=summary)
    return {"status": "ok", "tables": summary}


if __name__ == "__main__":
    # This block is for local testing only
    os.environ["DATA_BUCKET"] = "rearc-quest-testing-bucket"
    os.environ["BLS_PREFIX"] = "bls_data/"
    os.environ["POPULATION_TABLE_PREFIX"] = "population_data/tables/"
    print(handler(None, None))


__all__ = ["handler"]
