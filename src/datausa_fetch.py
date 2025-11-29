"""Population fetcher from the DataUSA API.

This module is structured for Lambda execution and stores both raw JSON and
normalized tabular outputs to S3.
"""

from __future__ import annotations
import io
from dataclasses import dataclass
from typing import Any, Dict, Iterable

import pandas as pd

from common.aws import S3Location, put_text_object, put_tabular_object
from common.http import DataUSARequestSession
from common.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class DataUSAConfig:
    """Configuration for the DataUSA population fetch."""

    bucket: str
    raw_prefix: str
    table_prefix: str
    contact_email: str
    api_url: str = (
        "https://honolulu-api.datausa.io/tesseract/data.csv?"
        "cube=acs_yg_total_population_1&drilldowns=Year,Nation&measures=Population"
    )

    def raw_destination(self) -> S3Location:
        return S3Location(bucket=self.bucket, prefix=self.raw_prefix)

    def table_destination(self) -> S3Location:
        return S3Location(bucket=self.bucket, prefix=self.table_prefix)


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Convert API records to a normalized DataFrame."""
    frame.rename(
        columns={"Year": "year", "Nation": "nation", "Population": "population"},
        inplace=True,
    )
    return frame[["year", "nation", "population"]]


def fetch_and_store(config: DataUSAConfig) -> None:
    """Fetch the population dataset and persist outputs to S3."""

    session = DataUSARequestSession(contact_email=config.contact_email)
    LOGGER.info("Requesting DataUSA population data", extra={"url": config.api_url})
    response_text = session.get_text(config.api_url)
    put_text_object(
        destination=config.raw_destination(),
        key="population.csv",
        content=response_text,
    )

    table = pd.read_csv(io.StringIO(response_text))
    table = normalize_frame(table)

    put_tabular_object(
        destination=config.table_destination(),
        key="population.csv",
        frame=table,
        format="csv",
    )


__all__ = [
    "DataUSAConfig",
    "fetch_and_store",
    "normalize_frame",
]

if __name__ == "__main__":
    from moto import mock_aws
    import boto3

    # This block is for local testing only
    with mock_aws():
        config = DataUSAConfig(
            bucket="rearc-quest-testing-bucket",
            raw_prefix="population_data/raw/",
            table_prefix="population_data/tables/",
            contact_email="test@test.com",
        )
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=config.bucket)

        fetch_and_store(config)
        print("Fetch and store complete.")
