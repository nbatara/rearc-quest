"""Population fetcher from the DataUSA API.

This module is structured for Lambda execution and stores both raw JSON and
normalized tabular outputs to S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable

import pandas as pd

from common.aws import S3Location, put_json_object, put_tabular_object
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
        "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?"
        "cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&"
        "measures=Population"
    )

    def raw_destination(self) -> S3Location:
        return S3Location(bucket=self.bucket, prefix=self.raw_prefix)

    def table_destination(self) -> S3Location:
        return S3Location(bucket=self.bucket, prefix=self.table_prefix)


def normalize_records(records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """Convert API records to a normalized DataFrame."""

    frame = pd.DataFrame(records)
    expected_columns = {"Year", "Nation", "Population"}
    missing = expected_columns - set(frame.columns)
    if missing:
        raise ValueError(f"Missing expected fields: {missing}")
    frame.rename(columns={"Year": "year", "Nation": "nation", "Population": "population"}, inplace=True)
    return frame[["year", "nation", "population"]]


def fetch_and_store(config: DataUSAConfig) -> None:
    """Fetch the population dataset and persist outputs to S3."""

    session = DataUSARequestSession(contact_email=config.contact_email)
    LOGGER.info("Requesting DataUSA population data", extra={"url": config.api_url})
    response_json = session.get_json(config.api_url)
    records = response_json.get("data", [])
    put_json_object(destination=config.raw_destination(), key="population.json", content=response_json)
    table = normalize_records(records)
    put_tabular_object(destination=config.table_destination(), key="population.parquet", frame=table, format="parquet")
    put_tabular_object(destination=config.table_destination(), key="population.csv", frame=table, format="csv")


__all__ = [
    "DataUSAConfig",
    "normalize_records",
    "fetch_and_store",
]
