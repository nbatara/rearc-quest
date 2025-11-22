"""Analytics queries using only pandas for approachability."""

from __future__ import annotations

import pandas as pd

from common.aws import S3Location, read_tabular_object
from common.logging import get_logger

LOGGER = get_logger(__name__)


class AnalyticsConfig:
    """Configuration for analytics inputs and outputs."""

    def __init__(
        self,
        bls_location: S3Location,
        population_location: S3Location,
    ) -> None:
        self.bls_location = bls_location
        self.population_location = population_location


def load_bls_dataset(location: S3Location) -> pd.DataFrame:
    """Load the BLS current data file into a DataFrame."""

    return read_tabular_object(location, key="pr.data.0.Current", format="csv")


def load_population_dataset(location: S3Location) -> pd.DataFrame:
    """Load normalized population data."""

    return read_tabular_object(location, key="population.parquet", format="parquet")


def population_stats(population_df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean and standard deviation of annual US population for 2013-2018."""

    filtered = population_df[population_df["year"].between(2013, 2018)]
    stats = filtered["population"].agg(["mean", "std"]).to_frame().T
    stats.insert(0, "year_range", "2013-2018")
    return stats


def best_year_by_series(bls_df: pd.DataFrame) -> pd.DataFrame:
    """For each series_id, find the year with the max summed quarterly value."""

    quarterly = bls_df[bls_df["period"].str.startswith("Q")].copy()
    quarterly["value"] = pd.to_numeric(quarterly["value"], errors="coerce")
    grouped = quarterly.groupby(["series_id", "year"], as_index=False)["value"].sum()

    # Within each series, pick the row with the highest yearly total.
    ordered = grouped.sort_values(["series_id", "value", "year"], ascending=[True, False, True])
    winners = ordered.groupby("series_id", as_index=False).head(1)
    return winners.reset_index(drop=True)


def series_with_population(bls_df: pd.DataFrame, population_df: pd.DataFrame) -> pd.DataFrame:
    """Join BLS values for PRS30006032 Q01 with population by year."""

    bls_filtered = bls_df[(bls_df["series_id"] == "PRS30006032") & (bls_df["period"] == "Q01")]
    merged = bls_filtered.merge(population_df, left_on="year", right_on="year", how="left")
    merged.rename(columns={"population": "Population"}, inplace=True)
    return merged[["series_id", "year", "period", "value", "Population"]]


def run_analytics(config: AnalyticsConfig) -> dict[str, pd.DataFrame]:
    """Load datasets and produce analytics tables."""

    bls_df = load_bls_dataset(config.bls_location)
    population_df = load_population_dataset(config.population_location)
    outputs = {
        "population_stats": population_stats(population_df),
        "best_year": best_year_by_series(bls_df),
        "series_population": series_with_population(bls_df, population_df),
    }
    for name, frame in outputs.items():
        LOGGER.info("Computed analytic table", extra={"table": name, "rows": len(frame)})
    return outputs


__all__ = [
    "AnalyticsConfig",
    "run_analytics",
    "load_bls_dataset",
    "load_population_dataset",
    "population_stats",
    "best_year_by_series",
    "series_with_population",
]
