"""Analytics queries using only pandas for approachability."""

from __future__ import annotations
from typing import Optional
import pandas as pd

from common.aws import S3Location, read_tabular_object, put_tabular_object
from common.logging import get_logger

LOGGER = get_logger(__name__)


class AnalyticsConfig:
    """Configuration for analytics inputs and outputs."""

    def __init__(
        self,
        bls_location: S3Location,
        population_location: S3Location,
        report_location: Optional[S3Location] = None,
    ) -> None:
        self.bls_location = bls_location
        self.population_location = population_location
        self.report_location = report_location


def load_bls_dataset(location: S3Location) -> pd.DataFrame:
    """Load the BLS current data file into a DataFrame."""

    df = read_tabular_object(
        location,
        key="pr.data.0.Current",
        format="csv",
        delimiter="\t",
        header=0,
        names=["series_id", "year", "period", "value", "footnote_codes"],
    )
    # Clean up fields
    df["series_id"] = df["series_id"].str.strip()

    # Ensure year is numeric so it matches population_df['year'] (int)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    # Make value numeric for downstream aggregations
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def load_population_dataset(location: S3Location) -> pd.DataFrame:
    """Load normalized population data."""
    df = read_tabular_object(location, key="population.csv", format="csv")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


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
    ordered = grouped.sort_values(
        ["series_id", "value", "year"], ascending=[True, False, True]
    )
    winners = ordered.groupby("series_id", as_index=False).head(1)
    return winners.reset_index(drop=True)


def series_with_population(
    bls_df: pd.DataFrame, population_df: pd.DataFrame
) -> pd.DataFrame:
    """Join BLS values for PRS30006032 Q01 with population by year."""

    bls_filtered = bls_df[
        (bls_df["series_id"] == "PRS30006032") & (bls_df["period"] == "Q01")
    ]
    merged = bls_filtered.merge(
        population_df, left_on="year", right_on="year", how="left"
    )
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
        LOGGER.info(
            "Computed analytic table", extra={"table": name, "rows": len(frame)}
        )

    if config.report_location:
        for name, frame in outputs.items():
            put_tabular_object(
                destination=config.report_location,
                key=f"{name}.csv",
                frame=frame,
                format="csv",
            )
            LOGGER.info(
                "Wrote analytic table to S3",
                extra={"table": name, "rows": len(frame)},
            )

    return outputs


if __name__ == "__main__":
    from moto import mock_aws
    import boto3
    from common.aws import S3Location
    import pandas as pd
    import io

    # This block is for local testing only
    with mock_aws():
        config = AnalyticsConfig(
            bls_location=S3Location(
                bucket="rearc-quest-testing-bucket", prefix="bls_data/"
            ),
            population_location=S3Location(
                bucket="rearc-quest-testing-bucket", prefix="population_data/tables/"
            ),
        )
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=config.bls_location.bucket)

        # Create dummy files for analytics to run
        bls_key = f"{config.bls_location.prefix}pr.data.0.Current"
        s3.put_object(
            Bucket=config.bls_location.bucket,
            Key=bls_key,
            Body="series_id\tyear\tperiod\tvalue\tfootnote_codes\n".encode("utf-8"),
        )

        pop_key = f"{config.population_location.prefix}population.csv"
        empty_df = pd.DataFrame({"year": [], "nation": [], "population": []})
        s3.put_object(
            Bucket=config.population_location.bucket,
            Key=pop_key,
            Body=empty_df.to_csv(index=False).encode("utf-8"),
        )

        outputs = run_analytics(config)
        for name, df in outputs.items():
            print(f"--- {name} ---")
            print(df)


__all__ = [
    "AnalyticsConfig",
    "run_analytics",
    "load_bls_dataset",
    "load_population_dataset",
    "population_stats",
    "best_year_by_series",
    "series_with_population",
]
