"""Lightweight, easy-to-follow AWS-style helpers."""

from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from typing import Iterable

import boto3
import pandas as pd
from botocore.exceptions import ClientError


@dataclass
class S3Location:
    bucket: str
    prefix: str = ""

    def path(self, key: str) -> str:
        key_path = f"{self.prefix}{key}" if self.prefix else key
        return f"s3://{self.bucket}/{key_path}"


@dataclass
class S3SyncResult:
    """Simple summary of what changed during a sync."""

    uploaded: list[str]
    deleted: list[str]


def _get_client():
    """Return a shared in-memory client so data persists across calls."""
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    return session.client("s3")


def ensure_bucket_prefix(bucket: str, prefix: str) -> None:
    """Placeholder to ensure S3 location is reachable."""
    client = _get_client()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise ValueError(f"Bucket not found: {bucket}") from e
        raise


def sync_s3_objects(
    destination: S3Location, object_keys: Iterable[str]
) -> S3SyncResult:
    """Plan uploads/deletes against the in-memory store.

    *Desired* keys are provided relative to the destination prefix; existing
    keys are discovered from the client. This mirrors how a manifest-based
    sync would behave without pulling in boto3.
    """

    client = _get_client()
    desired = [f"{destination.prefix}{key}" for key in object_keys]
    desired_set = set(desired)

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=destination.bucket, Prefix=destination.prefix)
    existing = set(obj["Key"] for page in pages for obj in page.get("Contents", []))

    uploads = sorted(desired_set - existing)
    deletes = sorted(existing - desired_set)

    # Store placeholder bytes for uploaded objects so analytics can read them
    # later if needed.
    for key in uploads:
        client.put_object(
            Bucket=destination.bucket, Key=key, Body=b"", ContentType="text/plain"
        )
    for key in deletes:
        client.delete_object(Bucket=destination.bucket, Key=key)

    return S3SyncResult(uploaded=uploads, deleted=deletes)


def put_json_object(destination: S3Location, key: str, content: dict) -> None:
    """Serialize JSON and store to S3."""
    body = json.dumps(content, indent=2).encode()
    client = _get_client()
    client.put_object(
        Bucket=destination.bucket,
        Key=f"{destination.prefix}{key}",
        Body=body,
        ContentType="application/json",
    )


def put_text_object(destination: S3Location, key: str, content: str) -> None:
    """Store plain text to S3."""
    body = content.encode()
    client = _get_client()
    client.put_object(
        Bucket=destination.bucket,
        Key=f"{destination.prefix}{key}",
        Body=body,
        ContentType="text/plain",
    )


def put_tabular_object(
    destination: S3Location, key: str, frame: pd.DataFrame, format: str
) -> None:
    """Write pandas DataFrame to S3 in the requested format."""

    if format == "parquet":
        body = frame.to_parquet(index=False)
        content_type = "application/octet-stream"
    elif format == "csv":
        body = frame.to_csv(index=False).encode()
        content_type = "text/csv"
    else:
        raise ValueError(f"Unsupported format: {format}")

    client = _get_client()
    client.put_object(
        Bucket=destination.bucket,
        Key=f"{destination.prefix}{key}",
        Body=body,
        ContentType=content_type,
    )


def read_tabular_object(
    location: S3Location, key: str, format: str, **kwargs
) -> pd.DataFrame:
    """Read a tabular object from S3 into a DataFrame."""

    client = _get_client()
    response = client.get_object(Bucket=location.bucket, Key=f"{location.prefix}{key}")
    body = response["Body"].read()
    buffer = io.BytesIO(body)
    if format == "csv":
        return pd.read_csv(buffer, **kwargs)
    if format == "parquet":
        return pd.read_parquet(buffer)
    raise ValueError(f"Unsupported format: {format}")


__all__ = [
    "S3Location",
    "S3SyncResult",
    "ensure_bucket_prefix",
    "put_json_object",
    "put_tabular_object",
    "put_text_object",
    "read_tabular_object",
    "sync_s3_objects",
]
