"""Lightweight, easy-to-follow AWS-style helpers.

These utilities intentionally avoid boto3 to keep the demo self contained.
An in-memory client mimics S3 so the ingest and analytics steps can be
walked through locally without extra services.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Iterable

import pandas as pd


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


class InMemoryS3Client:
    """Minimal in-memory client for local testing."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    def put_object(self, bucket: str, key: str, body: bytes, content_type: str | None = None) -> None:
        self.objects[(bucket, key)] = body

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        return [key for b, key in self.objects if b == bucket and key.startswith(prefix)]

    def delete_object(self, bucket: str, key: str) -> None:
        self.objects.pop((bucket, key), None)


DEFAULT_CLIENT = InMemoryS3Client()


def _get_client(client: InMemoryS3Client | None = None) -> InMemoryS3Client:
    """Return a shared in-memory client so data persists across calls."""

    return client or DEFAULT_CLIENT


def ensure_bucket_prefix(bucket: str, prefix: str) -> None:
    """Placeholder to ensure S3 location is reachable."""


def sync_s3_objects(
    destination: S3Location, object_keys: Iterable[str], client: InMemoryS3Client | None = None
) -> S3SyncResult:
    """Plan uploads/deletes against the in-memory store.

    *Desired* keys are provided relative to the destination prefix; existing
    keys are discovered from the client. This mirrors how a manifest-based
    sync would behave without pulling in boto3.
    """

    client = _get_client(client)
    desired = [f"{destination.prefix}{key}" for key in object_keys]
    desired_set = set(desired)
    existing = set(client.list_objects(destination.bucket, destination.prefix))

    uploads = sorted(desired_set - existing)
    deletes = sorted(existing - desired_set)

    # Store placeholder bytes for uploaded objects so analytics can read them
    # later if needed.
    for key in uploads:
        client.put_object(destination.bucket, key, b"", content_type="text/plain")
    for key in deletes:
        client.delete_object(destination.bucket, key)

    return S3SyncResult(uploaded=uploads, deleted=deletes)


def put_json_object(destination: S3Location, key: str, content: dict, client: InMemoryS3Client | None = None) -> None:
    """Serialize JSON and store to S3."""

    import json

    body = json.dumps(content, indent=2).encode()
    client = _get_client(client)
    client.put_object(destination.bucket, f"{destination.prefix}{key}", body, content_type="application/json")


def put_tabular_object(
    destination: S3Location, key: str, frame: pd.DataFrame, format: str, client: InMemoryS3Client | None = None
) -> None:
    """Write pandas DataFrame to S3 in the requested format."""

    buffer = io.BytesIO()
    if format == "parquet":
        frame.to_parquet(buffer, index=False)
        content_type = "application/octet-stream"
    elif format == "csv":
        buffer.write(frame.to_csv(index=False).encode())
        content_type = "text/csv"
    else:
        raise ValueError(f"Unsupported format: {format}")
    client = _get_client(client)
    client.put_object(destination.bucket, f"{destination.prefix}{key}", buffer.getvalue(), content_type=content_type)


def read_tabular_object(location: S3Location, key: str, format: str, client: InMemoryS3Client | None = None) -> pd.DataFrame:
    """Read a tabular object from S3 into a DataFrame."""

    client = _get_client(client)
    body = client.get_object(location.bucket, f"{location.prefix}{key}")
    buffer = io.BytesIO(body)
    if format == "csv":
        return pd.read_csv(buffer)
    if format == "parquet":
        return pd.read_parquet(buffer)
    raise ValueError(f"Unsupported format: {format}")


__all__ = [
    "S3Location",
    "S3SyncResult",
    "ensure_bucket_prefix",
    "sync_s3_objects",
    "put_json_object",
    "put_tabular_object",
    "read_tabular_object",
    "InMemoryS3Client",
]
