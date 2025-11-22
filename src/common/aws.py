"""Lightweight AWS helpers for S3 access and manifest tracking."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Iterable, Protocol

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
    uploaded: list[str]
    deleted: list[str]


class SupportsRead(Protocol):
    def get_object(self, bucket: str, key: str) -> bytes: ...

    def put_object(self, bucket: str, key: str, body: bytes, content_type: str | None = None) -> None: ...

    def list_objects(self, bucket: str, prefix: str) -> list[str]: ...


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


def ensure_bucket_prefix(bucket: str, prefix: str) -> None:
    """Placeholder to ensure S3 location is reachable.

    In production, use boto3 to validate/create prefixes or buckets.
    """


def sync_s3_objects(destination: S3Location, object_keys: Iterable[str], session: SupportsRead) -> S3SyncResult:
    """Sync a collection of object keys to the destination.

    The implementation here is a stub that reports planned uploads/deletions
    but does not perform them. Replace with boto3-driven logic when wiring to
    AWS.
    """

    uploads = [key for key in object_keys]
    deletes: list[str] = []
    return S3SyncResult(uploaded=uploads, deleted=deletes)


def put_json_object(destination: S3Location, key: str, content: dict, client: InMemoryS3Client | None = None) -> None:
    """Serialize JSON and store to S3."""

    import json

    body = json.dumps(content, indent=2).encode()
    client = client or InMemoryS3Client()
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
    client = client or InMemoryS3Client()
    client.put_object(destination.bucket, f"{destination.prefix}{key}", buffer.getvalue(), content_type=content_type)


def read_tabular_object(location: S3Location, key: str, format: str, client: InMemoryS3Client | None = None) -> pd.DataFrame:
    """Read a tabular object from S3 into a DataFrame."""

    client = client or InMemoryS3Client()
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
    "SupportsRead",
]
