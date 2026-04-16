"""s3mio — Pythonic S3 wrapper for AWS.

A lightweight library that wraps boto3 S3 with a clean, typed API
for common object storage operations.

Quick start::

    from s3mio import S3

    s3 = S3(region_name="us-east-1")
    bucket = s3.bucket("my-bucket")

    bucket.put("path/to/config.json", {"env": "prod"})
    cfg = bucket.get_json("path/to/config.json")

    # folder-style navigation
    folder = bucket / "dev" / "proyecto1"
    folder.put("demo.txt", "hello world")

Exports:
    S3:                  Central connection manager.
    Bucket:              Bucket operations wrapper.
    Prefix:              Scoped prefix / virtual folder.
    ObjectInfo:          Metadata returned by list() and head().
    S3Error:             Base exception class.
    ObjectNotFoundError: Object key does not exist (404).
    BucketNotFoundError: Bucket does not exist.
    AccessDeniedError:   Caller lacks permission (403).
    ValidationError:     Bad input detected before calling AWS.
    S3OperationError:    Generic unhandled AWS error.
"""

__version__ = "0.1.0"

from ._types import ObjectInfo
from .bucket import Bucket, Prefix
from .client import S3
from .exceptions import (
    AccessDeniedError,
    BucketNotFoundError,
    ObjectNotFoundError,
    S3Error,
    S3OperationError,
    ValidationError,
)

__all__ = [
    "__version__",
    "S3",
    "Bucket",
    "Prefix",
    "ObjectInfo",
    "S3Error",
    "ObjectNotFoundError",
    "BucketNotFoundError",
    "AccessDeniedError",
    "ValidationError",
    "S3OperationError",
]
