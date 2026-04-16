"""S3 connection manager for s3mio."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

import boto3

if TYPE_CHECKING:
    from .bucket import Bucket

logger = logging.getLogger("s3mio")

# Suffix used by S3 Express One Zone bucket names (e.g. "my-bucket--use1-az4--x-s3")
_EXPRESS_SUFFIX = "--x-s3"


class S3:
    """Central connection manager for s3mio.

    Holds the boto3 session and resource, and acts as a factory for Bucket
    instances. A single S3 instance should be shared across your application
    to reuse the underlying HTTP connection pool.

    Args:
        region_name:           AWS region (defaults to AWS_DEFAULT_REGION env var).
        aws_access_key_id:     Optional explicit credentials.
        aws_secret_access_key: Optional explicit credentials.
        aws_session_token:     Optional session token for temporary credentials.
        endpoint_url:          Custom endpoint (useful for LocalStack / MinIO).
        max_retries:           Maximum retry attempts on transient S3 errors
                               (ThrottlingException, ServiceUnavailable, etc.).
                               Default: 3. Set to 0 to disable retry.
                               **Note:** ``upload()``, ``download()``, and
                               ``copy()`` use the boto3 TransferManager, which
                               manages its own part-level retry via botocore.
                               These operations are not affected by this setting.
        retry_delay:           Base sleep time in seconds before the first retry.
                               Each subsequent retry doubles this value plus
                               random jitter. Default: 0.1. Same TransferManager
                               caveat as ``max_retries`` applies.
        max_retry_delay:       Upper bound on the sleep between retries in seconds
                               (default: 30). Prevents unbounded waits when
                               ``max_retries`` is set to a high value.
        **session_kwargs:      Any extra keyword arguments forwarded to boto3.Session.

    Example::

        s3 = S3(region_name="us-east-1")
        bucket = s3.bucket("my-bucket")

        # Custom retry settings
        s3 = S3(region_name="us-east-1", max_retries=5, retry_delay=0.2)
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 0.1,
        max_retry_delay: float = 30.0,
        **session_kwargs: Any,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._session = boto3.Session(
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            **session_kwargs,
        )
        self._resource = self._session.resource("s3", endpoint_url=endpoint_url)
        self._client = self._session.client("s3", endpoint_url=endpoint_url)
        logger.debug("S3 client initialized (region=%s)", region_name)

    @property
    def resource(self) -> Any:
        """boto3 S3 resource (service resource)."""
        return self._resource

    @property
    def client(self) -> Any:
        """boto3 S3 low-level client."""
        return self._client

    @property
    def max_retries(self) -> int:
        """Maximum number of retry attempts on transient S3 errors."""
        return self._max_retries

    @property
    def retry_delay(self) -> float:
        """Base delay in seconds between retries (doubles on each attempt)."""
        return self._retry_delay

    @property
    def max_retry_delay(self) -> float:
        """Maximum sleep cap in seconds between retries."""
        return self._max_retry_delay

    def bucket(self, name: str) -> "Bucket":  # noqa: F821
        """Return a Bucket wrapper for the given bucket name.

        S3 Express One Zone buckets are auto-detected by the ``--x-s3`` suffix
        in their name. No extra configuration is needed.

        Args:
            name: S3 bucket name.

        Returns:
            A :class:`Bucket` instance scoped to that bucket.

        Example::

            bucket = s3.bucket("my-bucket")
            express = s3.bucket("my-bucket--use1-az4--x-s3")  # Express One Zone
        """
        from .bucket import Bucket  # local import to avoid circular dependency

        is_express = name.endswith(_EXPRESS_SUFFIX)
        if is_express:
            logger.debug("Detected S3 Express One Zone bucket: %s", name)
        return Bucket(name=name, s3=self, express=is_express)
