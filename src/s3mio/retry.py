"""Retry logic with exponential backoff and jitter for s3mio."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable

from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    IncompleteReadError,
    ReadTimeoutError,
)

logger = logging.getLogger("s3mio")

# Error codes that are transient and safe to retry
_RETRYABLE_CODES: frozenset[str] = frozenset(
    {
        "ThrottlingException",
        "RequestThrottled",
        "Throttling",
        "RequestTimeout",
        "ServiceUnavailable",
        "InternalError",
        "InternalFailure",
        "SlowDown",  # S3-specific throttling code returned as HTTP 503
        "503",
    }
)

# BotoCoreError subclasses representing transient network failures (not ClientErrors)
_RETRYABLE_NETWORK_ERRORS = (
    EndpointConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
    IncompleteReadError,
)


def call_with_retry(
    func: Callable[..., Any],
    max_retries: int,
    base_delay: float,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Call *func* with exponential-backoff retry on transient S3 errors.

    On each failure the delay is ``base_delay * 2 ** attempt`` plus a random
    jitter in ``[0, base_delay)`` to spread concurrent retries apart.

    Args:
        func:        The callable to invoke.
        max_retries: Maximum number of retry attempts after the first call
                     (0 = no retry — fail immediately on any error).
        base_delay:  Base sleep duration in seconds before the first retry.
        *args:       Positional arguments forwarded to *func*.
        **kwargs:    Keyword arguments forwarded to *func*.

    Returns:
        The return value of *func* on success.

    Raises:
        ClientError: Propagated unchanged when the error code is not in
                     :data:`_RETRYABLE_CODES`, or when all *max_retries*
                     attempts are exhausted.
        BotoCoreError: Network-level errors (EndpointConnectionError,
                       ConnectTimeoutError, ReadTimeoutError,
                       IncompleteReadError) are retried like throttle errors.
                       Propagated when all *max_retries* attempts are exhausted.

    Example::

        from s3mio.retry import call_with_retry

        result = call_with_retry(
            client.put_object,
            max_retries=3,
            base_delay=0.1,
            Bucket="my-bucket",
            Key="file.txt",
            Body=b"hello",
        )
    """
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in _RETRYABLE_CODES or attempt >= max_retries:
                raise
            delay = base_delay * (2**attempt) + random.uniform(0, base_delay)
            logger.warning(
                "Transient S3 error [%s], retry %d/%d in %.2fs",
                code,
                attempt + 1,
                max_retries,
                delay,
            )
            time.sleep(delay)
        except _RETRYABLE_NETWORK_ERRORS as exc:
            if attempt >= max_retries:
                raise
            delay = base_delay * (2**attempt) + random.uniform(0, base_delay)
            logger.warning(
                "Network error (%s), retry %d/%d in %.2fs",
                type(exc).__name__,
                attempt + 1,
                max_retries,
                delay,
            )
            time.sleep(delay)
    return None  # unreachable — satisfies mypy
