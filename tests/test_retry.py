"""Tests for Fase 5 — retry and exponential backoff."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from s3mio import S3
from s3mio.bucket import Bucket
from s3mio.exceptions import ObjectNotFoundError, S3OperationError
from s3mio.retry import _RETRYABLE_CODES, call_with_retry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_error(code: str, message: str = "test error") -> ClientError:
    """Build a ClientError for a given error code."""
    return ClientError({"Error": {"Code": code, "Message": message}}, "TestOperation")


# ---------------------------------------------------------------------------
# call_with_retry — unit tests
# ---------------------------------------------------------------------------


class TestCallWithRetry:
    def test_returns_on_first_success(self) -> None:
        """call_with_retry returns immediately when the first call succeeds."""
        func = MagicMock(return_value="ok")
        result = call_with_retry(func, 3, 0.0)
        assert result == "ok"
        func.assert_called_once()

    def test_retries_on_throttling_exception(self) -> None:
        """Retries once on ThrottlingException and succeeds on second attempt."""
        func = MagicMock(side_effect=[_client_error("ThrottlingException"), "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 3, 0.0)
        assert result == "ok"
        assert func.call_count == 2

    def test_retries_on_slow_down(self) -> None:
        """Retries on SlowDown (S3-specific throttling code) and succeeds."""
        func = MagicMock(side_effect=[_client_error("SlowDown"), "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 3, 0.0)
        assert result == "ok"

    def test_retries_on_service_unavailable(self) -> None:
        """Retries on ServiceUnavailable and succeeds."""
        func = MagicMock(side_effect=[_client_error("ServiceUnavailable"), "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 3, 0.0)
        assert result == "ok"

    def test_retries_on_internal_error(self) -> None:
        """Retries on InternalError and succeeds."""
        func = MagicMock(side_effect=[_client_error("InternalError"), "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 3, 0.0)
        assert result == "ok"

    def test_retries_on_request_timeout(self) -> None:
        """Retries on RequestTimeout and succeeds."""
        func = MagicMock(side_effect=[_client_error("RequestTimeout"), "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 3, 0.0)
        assert result == "ok"

    def test_raises_after_max_retries_exhausted(self) -> None:
        """Raises ClientError after all retry attempts are consumed."""
        func = MagicMock(side_effect=_client_error("ThrottlingException"))
        with patch("s3mio.retry.time.sleep"):
            with pytest.raises(ClientError):
                call_with_retry(func, 2, 0.0)
        assert func.call_count == 3  # 1 initial + 2 retries

    def test_does_not_retry_non_retryable_error(self) -> None:
        """Non-retryable error is raised immediately without any retry."""
        func = MagicMock(side_effect=_client_error("NoSuchKey"))
        with patch("s3mio.retry.time.sleep") as mock_sleep:
            with pytest.raises(ClientError):
                call_with_retry(func, 3, 0.0)
        func.assert_called_once()
        mock_sleep.assert_not_called()

    def test_no_retry_when_max_retries_zero(self) -> None:
        """max_retries=0 means the function is called once — no retry on failure."""
        func = MagicMock(side_effect=_client_error("ThrottlingException"))
        with patch("s3mio.retry.time.sleep") as mock_sleep:
            with pytest.raises(ClientError):
                call_with_retry(func, 0, 0.0)
        func.assert_called_once()
        mock_sleep.assert_not_called()

    def test_forwards_positional_args(self) -> None:
        """Positional arguments are forwarded to the wrapped function."""
        func = MagicMock(return_value="result")
        call_with_retry(func, 1, 0.0, "a", "b")
        func.assert_called_once_with("a", "b")

    def test_forwards_keyword_args(self) -> None:
        """Keyword arguments are forwarded to the wrapped function."""
        func = MagicMock(return_value="result")
        call_with_retry(func, 1, 0.0, x=1, y=2)
        func.assert_called_once_with(x=1, y=2)

    def test_sleeps_between_retries(self) -> None:
        """time.sleep is called once per retry (not on the final failing call)."""
        func = MagicMock(side_effect=[_client_error("SlowDown"), _client_error("SlowDown"), "ok"])
        with patch("s3mio.retry.time.sleep") as mock_sleep:
            with patch("s3mio.retry.random.uniform", return_value=0.0):
                call_with_retry(func, 3, 0.1)
        assert mock_sleep.call_count == 2

    def test_sleep_duration_increases_exponentially(self) -> None:
        """Sleep durations follow exponential backoff (base * 2^attempt)."""
        func = MagicMock(
            side_effect=[
                _client_error("ThrottlingException"),
                _client_error("ThrottlingException"),
                "ok",
            ]
        )
        sleep_calls: list[float] = []
        with patch("s3mio.retry.random.uniform", return_value=0.0):
            with patch("s3mio.retry.time.sleep", side_effect=lambda d: sleep_calls.append(d)):
                call_with_retry(func, 3, 0.1)

        assert len(sleep_calls) == 2
        # attempt 0 → 0.1 * 2^0 = 0.1, attempt 1 → 0.1 * 2^1 = 0.2
        assert sleep_calls[0] == pytest.approx(0.1)
        assert sleep_calls[1] == pytest.approx(0.2)

    def test_all_retryable_codes_trigger_retry(self) -> None:
        """Every code in _RETRYABLE_CODES triggers at least one retry."""
        for code in _RETRYABLE_CODES:
            func = MagicMock(side_effect=[_client_error(code), "ok"])
            with patch("s3mio.retry.time.sleep"):
                result = call_with_retry(func, 1, 0.0)
            assert result == "ok", f"Code {code!r} should be retryable"

    def test_retries_on_endpoint_connection_error(self) -> None:
        """EndpointConnectionError (network failure) is retried like a throttle."""
        exc = EndpointConnectionError(endpoint_url="https://s3.amazonaws.com")
        func = MagicMock(side_effect=[exc, "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 1, 0.0)
        assert result == "ok"
        assert func.call_count == 2

    def test_retries_on_connect_timeout(self) -> None:
        """ConnectTimeoutError (connection timed out) is retried."""
        exc = ConnectTimeoutError(endpoint_url="https://s3.amazonaws.com")
        func = MagicMock(side_effect=[exc, "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 1, 0.0)
        assert result == "ok"

    def test_retries_on_read_timeout(self) -> None:
        """ReadTimeoutError (read timed out mid-response) is retried."""
        exc = ReadTimeoutError(endpoint_url="https://s3.amazonaws.com")
        func = MagicMock(side_effect=[exc, "ok"])
        with patch("s3mio.retry.time.sleep"):
            result = call_with_retry(func, 1, 0.0)
        assert result == "ok"

    def test_network_error_raises_after_max_retries(self) -> None:
        """Network errors are propagated once all retry attempts are exhausted."""
        exc = EndpointConnectionError(endpoint_url="https://s3.amazonaws.com")
        func = MagicMock(side_effect=exc)
        with patch("s3mio.retry.time.sleep"):
            with pytest.raises(EndpointConnectionError):
                call_with_retry(func, 2, 0.0)
        assert func.call_count == 3  # 1 initial + 2 retries


# ---------------------------------------------------------------------------
# S3 retry configuration
# ---------------------------------------------------------------------------


class TestS3RetryConfig:
    def test_default_max_retries(self, s3: S3) -> None:
        """S3 default max_retries is 3."""
        assert s3.max_retries == 3

    def test_default_retry_delay(self, s3: S3) -> None:
        """S3 default retry_delay is 0.1 seconds."""
        assert s3.retry_delay == 0.1

    def test_custom_max_retries(self) -> None:
        """S3 accepts a custom max_retries value."""
        s3 = S3(max_retries=5)
        assert s3.max_retries == 5

    def test_custom_retry_delay(self) -> None:
        """S3 accepts a custom retry_delay value."""
        s3 = S3(retry_delay=0.5)
        assert s3.retry_delay == 0.5

    def test_zero_max_retries_disables_retry(self) -> None:
        """max_retries=0 is valid and means no retry."""
        s3 = S3(max_retries=0)
        assert s3.max_retries == 0

    def test_bucket_inherits_max_retries(self, bucket: Bucket) -> None:
        """Bucket._max_retries is inherited from the parent S3 instance."""
        assert bucket._max_retries == 3

    def test_bucket_inherits_retry_delay(self, bucket: Bucket) -> None:
        """Bucket._retry_delay is inherited from the parent S3 instance."""
        assert bucket._retry_delay == 0.1

    def test_bucket_picks_up_custom_s3_retries(self) -> None:
        """Bucket created from a custom S3 inherits its retry settings."""
        import boto3
        from moto import mock_aws

        @mock_aws
        def _run() -> None:
            s3_custom = S3(max_retries=7, retry_delay=0.5)
            s3_custom._session = boto3.Session(region_name="us-east-1")
            s3_custom._resource = s3_custom._session.resource("s3")
            s3_custom._client = s3_custom._session.client("s3")
            b = s3_custom.bucket("any-name")
            assert b._max_retries == 7
            assert b._retry_delay == 0.5

        _run()


# ---------------------------------------------------------------------------
# Bucket integration — verify retry happens on actual operations
# ---------------------------------------------------------------------------


class TestBucketRetryIntegration:
    def test_put_retries_on_throttle_and_succeeds(self, bucket: Bucket) -> None:
        """bucket.put() retries on ThrottlingException and succeeds."""
        call_count = [0]
        original = bucket._boto_bucket.put_object

        def flaky_put(**kwargs):
            """Fail once with ThrottlingException then succeed."""
            call_count[0] += 1
            if call_count[0] == 1:
                raise _client_error("ThrottlingException")
            return original(**kwargs)

        with patch.object(bucket._boto_bucket, "put_object", side_effect=flaky_put):
            with patch("s3mio.retry.time.sleep"):
                bucket.put("file.txt", "hello")

        assert call_count[0] == 2

    def test_put_raises_s3_operation_error_after_exhaustion(self, bucket: Bucket) -> None:
        """bucket.put() raises S3OperationError when all retries fail."""
        bucket._max_retries = 2

        def always_throttle(**kwargs):
            """Always raise ThrottlingException."""
            raise _client_error("ThrottlingException")

        with patch.object(bucket._boto_bucket, "put_object", side_effect=always_throttle):
            with patch("s3mio.retry.time.sleep"):
                with pytest.raises(S3OperationError):
                    bucket.put("file.txt", "data")

    def test_head_retries_on_throttle_and_succeeds(self, bucket: Bucket) -> None:
        """bucket.head() retries on ThrottlingException and returns ObjectInfo."""
        bucket.put("file.txt", "hello")
        call_count = [0]
        original = bucket._s3.client.head_object

        def flaky_head(**kwargs):
            """Fail once with ThrottlingException then succeed."""
            call_count[0] += 1
            if call_count[0] == 1:
                raise _client_error("ThrottlingException")
            return original(**kwargs)

        with patch.object(bucket._s3.client, "head_object", side_effect=flaky_head):
            with patch("s3mio.retry.time.sleep"):
                info = bucket.head("file.txt")

        assert info.key == "file.txt"
        assert call_count[0] == 2

    def test_not_found_not_retried(self, bucket: Bucket) -> None:
        """ObjectNotFoundError is raised without retry (404 is not retryable)."""
        with patch("s3mio.retry.time.sleep") as mock_sleep:
            with pytest.raises(ObjectNotFoundError):
                bucket.get_bytes("nonexistent.txt")
        mock_sleep.assert_not_called()

    def test_delete_many_retries_on_throttle(self, bucket: Bucket) -> None:
        """bucket.delete_many() retries on ServiceUnavailable and succeeds."""
        bucket.put("a.txt", "a")
        call_count = [0]
        original = bucket._s3.client.delete_objects

        def flaky_delete(**kwargs):
            """Fail once with ServiceUnavailable then succeed."""
            call_count[0] += 1
            if call_count[0] == 1:
                raise _client_error("ServiceUnavailable")
            return original(**kwargs)

        with patch.object(bucket._s3.client, "delete_objects", side_effect=flaky_delete):
            with patch("s3mio.retry.time.sleep"):
                result = bucket.delete_many(["a.txt"])

        assert len(result) == 1
        assert call_count[0] == 2

    def test_copy_preserves_content(self, bucket: Bucket) -> None:
        """bucket.copy() correctly copies object content via TransferManager."""
        bucket.put("src.txt", "content")
        bucket.copy("src.txt", "dst.txt")
        assert bucket.exists("dst.txt")
        assert bucket.get_text("dst.txt") == "content"
