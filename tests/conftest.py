"""pytest fixtures for s3mio tests."""

from typing import Any

import boto3
import pytest
from moto import mock_aws

from s3mio import S3

TEST_BUCKET = "test-bucket"
TEST_REGION = "us-east-1"


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set fake AWS credentials so moto never hits real AWS."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_REGION)


def _create_bucket(client: Any, bucket: str, region: str) -> None:
    """Create a bucket respecting the us-east-1 quirk.

    S3 does not accept LocationConstraint for us-east-1 (the default region).
    """
    if region == "us-east-1":
        client.create_bucket(Bucket=bucket)
    else:
        client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


@pytest.fixture
def s3_client(aws_credentials: None):  # type: ignore[no-untyped-def]
    """Mocked S3 client with a pre-created test bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        _create_bucket(client, TEST_BUCKET, TEST_REGION)
        yield client


@pytest.fixture
def s3(aws_credentials: None):  # type: ignore[no-untyped-def]
    """Mocked S3 instance (s3mio) with a pre-created test bucket."""
    with mock_aws():
        raw = boto3.client("s3", region_name=TEST_REGION)
        _create_bucket(raw, TEST_BUCKET, TEST_REGION)
        yield S3(region_name=TEST_REGION)


@pytest.fixture
def bucket(s3: S3):  # type: ignore[no-untyped-def]
    """A Bucket instance pointing at the mocked test bucket."""
    return s3.bucket(TEST_BUCKET)
