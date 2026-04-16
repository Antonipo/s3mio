"""s3mio exception hierarchy.

All exceptions raised by s3mio are subclasses of S3Error, so callers
can catch at any granularity they need::

    from s3mio import S3Error, ObjectNotFoundError

    try:
        obj = bucket.get_json("missing.json")
    except ObjectNotFoundError:
        # handle 404
    except S3Error:
        # handle any other s3mio error
"""


class S3Error(Exception):
    """Base class for all s3mio errors."""


class ObjectNotFoundError(S3Error):
    """Raised when the requested object key does not exist in the bucket."""

    def __init__(self, key: str, bucket: str) -> None:
        self.key = key
        self.bucket = bucket
        super().__init__(f"Object not found: s3://{bucket}/{key}")


class BucketNotFoundError(S3Error):
    """Raised when the target bucket does not exist or is not accessible."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        super().__init__(f"Bucket not found: {bucket}")


class AccessDeniedError(S3Error):
    """Raised when the caller lacks permission for the requested operation."""

    def __init__(self, key: str, bucket: str, operation: str = "access") -> None:
        self.key = key
        self.bucket = bucket
        self.operation = operation
        super().__init__(f"Access denied: cannot {operation} s3://{bucket}/{key}")


class ValidationError(S3Error):
    """Raised when s3mio detects invalid input before calling AWS."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class S3OperationError(S3Error):
    """Raised for unhandled boto3 / AWS errors not covered by specific subclasses."""

    def __init__(self, message: str, error_code: str = "") -> None:
        self.error_code = error_code
        super().__init__(message)
