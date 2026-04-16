"""Bucket and Prefix classes for s3mio."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Generator, Iterator, List, Optional, cast

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from ._types import ObjectInfo
from .exceptions import (
    AccessDeniedError,
    BucketNotFoundError,
    ObjectNotFoundError,
    S3Error,
    S3OperationError,
    ValidationError,
)
from .retry import call_with_retry

if TYPE_CHECKING:
    from .client import S3

logger = logging.getLogger("s3mio")

# MIME types used in put() smart detection
_CONTENT_TYPE_JSON = "application/json"
_CONTENT_TYPE_TEXT = "text/plain; charset=utf-8"
_CONTENT_TYPE_BINARY = "application/octet-stream"

# Multipart upload threshold — files larger than this use multipart automatically
_MULTIPART_THRESHOLD = 8 * 1024 * 1024  # 8 MB

# Default chunk size for stream()
_DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

# Maximum keys per S3 DeleteObjects call (AWS hard limit)
_BULK_DELETE_LIMIT = 1000


def _map_client_error(
    error: ClientError,
    key: str,
    bucket: str,
    operation: str = "access",
) -> S3Error:
    """Map a boto3 ClientError to the appropriate s3mio exception."""
    code = error.response["Error"]["Code"]
    if code in ("NoSuchKey", "404"):
        return ObjectNotFoundError(key=key, bucket=bucket)
    if code == "NoSuchBucket":
        return BucketNotFoundError(bucket=bucket)
    if code in ("AccessDenied", "403"):
        return AccessDeniedError(key=key, bucket=bucket, operation=operation)
    return S3OperationError(
        f"S3 error [{code}] on {operation} s3://{bucket}/{key}: "
        f"{error.response['Error'].get('Message', '')}",
        error_code=code,
    )


class Bucket:
    """Wrapper around a single S3 bucket.

    Obtain instances via :meth:`S3.bucket` rather than instantiating directly::

        s3 = S3(region_name="us-east-1")
        bucket = s3.bucket("my-bucket")

    Folder-like navigation uses the ``/`` operator (pathlib-style)::

        folder = bucket / "logs" / "2025"
        folder.put("app.log", log_text)
        for obj in folder:
            print(obj.key)
    """

    def __init__(self, name: str, s3: "S3", express: bool = False) -> None:
        self._name = name
        self._s3 = s3
        self._express = express
        self._boto_bucket = s3.resource.Bucket(name)
        self._max_retries: int = s3.max_retries
        self._retry_delay: float = s3.retry_delay

    def _call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute *func* with exponential-backoff retry on transient S3 errors.

        Delegates to :func:`~s3mio.retry.call_with_retry` using this bucket's
        retry configuration (``_max_retries`` and ``_retry_delay``).

        Args:
            func:     The boto3 callable to invoke.
            *args:    Positional arguments forwarded to *func*.
            **kwargs: Keyword arguments forwarded to *func*.

        Returns:
            The return value of *func*.
        """
        return call_with_retry(func, self._max_retries, self._retry_delay, *args, **kwargs)

    @property
    def name(self) -> str:
        """Bucket name."""
        return self._name

    @property
    def express(self) -> bool:
        """True if this is an S3 Express One Zone bucket."""
        return self._express

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def put(
        self,
        key: str,
        data: Any,
        content_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Put an object into the bucket.

        Type detection (when *content_type* is not provided):

        - ``dict`` / ``list`` → JSON-serialised, ``application/json``
        - ``str``             → UTF-8 encoded, ``text/plain; charset=utf-8``
        - ``bytes``           → raw, ``application/octet-stream``

        Args:
            key:          S3 key for the object.
            data:         Object body (dict, list, str, or bytes).
            content_type: Override the inferred Content-Type.
            metadata:     User-defined metadata (max 2 KB, string values).
            tags:         Object tags as key-value pairs.

        Raises:
            ValidationError:     If *key* is empty or *data* type is unsupported.
            BucketNotFoundError: If the bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:PutObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            bucket.put("config.json", {"env": "prod"})
            bucket.put("banner.png", image_bytes, content_type="image/png")
            bucket.put("note.txt", "hello", tags={"owner": "antonio"})
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        body, detected_ct = _serialize(data)
        ct = content_type or detected_ct

        kwargs: dict[str, Any] = {
            "Key": key,
            "Body": body,
            "ContentType": ct,
        }
        if metadata:
            kwargs["Metadata"] = metadata
        if tags:
            kwargs["Tagging"] = _encode_tags(tags)

        try:
            self._call(self._boto_bucket.put_object, **kwargs)
            logger.debug("put s3://%s/%s (%s)", self._name, key, ct)
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "put") from exc

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_json(self, key: str) -> Any:
        """Download an object and deserialise it as JSON.

        Args:
            key: S3 key of the object.

        Returns:
            The parsed JSON value (dict, list, str, int, etc.).

        Raises:
            ObjectNotFoundError: If the key does not exist.
            S3OperationError:    For any other AWS error.
        """
        body = self.get_bytes(key)
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise S3OperationError(
                f"Object s3://{self._name}/{key} is not valid JSON."
            ) from exc

    def get_text(self, key: str, encoding: str = "utf-8") -> str:
        """Download an object and decode it as text.

        Args:
            key:      S3 key of the object.
            encoding: Text encoding (default: utf-8).

        Returns:
            The decoded string.

        Raises:
            ObjectNotFoundError: If the key does not exist.
            S3OperationError:    For any other AWS error.
        """
        return self.get_bytes(key).decode(encoding)

    def get_bytes(self, key: str) -> bytes:
        """Download an object and return its raw bytes.

        Args:
            key: S3 key of the object.

        Returns:
            Raw object body as bytes.

        Raises:
            ObjectNotFoundError: If the key does not exist.
            S3OperationError:    For any other AWS error.
        """
        if not key:
            raise ValidationError("Object key must not be empty.")
        try:
            response = self._call(self._boto_bucket.Object(key).get)
            body: bytes = response["Body"].read()
            logger.debug("get s3://%s/%s (%d bytes)", self._name, key, len(body))
            return body
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "get") from exc

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, key: str) -> None:
        """Delete a single object.

        Args:
            key: S3 key to delete.

        Raises:
            ValidationError:     If *key* is empty.
            BucketNotFoundError: If the bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:DeleteObject`` permission.
            S3OperationError:    For any other AWS error.
        """
        if not key:
            raise ValidationError("Object key must not be empty.")
        try:
            self._call(self._boto_bucket.Object(key).delete)
            logger.debug("delete s3://%s/%s", self._name, key)
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "delete") from exc

    # ------------------------------------------------------------------
    # Exists
    # ------------------------------------------------------------------

    def exists(self, key: str) -> bool:
        """Check whether an object exists without downloading its body.

        Args:
            key: S3 key to check.

        Returns:
            True if the object exists, False if it doesn't.

        Raises:
            ValidationError:  If *key* is empty.
            AccessDeniedError: If the caller lacks ``s3:GetObject`` permission.
            S3OperationError: For unexpected AWS errors.
        """
        if not key:
            raise ValidationError("Object key must not be empty.")
        try:
            self._call(self._s3.client.head_object, Bucket=self._name, Key=key)
            return True
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                return False
            raise _map_client_error(exc, key, self._name, "head") from exc

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list(
        self,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
    ) -> list[ObjectInfo]:
        """List objects in the bucket, optionally filtered by prefix.

        Paginates automatically — returns all matching keys up to *max_keys*
        per page (AWS limit), collecting all pages.

        Args:
            prefix:    Key prefix filter (e.g. ``"logs/"``).
            delimiter: Hierarchy delimiter (e.g. ``"/"`` to simulate folders).
            max_keys:  Page size for each ListObjectsV2 call (default 1000).

        Returns:
            List of :class:`ObjectInfo` sorted by key.

        Raises:
            BucketNotFoundError: If the bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:ListBucket`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            objects = bucket.list(prefix="users/")
            for obj in objects:
                print(obj.key, obj.size)
        """
        params: dict[str, Any] = {
            "Bucket": self._name,
            "MaxKeys": max_keys,
        }
        if prefix:
            params["Prefix"] = prefix
        if delimiter:
            params["Delimiter"] = delimiter

        def _fetch_all_pages() -> list[ObjectInfo]:
            """Paginate list_objects_v2 and collect all results."""
            items: list[ObjectInfo] = []
            paginator = self._s3.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(**params):
                for obj in page.get("Contents", []):
                    items.append(
                        ObjectInfo(
                            key=obj["Key"],
                            size=obj["Size"],
                            last_modified=obj["LastModified"],
                            etag=obj["ETag"].strip('"'),
                            storage_class=obj.get("StorageClass", "STANDARD"),
                        )
                    )
            return items

        try:
            return cast(list[ObjectInfo], self._call(_fetch_all_pages))
        except ClientError as exc:
            raise _map_client_error(exc, prefix, self._name, "list") from exc

    # ------------------------------------------------------------------
    # File I/O — upload / download
    # ------------------------------------------------------------------

    def upload(
        self,
        local_path: str | Path,
        key: str,
        on_progress: Optional[Callable[[float], None]] = None,
    ) -> None:
        """Upload a local file to S3.

        Multipart upload is used automatically when the file exceeds 8 MB,
        splitting it into parallel parts for faster transfers. This is
        completely transparent — no extra configuration needed.

        Args:
            local_path:  Path to the local file (str or :class:`pathlib.Path`).
            key:         Destination S3 key.
            on_progress: Optional callback invoked periodically with the
                         current upload percentage (0.0 – 100.0). Example::

                             bucket.upload("video.mp4", "media/video.mp4",
                                           on_progress=lambda p: print(f"{p:.0f}%"))

        Raises:
            ValidationError:     If *key* is empty or the local file does not exist.
            BucketNotFoundError: If the bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:PutObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            # Simple upload
            bucket.upload("data/report.csv", "reports/2025/report.csv")

            # With progress bar
            bucket.upload("backup.tar.gz", "backups/backup.tar.gz",
                          on_progress=lambda pct: print(f"\\r{pct:.1f}%", end=""))
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        local_path = Path(local_path)
        if not local_path.exists():
            raise ValidationError(f"Local file not found: {local_path}")
        if not local_path.is_file():
            raise ValidationError(f"Path is not a file: {local_path}")

        config = TransferConfig(multipart_threshold=_MULTIPART_THRESHOLD)

        callback: Optional[Callable[[int], None]] = None
        if on_progress is not None:
            file_size = local_path.stat().st_size
            transferred: list[int] = [0]

            def _progress_callback(bytes_amount: int) -> None:
                """Inner callback that converts bytes → percentage."""
                transferred[0] += bytes_amount
                pct = (transferred[0] / file_size * 100.0) if file_size > 0 else 100.0
                on_progress(min(pct, 100.0))

            callback = _progress_callback

        try:
            self._call(
                self._s3.client.upload_file,
                str(local_path),
                self._name,
                key,
                Config=config,
                Callback=callback,
            )
            logger.debug("upload %s → s3://%s/%s", local_path, self._name, key)
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "upload") from exc

    def download(
        self,
        key: str,
        local_path: str | Path,
    ) -> None:
        """Download an S3 object to a local file.

        Parent directories of *local_path* are created automatically if they
        do not exist. The file is written atomically via a temporary file so
        a failed download never leaves a partially written file at the target.

        Args:
            key:        S3 key of the object to download.
            local_path: Destination path (str or :class:`pathlib.Path`).

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the key does not exist in S3.
            AccessDeniedError:   If the caller lacks ``s3:GetObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            bucket.download("reports/2025/report.csv", "/tmp/report.csv")
            bucket.download("models/v3.pkl", Path("models") / "v3.pkl")
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            self._call(self._s3.client.download_file, self._name, key, str(local_path))
            logger.debug("download s3://%s/%s → %s", self._name, key, local_path)
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "download") from exc

    # ------------------------------------------------------------------
    # Streaming — open / stream
    # ------------------------------------------------------------------

    @contextmanager
    def open(
        self,
        key: str,
        encoding: str = "utf-8",
    ) -> Generator[Iterator[str], None, None]:
        """Stream an S3 object as a text file, line by line.

        Opens a streaming connection to S3 and yields an iterator over the
        decoded lines. The connection is closed automatically when the
        context manager exits — no data is buffered in memory.

        Trailing newline characters (``\\n``, ``\\r\\n``) are stripped from
        each line, consistent with Python's built-in ``open()`` in text mode.

        Args:
            key:      S3 key of the object.
            encoding: Text encoding used to decode each line (default: utf-8).

        Yields:
            An iterator of decoded text lines (newlines stripped).

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the key does not exist.
            AccessDeniedError:   If the caller lacks ``s3:GetObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            # Stream a large JSONL file without loading it into memory
            with bucket.open("events/2025.jsonl") as lines:
                for line in lines:
                    event = json.loads(line)
                    process(event)

            # Stream a CSV file
            with bucket.open("data/users.csv") as lines:
                header = next(lines)
                for row in lines:
                    print(row)
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        try:
            response = self._call(self._s3.client.get_object, Bucket=self._name, Key=key)
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "open") from exc

        body = response["Body"]
        try:
            yield _iter_lines(body, encoding)
        finally:
            body.close()

    def stream(
        self,
        key: str,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """Stream an S3 object as raw binary chunks.

        Opens a streaming connection and yields fixed-size byte chunks until
        the object is fully consumed. Useful for large binary files (videos,
        archives, model weights) that must not be loaded entirely into memory.

        Args:
            key:        S3 key of the object.
            chunk_size: Number of bytes per chunk (default: 8 MB).

        Yields:
            ``bytes`` chunks of at most *chunk_size* bytes each.
            The last chunk may be smaller if the object size is not a
            multiple of *chunk_size*.

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the key does not exist.
            AccessDeniedError:   If the caller lacks ``s3:GetObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            # Re-upload a large file to another bucket in chunks
            with open("local_copy.bin", "wb") as f:
                for chunk in bucket.stream("backups/large.bin", chunk_size=16 * 1024 * 1024):
                    f.write(chunk)

            # Process a binary stream
            for chunk in bucket.stream("data/embeddings.bin"):
                process_chunk(chunk)
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        try:
            response = self._call(self._s3.client.get_object, Bucket=self._name, Key=key)
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "stream") from exc

        body = response["Body"]
        try:
            while True:
                chunk = body.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            body.close()

    # ------------------------------------------------------------------
    # Batch operations — delete_many / copy / copy_many
    # ------------------------------------------------------------------

    def delete_many(self, keys: List[str]) -> int:
        """Delete multiple objects in a single bulk operation.

        S3 supports up to 1000 keys per ``DeleteObjects`` call. When *keys*
        exceeds that limit, s3mio automatically splits it into chunks and
        issues multiple requests.

        Args:
            keys: List of S3 keys to delete. Empty list is a no-op.

        Returns:
            Total number of objects deleted.

        Raises:
            BucketNotFoundError: If the bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:DeleteObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            deleted = bucket.delete_many(["tmp/a.json", "tmp/b.json", "tmp/c.json"])
            print(f"Removed {deleted} objects")
        """
        if not keys:
            return 0

        deleted_count = 0
        for i in range(0, len(keys), _BULK_DELETE_LIMIT):
            chunk = keys[i : i + _BULK_DELETE_LIMIT]
            payload = {"Objects": [{"Key": k} for k in chunk], "Quiet": True}
            try:
                self._call(self._s3.client.delete_objects, Bucket=self._name, Delete=payload)
                deleted_count += len(chunk)
                logger.debug(
                    "delete_many s3://%s — deleted %d objects (chunk %d-%d)",
                    self._name,
                    len(chunk),
                    i,
                    i + len(chunk),
                )
            except ClientError as exc:
                raise _map_client_error(exc, chunk[0], self._name, "delete_many") from exc

        return deleted_count

    def copy(
        self,
        src_key: str,
        dest_key: str,
        dest_bucket: Optional[str] = None,
    ) -> None:
        """Copy a single object within this bucket or to another bucket.

        The source object is not modified. If *dest_bucket* is omitted, the
        copy stays within the same bucket.

        Args:
            src_key:     S3 key of the source object.
            dest_key:    S3 key for the copy destination.
            dest_bucket: Destination bucket name. Defaults to this bucket.

        Raises:
            ValidationError:     If *src_key* or *dest_key* is empty.
            ObjectNotFoundError: If *src_key* does not exist.
            BucketNotFoundError: If the destination bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:CopyObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            # Copy within the same bucket
            bucket.copy("drafts/report.pdf", "published/report.pdf")

            # Copy to another bucket
            bucket.copy("data/export.csv", "archive/export.csv",
                        dest_bucket="my-archive-bucket")
        """
        if not src_key:
            raise ValidationError("Source key must not be empty.")
        if not dest_key:
            raise ValidationError("Destination key must not be empty.")

        target_bucket = dest_bucket or self._name
        copy_source = {"Bucket": self._name, "Key": src_key}

        try:
            self._call(
                self._s3.client.copy_object,
                CopySource=copy_source,
                Bucket=target_bucket,
                Key=dest_key,
            )
            logger.debug(
                "copy s3://%s/%s → s3://%s/%s",
                self._name,
                src_key,
                target_bucket,
                dest_key,
            )
        except ClientError as exc:
            raise _map_client_error(exc, src_key, self._name, "copy") from exc

    def copy_many(
        self,
        pairs: List[tuple[str, str]],
        dest_bucket: Optional[str] = None,
    ) -> int:
        """Copy multiple objects in sequence.

        Each pair is ``(src_key, dest_key)``. If *dest_bucket* is omitted,
        all copies stay within this bucket.

        S3 has no native batch-copy API, so this method issues one
        ``CopyObject`` call per pair. For very large lists consider running
        this in a background thread.

        Args:
            pairs:       List of ``(src_key, dest_key)`` tuples.
            dest_bucket: Destination bucket for all pairs. Defaults to this bucket.

        Returns:
            Number of objects successfully copied.

        Raises:
            ValidationError:     If any key in *pairs* is empty.
            ObjectNotFoundError: If a source key does not exist.
            BucketNotFoundError: If the destination bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:CopyObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            bucket.copy_many([
                ("raw/jan.csv",  "processed/jan.csv"),
                ("raw/feb.csv",  "processed/feb.csv"),
                ("raw/mar.csv",  "processed/mar.csv"),
            ])

            # Cross-bucket batch copy
            bucket.copy_many(
                [("exports/users.csv", "users.csv")],
                dest_bucket="analytics-bucket",
            )
        """
        if not pairs:
            return 0

        for src_key, dest_key in pairs:
            self.copy(src_key, dest_key, dest_bucket=dest_bucket)

        return len(pairs)

    # ------------------------------------------------------------------
    # Tags — set_tags / get_tags
    # ------------------------------------------------------------------

    def set_tags(self, key: str, tags: dict[str, str]) -> None:
        """Replace all tags on an existing object.

        Any tags previously set on the object are removed and replaced with
        the new set. To delete all tags, pass an empty dict.

        Args:
            key:  S3 key of the object to tag.
            tags: New tags as a ``{key: value}`` dict. All keys and values
                  must be strings. AWS limits tag keys to 128 characters and
                  values to 256 characters, with a maximum of 10 tags per object.

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the object does not exist.
            AccessDeniedError:   If the caller lacks ``s3:PutObjectTagging`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            bucket.set_tags("reports/q1.pdf", {"env": "prod", "owner": "data-team"})

            # Remove all tags
            bucket.set_tags("reports/q1.pdf", {})
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        try:
            if not tags:
                # S3 requires delete_object_tagging to remove all tags;
                # put_object_tagging with an empty TagSet is not valid.
                self._call(self._s3.client.delete_object_tagging, Bucket=self._name, Key=key)
                logger.debug("set_tags s3://%s/%s — cleared all tags", self._name, key)
            else:
                tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
                self._call(
                    self._s3.client.put_object_tagging,
                    Bucket=self._name,
                    Key=key,
                    Tagging={"TagSet": tag_set},
                )
                logger.debug(
                    "set_tags s3://%s/%s — %d tag(s)", self._name, key, len(tags)
                )
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "set_tags") from exc

    def get_tags(self, key: str) -> dict[str, str]:
        """Return all tags for an existing object.

        Args:
            key: S3 key of the object.

        Returns:
            A ``{tag_key: tag_value}`` dict. Returns an empty dict if the
            object has no tags.

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the object does not exist.
            AccessDeniedError:   If the caller lacks ``s3:GetObjectTagging`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            tags = bucket.get_tags("reports/q1.pdf")
            print(tags)  # {"env": "prod", "owner": "data-team"}
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        try:
            response = self._call(
                self._s3.client.get_object_tagging,
                Bucket=self._name,
                Key=key,
            )
            logger.debug("get_tags s3://%s/%s", self._name, key)
            return {tag["Key"]: tag["Value"] for tag in response.get("TagSet", [])}
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "get_tags") from exc

    # ------------------------------------------------------------------
    # Head — object inspection without body download
    # ------------------------------------------------------------------

    def head(self, key: str) -> ObjectInfo:
        """Return full metadata for an object without downloading its body.

        Unlike :meth:`get_bytes`, ``head()`` only fetches HTTP headers, making
        it ideal for checking file size, content type, or user-defined metadata
        without the cost of a full download.

        The returned :class:`ObjectInfo` includes all fields, including
        ``content_type`` and ``metadata``, which are *not* populated by
        :meth:`list`.

        Args:
            key: S3 key of the object to inspect.

        Returns:
            A fully-populated :class:`ObjectInfo` instance.

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the object does not exist.
            AccessDeniedError:   If the caller lacks ``s3:GetObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            info = bucket.head("reports/q1.pdf")
            print(info.size)          # 204_800
            print(info.content_type)  # "application/pdf"
            print(info.metadata)      # {"author": "antonio", "version": "3"}
            print(info.etag)          # "d41d8cd98f00b204e9800998ecf8427e"
        """
        if not key:
            raise ValidationError("Object key must not be empty.")

        try:
            resp = self._call(self._s3.client.head_object, Bucket=self._name, Key=key)
            logger.debug("head s3://%s/%s", self._name, key)
            return ObjectInfo(
                key=key,
                size=resp["ContentLength"],
                last_modified=resp["LastModified"],
                etag=resp["ETag"].strip('"'),
                storage_class=resp.get("StorageClass", "STANDARD"),
                content_type=resp.get("ContentType", ""),
                metadata=resp.get("Metadata", {}),
            )
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "head") from exc

    # ------------------------------------------------------------------
    # Presigned URLs — temporary access without credentials
    # ------------------------------------------------------------------

    def presign(
        self,
        key: str,
        expires_in: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate a presigned URL for temporary, credential-free access.

        The URL grants access to a single object for a limited time without
        requiring AWS credentials. Useful for sharing files with external
        users or enabling browser-side uploads directly to S3.

        Supported methods:

        - ``"GET"``  — allows downloading the object (default).
        - ``"PUT"``  — allows uploading / replacing the object.

        Args:
            key:        S3 key of the object.
            expires_in: URL validity in seconds (default: 3600 = 1 hour).
                        Minimum: 1. Maximum: 604800 (7 days) for most configurations.
            method:     HTTP method the URL will grant. Must be ``"GET"`` or ``"PUT"``.

        Returns:
            A presigned HTTPS URL string.

        Raises:
            ValidationError: If *key* is empty, *expires_in* is not positive,
                             or *method* is not ``"GET"`` or ``"PUT"``.
            S3OperationError: For unexpected errors during URL generation.

        Example::

            # Share a file for 24 hours
            url = bucket.presign("exports/report.pdf", expires_in=86400)

            # Generate an upload URL for client-side PUT (expires in 5 minutes)
            upload_url = bucket.presign("uploads/photo.jpg", method="PUT", expires_in=300)
        """
        if not key:
            raise ValidationError("Object key must not be empty.")
        if expires_in <= 0:
            raise ValidationError(
                f"expires_in must be a positive integer, got {expires_in}."
            )
        method = method.upper()
        if method not in ("GET", "PUT"):
            raise ValidationError(
                f"Invalid method {method!r}. Supported values: 'GET', 'PUT'."
            )

        operation = "get_object" if method == "GET" else "put_object"
        try:
            url: str = self._call(
                self._s3.client.generate_presigned_url,
                operation,
                Params={"Bucket": self._name, "Key": key},
                ExpiresIn=expires_in,
            )
            logger.debug(
                "presign s3://%s/%s method=%s expires_in=%ds",
                self._name,
                key,
                method,
                expires_in,
            )
            return url
        except ClientError as exc:
            raise _map_client_error(exc, key, self._name, "presign") from exc

    # ------------------------------------------------------------------
    # Prefix / folder navigation
    # ------------------------------------------------------------------

    def prefix(self, path: str) -> "Prefix":
        """Return a Prefix scoped to *path* within this bucket.

        Equivalent to ``bucket / "path"`` but accepts slashes in the string.

        Args:
            path: Prefix path (e.g. ``"logs/2025/"``).

        Returns:
            A :class:`Prefix` instance.
        """
        return Prefix(bucket=self, path=_normalise_prefix(path))

    def __truediv__(self, segment: str) -> "Prefix":
        """Pathlib-style folder navigation.

        Example::

            folder = bucket / "dev" / "proyecto1"
            folder.put("demo.txt", "hello")
        """
        return Prefix(bucket=self, path=_normalise_prefix(segment))

    def __repr__(self) -> str:
        return f"Bucket(name={self._name!r})"


# ---------------------------------------------------------------------------
# Prefix
# ---------------------------------------------------------------------------


class Prefix:
    """A scoped view into a bucket path — behaves like a virtual folder.

    Obtain via :meth:`Bucket.prefix` or the ``/`` operator::

        folder = bucket / "dev" / "proyecto1"
        folder.put("demo.txt", "hello")         # writes dev/proyecto1/demo.txt
        folder.get_text("demo.txt")              # reads  dev/proyecto1/demo.txt
        for obj in folder:                       # lists  dev/proyecto1/
            print(obj.key)
        folder.delete_all()                      # deletes everything under prefix
    """

    def __init__(self, bucket: Bucket, path: str) -> None:
        self._bucket = bucket
        self._path = path  # always ends with "/"

    @property
    def full_prefix(self) -> str:
        """The full prefix string, e.g. ``"dev/proyecto1/"``."""
        return self._path

    @property
    def bucket_name(self) -> str:
        """Name of the parent bucket."""
        return self._bucket.name

    def _full_key(self, key: str) -> str:
        """Combine the prefix path with a relative key."""
        if not key:
            raise ValidationError("Object key must not be empty.")
        return self._path + key

    # ------------------------------------------------------------------
    # Write / Read / Delete — delegate to Bucket with prefixed keys
    # ------------------------------------------------------------------

    def put(
        self,
        key: str,
        data: Any,
        content_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Put an object under this prefix. See :meth:`Bucket.put`."""
        self._bucket.put(
            self._full_key(key),
            data,
            content_type=content_type,
            metadata=metadata,
            tags=tags,
        )

    def get_json(self, key: str) -> Any:
        """Get a JSON object under this prefix. See :meth:`Bucket.get_json`."""
        return self._bucket.get_json(self._full_key(key))

    def get_text(self, key: str, encoding: str = "utf-8") -> str:
        """Get a text object under this prefix. See :meth:`Bucket.get_text`."""
        return self._bucket.get_text(self._full_key(key), encoding=encoding)

    def get_bytes(self, key: str) -> bytes:
        """Get a bytes object under this prefix. See :meth:`Bucket.get_bytes`."""
        return self._bucket.get_bytes(self._full_key(key))

    def delete(self, key: str) -> None:
        """Delete an object under this prefix. See :meth:`Bucket.delete`."""
        self._bucket.delete(self._full_key(key))

    def exists(self, key: str) -> bool:
        """Check existence of an object under this prefix. See :meth:`Bucket.exists`."""
        return self._bucket.exists(self._full_key(key))

    def list(self) -> list[ObjectInfo]:
        """List all objects under this prefix.

        Returns:
            List of :class:`ObjectInfo` sorted by key.
        """
        return self._bucket.list(prefix=self._path)

    def delete_all(self) -> int:
        """Delete every object under this prefix.

        Delegates to :meth:`Bucket.delete_many` for efficient bulk deletion
        (up to 1000 keys per S3 call).

        Returns:
            Number of objects deleted.

        Example::

            deleted = (bucket / "tmp").delete_all()
            print(f"Cleaned up {deleted} objects")
        """
        keys = [obj.key for obj in self.list()]
        if not keys:
            return 0

        deleted = self._bucket.delete_many(keys)
        logger.debug(
            "delete_all s3://%s/%s* — deleted %d objects",
            self._bucket.name,
            self._path,
            deleted,
        )
        return deleted

    # ------------------------------------------------------------------
    # Batch operations — delete_many / copy / copy_many
    # ------------------------------------------------------------------

    def delete_many(self, keys: List[str]) -> int:
        """Delete multiple objects under this prefix in bulk.

        Each key in *keys* is treated as relative to this prefix — the full
        S3 key will be ``{prefix}/{key}``.

        Args:
            keys: Relative key names to delete. Empty list is a no-op.

        Returns:
            Number of objects deleted.

        Raises:
            BucketNotFoundError: If the bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:DeleteObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            folder = bucket / "tmp"
            folder.delete_many(["cache1.json", "cache2.json"])
            # deletes: tmp/cache1.json, tmp/cache2.json
        """
        full_keys = [self._full_key(k) for k in keys]
        return self._bucket.delete_many(full_keys)

    def copy(
        self,
        src_key: str,
        dest_key: str,
        dest_bucket: Optional[str] = None,
    ) -> None:
        """Copy an object within this prefix (or to another location).

        Both *src_key* and *dest_key* are treated as relative to this prefix
        unless *dest_bucket* is provided, in which case *dest_key* is used
        as an absolute key in that bucket.

        Args:
            src_key:     Relative source filename within this prefix.
            dest_key:    Relative destination filename within this prefix
                         (or absolute key when *dest_bucket* is set).
            dest_bucket: Optional destination bucket. When omitted, the copy
                         stays within the same bucket under the same prefix.

        Raises:
            ValidationError:     If either key is empty.
            ObjectNotFoundError: If the source object does not exist.
            BucketNotFoundError: If the destination bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:CopyObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            folder = bucket / "reports"
            folder.copy("draft.pdf", "final.pdf")
            # copies: reports/draft.pdf → reports/final.pdf

            folder.copy("final.pdf", "archive/final.pdf",
                        dest_bucket="archive-bucket")
        """
        full_src = self._full_key(src_key)
        # When copying within the same prefix, prefix dest_key too
        full_dest = dest_key if dest_bucket else self._full_key(dest_key)
        self._bucket.copy(full_src, full_dest, dest_bucket=dest_bucket)

    def copy_many(
        self,
        pairs: List[tuple[str, str]],
        dest_bucket: Optional[str] = None,
    ) -> int:
        """Copy multiple objects within this prefix in sequence.

        Each pair is ``(src_key, dest_key)`` where both are relative to this
        prefix (unless *dest_bucket* is provided, in which case dest_key is
        absolute in that bucket).

        Args:
            pairs:       List of ``(src_key, dest_key)`` tuples.
            dest_bucket: Destination bucket for all copies. Defaults to same bucket.

        Returns:
            Number of objects copied.

        Raises:
            ValidationError:     If any key is empty.
            ObjectNotFoundError: If a source object does not exist.
            BucketNotFoundError: If the destination bucket does not exist.
            AccessDeniedError:   If the caller lacks ``s3:CopyObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            folder = bucket / "raw"
            folder.copy_many([
                ("jan.csv", "jan_backup.csv"),
                ("feb.csv", "feb_backup.csv"),
            ])
            # copies: raw/jan.csv → raw/jan_backup.csv, etc.
        """
        for src_key, dest_key in pairs:
            self.copy(src_key, dest_key, dest_bucket=dest_bucket)
        return len(pairs)

    # ------------------------------------------------------------------
    # Tags — set_tags / get_tags
    # ------------------------------------------------------------------

    def set_tags(self, key: str, tags: dict[str, str]) -> None:
        """Replace all tags on an object under this prefix.

        See :meth:`Bucket.set_tags` for full parameter documentation.

        Args:
            key:  Relative filename within this prefix.
            tags: New tags as a ``{key: value}`` dict.

        Example::

            folder = bucket / "reports"
            folder.set_tags("q1.pdf", {"env": "prod", "reviewed": "true"})
            # tags: reports/q1.pdf
        """
        self._bucket.set_tags(self._full_key(key), tags)

    def get_tags(self, key: str) -> dict[str, str]:
        """Return all tags for an object under this prefix.

        See :meth:`Bucket.get_tags` for full parameter documentation.

        Args:
            key: Relative filename within this prefix.

        Returns:
            A ``{tag_key: tag_value}`` dict. Empty dict if no tags.

        Example::

            folder = bucket / "reports"
            tags = folder.get_tags("q1.pdf")
            # reads tags from: reports/q1.pdf
        """
        return self._bucket.get_tags(self._full_key(key))

    # ------------------------------------------------------------------
    # File I/O — upload / download
    # ------------------------------------------------------------------

    def upload(
        self,
        local_path: str | Path,
        key: str,
        on_progress: Optional[Callable[[float], None]] = None,
    ) -> None:
        """Upload a local file under this prefix.

        The final S3 key is ``{prefix}/{key}``. See :meth:`Bucket.upload`
        for full parameter documentation and multipart behaviour.

        Args:
            local_path:  Path to the local file (str or :class:`pathlib.Path`).
            key:         Relative filename within this prefix.
            on_progress: Optional callback with upload percentage (0.0–100.0).

        Example::

            folder = bucket / "backups" / "2025"
            folder.upload("nightly.tar.gz", "nightly.tar.gz",
                          on_progress=lambda p: print(f"{p:.0f}%"))
            # S3 key: backups/2025/nightly.tar.gz
        """
        self._bucket.upload(local_path, self._full_key(key), on_progress=on_progress)

    def download(
        self,
        key: str,
        local_path: str | Path,
    ) -> None:
        """Download an object under this prefix to a local file.

        See :meth:`Bucket.download` for full parameter documentation.

        Args:
            key:        Relative filename within this prefix.
            local_path: Destination path on disk.

        Example::

            folder = bucket / "reports"
            folder.download("jan.csv", "/tmp/jan.csv")
            # downloads: reports/jan.csv
        """
        self._bucket.download(self._full_key(key), local_path)

    # ------------------------------------------------------------------
    # Streaming — open / stream
    # ------------------------------------------------------------------

    @contextmanager
    def open(
        self,
        key: str,
        encoding: str = "utf-8",
    ) -> Generator[Iterator[str], None, None]:
        """Stream a text object under this prefix, line by line.

        See :meth:`Bucket.open` for full parameter documentation.

        Args:
            key:      Relative filename within this prefix.
            encoding: Text encoding (default: utf-8).

        Yields:
            An iterator of decoded text lines (newlines stripped).

        Example::

            folder = bucket / "events" / "2025"
            with folder.open("january.jsonl") as lines:
                for line in lines:
                    process(json.loads(line))
            # reads: events/2025/january.jsonl
        """
        with self._bucket.open(self._full_key(key), encoding=encoding) as lines:
            yield lines

    def stream(
        self,
        key: str,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """Stream a binary object under this prefix as raw chunks.

        See :meth:`Bucket.stream` for full parameter documentation.

        Args:
            key:        Relative filename within this prefix.
            chunk_size: Bytes per chunk (default: 8 MB).

        Yields:
            ``bytes`` chunks of at most *chunk_size* bytes each.

        Example::

            folder = bucket / "media"
            for chunk in folder.stream("video.mp4", chunk_size=16 * 1024 * 1024):
                process_chunk(chunk)
            # streams: media/video.mp4
        """
        yield from self._bucket.stream(self._full_key(key), chunk_size=chunk_size)

    # ------------------------------------------------------------------
    # Head — object inspection without body download
    # ------------------------------------------------------------------

    def head(self, key: str) -> ObjectInfo:
        """Return full metadata for an object under this prefix.

        See :meth:`Bucket.head` for full documentation. The returned
        :class:`ObjectInfo` includes ``content_type`` and ``metadata``
        in addition to the standard list fields.

        Args:
            key: Relative filename within this prefix.

        Returns:
            A fully-populated :class:`ObjectInfo` instance.

        Raises:
            ValidationError:     If *key* is empty.
            ObjectNotFoundError: If the object does not exist.
            AccessDeniedError:   If the caller lacks ``s3:GetObject`` permission.
            S3OperationError:    For any other AWS error.

        Example::

            folder = bucket / "reports"
            info = folder.head("q1.pdf")
            print(info.size, info.content_type)
            # reads head from: reports/q1.pdf
        """
        return self._bucket.head(self._full_key(key))

    # ------------------------------------------------------------------
    # Presigned URLs — temporary access without credentials
    # ------------------------------------------------------------------

    def presign(
        self,
        key: str,
        expires_in: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate a presigned URL for an object under this prefix.

        See :meth:`Bucket.presign` for full documentation.

        Args:
            key:        Relative filename within this prefix.
            expires_in: URL validity in seconds (default: 3600).
            method:     HTTP method: ``"GET"`` (download) or ``"PUT"`` (upload).

        Returns:
            A presigned HTTPS URL string.

        Raises:
            ValidationError: If *key* is empty, *expires_in* is not positive,
                             or *method* is not ``"GET"`` or ``"PUT"``.

        Example::

            folder = bucket / "exports"
            url = folder.presign("report.pdf", expires_in=3600)
            # presigns: exports/report.pdf
        """
        return self._bucket.presign(self._full_key(key), expires_in=expires_in, method=method)

    # ------------------------------------------------------------------
    # Prefix chaining — supports bucket / "a" / "b" / "c"
    # ------------------------------------------------------------------

    def __truediv__(self, segment: str) -> "Prefix":
        """Chain an additional path segment onto this prefix.

        Example::

            deep = bucket / "a" / "b" / "c"
            # full_prefix == "a/b/c/"
        """
        return Prefix(bucket=self._bucket, path=self._path + _normalise_prefix(segment))

    # ------------------------------------------------------------------
    # Iteration — for obj in folder: ...
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[ObjectInfo]:
        """Iterate over all objects under this prefix."""
        return iter(self.list())

    def __repr__(self) -> str:
        return f"Prefix(bucket={self._bucket.name!r}, path={self._path!r})"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_prefix(path: str) -> str:
    """Strip leading slashes and ensure a single trailing slash."""
    path = path.strip("/")
    if not path:
        raise ValidationError("Prefix segment must not be empty.")
    return path + "/"


def _serialize(data: Any) -> tuple[bytes, str]:
    """Convert *data* to (bytes, content_type) for storage in S3.

    Rules:
    - dict / list → JSON (application/json)
    - str         → UTF-8 bytes (text/plain; charset=utf-8)
    - bytes       → as-is (application/octet-stream)

    Raises:
        ValidationError: For unsupported types.
    """
    if isinstance(data, (dict, list)):
        return json.dumps(data, ensure_ascii=False).encode("utf-8"), _CONTENT_TYPE_JSON
    if isinstance(data, str):
        return data.encode("utf-8"), _CONTENT_TYPE_TEXT
    if isinstance(data, (bytes, bytearray)):
        return bytes(data), _CONTENT_TYPE_BINARY
    raise ValidationError(
        f"Unsupported data type for put(): {type(data).__name__}. "
        "Use dict, list, str, or bytes."
    )


def _encode_tags(tags: dict[str, str]) -> str:
    """Encode a tags dict as a URL query string for S3 Tagging."""
    from urllib.parse import urlencode

    return urlencode(tags)


def _iter_lines(body: Any, encoding: str) -> Iterator[str]:
    """Yield decoded, newline-stripped lines from a boto3 StreamingBody.

    Reads the streaming body line by line using ``iter_lines()``, decodes
    each chunk with *encoding*, and strips trailing ``\\r`` and ``\\n``
    characters to match the behaviour of Python's built-in text-mode
    ``open()``.

    Args:
        body:     A boto3 ``StreamingBody`` object.
        encoding: The character encoding used to decode each line.

    Yields:
        Decoded text lines without trailing newline characters.
    """
    for raw_line in body.iter_lines():
        yield raw_line.decode(encoding).rstrip("\r\n")
