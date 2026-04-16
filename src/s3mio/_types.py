"""Internal types and protocols for s3mio."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional, Protocol, runtime_checkable


@dataclass
class CopyResult:
    """Result returned by :meth:`Bucket.copy_many`.

    Each element is the complete pair ``(src_key, dest_key)`` — the atomic
    unit of a copy operation — so the caller never has to reconstruct the
    mapping from a single-key list.

    Attributes:
        done:   Pairs that S3 confirmed as successfully copied.
        failed: Pairs that could not be copied, as
                ``(src_key, dest_key, error_message)`` triples.

    Example::

        result = bucket.copy_many(pairs)
        if not result:
            for src, dest, err in result.failed:
                print(f"  {src!r} → {dest!r} failed: {err}")
        # Retry just the failed ones:
        bucket.copy_many(result.failed_pairs())
    """

    done: list[tuple[str, str]]           # (src_key, dest_key)
    failed: list[tuple[str, str, str]]    # (src_key, dest_key, error_message)

    def __len__(self) -> int:
        """Return the number of successfully copied pairs."""
        return len(self.done)

    def __bool__(self) -> bool:
        """Return True when there are no failures."""
        return not self.failed

    def failed_pairs(self) -> list[tuple[str, str]]:
        """Return ``(src_key, dest_key)`` pairs for all failures, ready to retry."""
        return [(s, d) for s, d, _ in self.failed]


@dataclass
class DeleteResult:
    """Result returned by :meth:`Bucket.delete_many`.

    Attributes:
        deleted: Keys that S3 confirmed as successfully deleted.
        failed:  Keys that S3 could not delete, as ``(key, error_code)`` pairs.
                 Common causes: object-lock, versioning, insufficient permissions.

    Example::

        result = bucket.delete_many(keys)
        if not result:                         # bool(result) is False when failures exist
            for key, code in result.failed:
                print(f"Failed to delete {key!r}: {code}")
        print(f"Deleted {len(result)} objects")
    """

    deleted: list[str]
    failed: list[tuple[str, str]]  # (key, error_code)

    def __len__(self) -> int:
        """Return the number of successfully deleted objects."""
        return len(self.deleted)

    def __bool__(self) -> bool:
        """Return True when there are no failures."""
        return not self.failed


@dataclass
class ObjectInfo:
    """Metadata for a single S3 object returned by :meth:`Bucket.list` or :meth:`Bucket.head`.

    Fields populated by ``list()``
        key, size, last_modified, etag, storage_class.

    Additional fields populated only by ``head()``
        content_type, metadata.

    Attributes:
        key:           Full S3 key (e.g. ``"users/john.json"``).
        size:          Object size in bytes.
        last_modified: UTC timestamp of the last modification.
        etag:          Entity tag (MD5 or multipart hash, quotes stripped).
        storage_class: S3 storage class (e.g. ``"STANDARD"``, ``"INTELLIGENT_TIERING"``).
        content_type:  MIME type of the object (e.g. ``"application/json"``).
                       Empty string when returned from ``list()`` — use ``head()``
                       to retrieve this field.
        metadata:      User-defined metadata key-value pairs set at upload time.
                       Empty dict when returned from ``list()`` — use ``head()``
                       to retrieve this field.
        tags:          Object tags as ``{key: value}`` pairs.
                       Empty dict unless ``head(key, with_tags=True)`` is used.
    """

    key: str
    size: int
    last_modified: datetime
    etag: str
    storage_class: str
    content_type: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class BucketProtocol(Protocol):
    """Structural protocol for Bucket-like objects.

    Useful for type-checking code that accepts either a :class:`Bucket` or any
    compatible implementation. Kept in sync with the public API of ``Bucket``.
    """

    @property
    def name(self) -> str: ...

    # Core CRUD
    def put(self, key: str, data: Any, **kwargs: Any) -> None: ...
    def get_json(self, key: str) -> Any: ...
    def get_text(self, key: str, encoding: str = ...) -> str: ...
    def get_bytes(self, key: str) -> bytes: ...
    def delete(self, key: str) -> None: ...
    def exists(self, key: str) -> bool: ...

    # List
    def list(self, prefix: str = ..., delimiter: str = ...) -> List[ObjectInfo]: ...
    def iter_list(self, prefix: str = ..., delimiter: str = ...) -> Iterator[ObjectInfo]: ...

    # File I/O
    def upload(
        self,
        local_path: str | Path,
        key: str,
        on_progress: Optional[Callable[[float], None]] = ...,
        **kwargs: Any,
    ) -> None: ...
    def download(self, key: str, local_path: str | Path) -> None: ...

    # Batch
    def delete_many(self, keys: List[str]) -> DeleteResult: ...
    def copy(self, src_key: str, dest_key: str, dest_bucket: Optional[str] = ...) -> None: ...
    def copy_many(
        self, pairs: List[tuple[str, str]], **kwargs: Any
    ) -> CopyResult: ...

    # Tags
    def set_tags(self, key: str, tags: dict[str, str]) -> None: ...
    def get_tags(self, key: str) -> dict[str, str]: ...

    # Metadata & presign
    def head(self, key: str, *, with_tags: bool = ...) -> ObjectInfo: ...
    def presign(self, key: str, expires_in: int = ..., method: str = ...) -> str: ...

    def __truediv__(self, segment: str) -> "PrefixProtocol": ...


@runtime_checkable
class PrefixProtocol(Protocol):
    """Structural protocol for Prefix-like objects.

    Kept in sync with the public API of :class:`Prefix`.
    """

    @property
    def full_prefix(self) -> str: ...

    @property
    def bucket_name(self) -> str: ...

    # Core CRUD
    def put(self, key: str, data: Any, **kwargs: Any) -> None: ...
    def get_json(self, key: str) -> Any: ...
    def get_text(self, key: str, encoding: str = ...) -> str: ...
    def get_bytes(self, key: str) -> bytes: ...
    def delete(self, key: str) -> None: ...
    def exists(self, key: str) -> bool: ...

    # List
    def list(self) -> List[ObjectInfo]: ...
    def iter_list(self) -> Iterator[ObjectInfo]: ...

    # File I/O
    def upload(
        self,
        local_path: str | Path,
        key: str,
        on_progress: Optional[Callable[[float], None]] = ...,
        **kwargs: Any,
    ) -> None: ...
    def download(self, key: str, local_path: str | Path) -> None: ...

    # Batch
    def delete_many(self, keys: List[str]) -> DeleteResult: ...
    def delete_all(self) -> DeleteResult: ...
    def copy(self, src_key: str, dest_key: str, dest_bucket: Optional[str] = ...) -> None: ...
    def copy_many(
        self, pairs: List[tuple[str, str]], **kwargs: Any
    ) -> CopyResult: ...

    # Tags
    def set_tags(self, key: str, tags: dict[str, str]) -> None: ...
    def get_tags(self, key: str) -> dict[str, str]: ...

    # Metadata & presign
    def head(self, key: str, *, with_tags: bool = ...) -> ObjectInfo: ...
    def presign(self, key: str, expires_in: int = ..., method: str = ...) -> str: ...

    def __truediv__(self, segment: str) -> "PrefixProtocol": ...
    def __iter__(self) -> Iterator[ObjectInfo]: ...
