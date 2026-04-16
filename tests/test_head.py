"""Tests for Fase 4 — head()."""

from datetime import datetime

import pytest

from s3mio._types import ObjectInfo
from s3mio.bucket import Bucket
from s3mio.exceptions import ObjectNotFoundError, ValidationError

# ---------------------------------------------------------------------------
# Bucket.head
# ---------------------------------------------------------------------------


class TestBucketHead:
    def test_head_returns_object_info(self, bucket: Bucket) -> None:
        """head() returns an ObjectInfo instance."""
        bucket.put("file.txt", "hello")
        info = bucket.head("file.txt")
        assert isinstance(info, ObjectInfo)

    def test_head_key_matches(self, bucket: Bucket) -> None:
        """The returned ObjectInfo.key equals the requested key."""
        bucket.put("docs/readme.txt", "content")
        info = bucket.head("docs/readme.txt")
        assert info.key == "docs/readme.txt"

    def test_head_size_is_correct(self, bucket: Bucket) -> None:
        """ObjectInfo.size reflects the actual byte length of the object."""
        data = b"hello world"
        bucket.put("size_test.bin", data)
        info = bucket.head("size_test.bin")
        assert info.size == len(data)

    def test_head_last_modified_is_datetime(self, bucket: Bucket) -> None:
        """ObjectInfo.last_modified is a datetime instance."""
        bucket.put("dated.txt", "data")
        info = bucket.head("dated.txt")
        assert isinstance(info.last_modified, datetime)

    def test_head_etag_is_non_empty_string(self, bucket: Bucket) -> None:
        """ObjectInfo.etag is a non-empty string without surrounding quotes."""
        bucket.put("tagged.txt", "hello")
        info = bucket.head("tagged.txt")
        assert isinstance(info.etag, str)
        assert len(info.etag) > 0
        assert not info.etag.startswith('"')
        assert not info.etag.endswith('"')

    def test_head_storage_class_is_standard(self, bucket: Bucket) -> None:
        """Default storage class for new objects is STANDARD."""
        bucket.put("standard.txt", "data")
        info = bucket.head("standard.txt")
        assert info.storage_class == "STANDARD"

    def test_head_content_type_json(self, bucket: Bucket) -> None:
        """head() reports application/json for JSON objects."""
        bucket.put("data.json", {"key": "value"})
        info = bucket.head("data.json")
        assert "json" in info.content_type.lower()

    def test_head_content_type_text(self, bucket: Bucket) -> None:
        """head() reports text/plain for string objects."""
        bucket.put("note.txt", "hello world")
        info = bucket.head("note.txt")
        assert "text" in info.content_type.lower()

    def test_head_content_type_binary(self, bucket: Bucket) -> None:
        """head() reports application/octet-stream for bytes objects."""
        bucket.put("binary.bin", b"\x00\x01\x02")
        info = bucket.head("binary.bin")
        assert info.content_type == "application/octet-stream"

    def test_head_content_type_override(self, bucket: Bucket) -> None:
        """head() returns the explicitly set content_type."""
        bucket.put("photo.jpg", b"\xff\xd8", content_type="image/jpeg")
        info = bucket.head("photo.jpg")
        assert info.content_type == "image/jpeg"

    def test_head_metadata_is_returned(self, bucket: Bucket) -> None:
        """User-defined metadata set at upload time is included in head()."""
        bucket.put("doc.txt", "content", metadata={"author": "antonio", "version": "2"})
        info = bucket.head("doc.txt")
        assert info.metadata.get("author") == "antonio"
        assert info.metadata.get("version") == "2"

    def test_head_metadata_empty_when_none_set(self, bucket: Bucket) -> None:
        """head() returns an empty dict when no user metadata was set."""
        bucket.put("plain.txt", "no metadata here")
        info = bucket.head("plain.txt")
        assert info.metadata == {}

    def test_head_empty_key_raises(self, bucket: Bucket) -> None:
        """head() with an empty key raises ValidationError."""
        with pytest.raises(ValidationError):
            bucket.head("")

    def test_head_missing_key_raises(self, bucket: Bucket) -> None:
        """head() on a non-existent key raises ObjectNotFoundError."""
        with pytest.raises(ObjectNotFoundError):
            bucket.head("does/not/exist.txt")

    def test_head_does_not_download_body(self, bucket: Bucket) -> None:
        """head() can inspect a large object without fetching its data."""
        # Put a non-trivial amount of content
        data = b"x" * 100_000
        bucket.put("large.bin", data)
        # head() should return size info without having read 100k bytes
        info = bucket.head("large.bin")
        assert info.size == 100_000

    def test_head_after_put_with_tags_returns_size(self, bucket: Bucket) -> None:
        """head() works correctly for objects that also have tags set."""
        bucket.put("tagged.txt", "hello", tags={"env": "prod"})
        info = bucket.head("tagged.txt")
        assert info.size == len(b"hello")

    def test_head_unicode_content_size(self, bucket: Bucket) -> None:
        """head() reports the byte length (not character count) for unicode content."""
        text = "héllo"  # 'é' is 2 bytes in UTF-8
        bucket.put("unicode.txt", text)
        info = bucket.head("unicode.txt")
        assert info.size == len(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Prefix.head
# ---------------------------------------------------------------------------


class TestPrefixHead:
    def test_prefix_head_uses_full_key(self, bucket: Bucket) -> None:
        """Prefix.head inspects the correct prefixed S3 key."""
        folder = bucket / "reports"
        bucket.put("reports/q1.pdf", b"%PDF", content_type="application/pdf")
        info = folder.head("q1.pdf")
        assert info.key == "reports/q1.pdf"
        assert info.content_type == "application/pdf"

    def test_prefix_head_returns_correct_size(self, bucket: Bucket) -> None:
        """Prefix.head reports the exact size of the prefixed object."""
        folder = bucket / "data"
        content = b"hello world!"
        folder.put("greeting.bin", content)
        info = folder.head("greeting.bin")
        assert info.size == len(content)

    def test_prefix_head_returns_metadata(self, bucket: Bucket) -> None:
        """Prefix.head returns user metadata for the prefixed object."""
        folder = bucket / "docs"
        folder.put("readme.txt", "info", metadata={"lang": "es"})
        info = folder.head("readme.txt")
        assert info.metadata.get("lang") == "es"

    def test_prefix_head_missing_key_raises(self, bucket: Bucket) -> None:
        """Prefix.head raises ObjectNotFoundError for a missing key."""
        folder = bucket / "nothing"
        with pytest.raises(ObjectNotFoundError):
            folder.head("ghost.txt")

    def test_prefix_head_empty_key_raises(self, bucket: Bucket) -> None:
        """Prefix.head raises ValidationError for an empty key."""
        folder = bucket / "x"
        with pytest.raises(ValidationError):
            folder.head("")
